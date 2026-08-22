#!/usr/bin/env python3
"""
routing_daemon.py — 路由分发 daemon，定期轮询 bus 未消费消息并分发。

职责：
- 每 60s 调 route_all() 分发未消费消息给对应消费者
- 每 300s 调 route_all_to_ccs() 推送给 CCS
- 健康检查 + 重连

与 workflow-engine.service（pipeflow/daemon.py）职责分离：
  本 daemon = 消息路由分发（bus → 角色）
  workflow daemon = 工作流步骤推进（engine.run_once）
"""
import fcntl, importlib, importlib.util, json, os, sys, time
from pathlib import Path
# Ensure pipeline src is first in sys.path
_PIPELINE_SRC = str(Path(__file__).resolve().parent)
if _PIPELINE_SRC not in sys.path or sys.path[0] != _PIPELINE_SRC:
    sys.path.insert(0, _PIPELINE_SRC)
# Pre-load pipeline's config_loader before .hermes/scripts shadows it
import config_loader as _cfg_preload
_cfg_preload.get_config  # 副作用引用：确保模块加载，缓存正确版本到 sys.modules

# 使用绝对导入确保读到 pipeline 的 paths（避免被 launcher 的 paths 遮蔽）
_paths_spec = importlib.util.spec_from_file_location(
    "pipeline_paths", str(Path(_PIPELINE_SRC) / "paths.py"))
_paths_mod = importlib.util.module_from_spec(_paths_spec)
_paths_spec.loader.exec_module(_paths_mod)
SENTINEL_DIR = _paths_mod.CCS_SENTINEL_DIR

from routing.auto import route_all, route_all_to_ccs
from reliability import LOGGER, METRICS

__all__ = ["route_all", "route_all_to_ccs", "LOGGER", "METRICS"]

INTERVAL = 60       # route-all 间隔
CCS_INTERVAL = 300  # route-all-to-ccs 间隔
EVAL_INTERVAL = 600 # eval_criteria 检查间隔（每 10 分钟）
DRIFT_INTERVAL = 3600  # 角色输出漂移检测间隔（每 1 小时）
SURVIVAL_INTERVAL = 120  # 存活检测间隔（每 2 分钟）
P0_CHECK_INTERVAL = 1800  # P0 超时检测间隔（每 30 分钟）
CRON_SCHEDULE_TRACKER = Path.home() / ".hermes" / "run" / "cron_schedule_triggered.json"  # 去重跟踪


def _any_live_targets() -> bool:
    """如果存在任意 pid>0 且 tmux_session 非空的 CCS 哨兵则返回 True。"""
    for p in SENTINEL_DIR.glob("*.json"):
        try:
            s = json.loads(p.read_text())
            if s.get("pid", 0) > 0 and s.get("tmux_session", "").strip():
                return True
        except Exception:
            continue
    return False


def _check_cron_schedules() -> int:
    """检查角色 JSON 的 cron_schedule，到期角色执行 ccs.py send。

    使用 croniter 做时间匹配，/tmp/cron_schedule_triggered.json 做去重。
    返回触发次数。各 daemon 调用：routing_daemon 每 300s。
    """
    try:
        import croniter
    except ImportError:
        LOGGER.warning("croniter 未安装，跳过 cron_schedule 检查")
        return 0

    # 加载角色路径配置
    try:
        from session_loader import get_roles_dir
        roles_dir = get_roles_dir() / "personas" / "session-roles"
    except ImportError:
        roles_dir = Path.home() / "hermes-session-roles" / "personas" / "session-roles"

    if not roles_dir.exists():
        LOGGER.debug("cron_schedule: 角色目录不存在 %s", roles_dir)
        return 0

    # 读取去重跟踪
    triggered = {}
    if CRON_SCHEDULE_TRACKER.exists():
        try:
            triggered = json.loads(CRON_SCHEDULE_TRACKER.read_text())
        except (json.JSONDecodeError, OSError):
            triggered = {}

    now = time.time()
    now_minute = int(now // 60) * 60  # 当前分钟的开始
    triggered_count = 0
    launcher = _paths_mod.CCS_CLI

    for f in sorted(roles_dir.glob("persona_*.json")):
        try:
            data = json.loads(f.read_text())
            cron_exp = data.get("cron_schedule", "").strip()
            if not cron_exp:
                continue
            role = data.get("name", "")
            if not role:
                continue
            # 检查是否触发了
            try:
                cron = croniter.croniter(cron_exp, now - 60)
                prev = cron.get_prev(float)
                if now_minute <= prev < now:
                    # 去重：同一角色同一分钟不重复触发
                    key = f"{role}:{cron_exp}"
                    last = triggered.get(key, 0)
                    if now - last > 60:
                        triggered[key] = now
                        LOGGER.info("cron_schedule 触发: %s (%s)", role, cron_exp)
                        subprocess_run = __import__("subprocess").run
                        subprocess_run(
                            [sys.executable, str(launcher), "send", role,
                             f"定时任务触发 (cron: {cron_exp})"],
                            capture_output=True, timeout=10,
                        )
                        triggered_count += 1
            except (ValueError, KeyError):
                continue  # cron 表达式无效
        except Exception as e:
            LOGGER.debug("cron_schedule 解析失败 %s: %s", f.name, e)
            continue

    # 写回去重跟踪
    CRON_SCHEDULE_TRACKER.write_text(json.dumps(triggered))
    if triggered_count:
        LOGGER.info("cron_schedule: 本次触发 %d 个角色", triggered_count)
    return triggered_count


def main():
    LOGGER.info("routing_daemon 启动 PID=%d", os.getpid())
    last_ccs = 0
    last_eval = 0
    last_survival = 0
    last_drift = 0
    last_metrics = 0
    last_p0_check = 0
    last_g3_scan = 0

    # ── 单实例 PID 锁 ──
    _pid_file = Path.home() / ".hermes" / "run" / "routing_daemon.pid"
    _pid_lock_fd = open(_pid_file.with_suffix(".lock"), "w")
    try:
        fcntl.flock(_pid_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        LOGGER.error("routing_daemon 已运行（PID 锁被占用），退出")
        return
    _pid_file.write_text(str(os.getpid()))
    # ────────────────────

    # 延迟导入 eval_checker（避免模块级循环引用）
    _run_eval = None
    _eval_checker_mtime = 0.0

    try:
        error_backoff = 0
        while True:
            if not _any_live_targets():
                LOGGER.warning("无存活 CCS 目标，休眠 300s")
                time.sleep(300)
                continue

            try:
                r = route_all()
                routed_n, total_n = r.get("routed", 0), r.get("total", 0)
                if routed_n:
                    for d in r.get("details", []):
                        roles = [a["role"] for a in d.get("assigned", []) if a.get("status") == "consumed"]
                        if roles:
                            LOGGER.info("route → [%s] #%d %s → %s",
                                        d.get("category", "?"), d.get("id", 0),
                                        d.get("title", "")[:50], ",".join(roles))
                    LOGGER.info("route_all: %d/%d routed", routed_n, total_n)
                elif total_n:
                    LOGGER.debug("route_all: 0/%d 无可消费消息或无匹配消费者", total_n)
                error_backoff = 0
            except Exception as e:
                LOGGER.error("route_all 异常 (backoff=%ds): %s", error_backoff, e)
                METRICS.inc("daemon_errors_total")
                error_backoff = min(error_backoff + 5, 120)

            now = time.time()

            # 定时推送给 CCS + cron_schedule 消费
            if now - last_ccs >= CCS_INTERVAL:
                try:
                    r = route_all_to_ccs()
                    ccs_routed = r.get("routed", 0)
                    ccs_total = r.get("total", 0)
                    if ccs_routed:
                        for d in r.get("details", []):
                            LOGGER.info("ccs-route [%s] #%d → %s",
                                        d.get("category", "?"), d.get("id", 0),
                                        ",".join(d.get("assigned", [])))
                    LOGGER.info("route_all_to_ccs: %d/%d routed", ccs_routed, ccs_total)
                    error_backoff = 0
                except Exception as e:
                    LOGGER.error("route_all_to_ccs 异常 (backoff=%ds): %s", error_backoff, e)
                    METRICS.inc("daemon_errors_total")
                    error_backoff = min(error_backoff + 5, 120)
                try:
                    _check_cron_schedules()
                except Exception as e:
                    LOGGER.error("cron_schedule 检查异常: %s", e)
                    METRICS.inc("daemon_errors_total")
                last_ccs = now

            # 定时执行 eval_criteria 检查
            if now - last_eval >= EVAL_INTERVAL:
                # D6 生长接入: eval_consistency 预检 persona eval_criteria 与白名单一致性，
                # 防止运行时 exit=-3 静默跳过（2026-08-11 白名单漂移事故根因）
                try:
                    from eval_consistency import main as _ec_main
                    import io, contextlib
                    _ec_buf = io.StringIO()
                    with contextlib.redirect_stdout(_ec_buf):
                        _ec_main(["--json"])
                    LOGGER.debug("eval_consistency 预检: %s", _ec_buf.getvalue()[:200])
                except Exception as _ec:
                    LOGGER.warning("eval_consistency 预检异常（不阻塞 eval 执行）: %s", _ec)
                if _run_eval is None:
                    try:
                        from eval_checker import run_eval_check
                        _run_eval = run_eval_check
                        _eval_checker_mtime = os.path.getmtime(str(Path(__file__).parent / "eval_checker.py"))
                    except Exception:
                        _run_eval = lambda: {"checked": 0}
                # ponytail: 检测 eval_checker.py 变更，热重载无需重启 daemon
                eval_checker_mtime = os.path.getmtime(str(Path(__file__).parent / "eval_checker.py"))
                if _eval_checker_mtime != eval_checker_mtime:
                    try:
                        # 先清 sys.modules 缓存，强制 reimport（防止 importlib.reload 重用旧闭包）
                        for _key in list(sys.modules):
                            if _key == "eval_checker" or _key.startswith("eval_checker."):
                                del sys.modules[_key]
                        import eval_checker
                        importlib.reload(eval_checker)
                        _run_eval = eval_checker.run_eval_check
                        _old_mtime = _eval_checker_mtime
                        _eval_checker_mtime = eval_checker_mtime
                        LOGGER.info("eval_checker 热重载成功 (mtime %s→%s)",
                                    _old_mtime, eval_checker_mtime)
                    except Exception as _e:
                        LOGGER.warning("eval_checker 热重载失败: %s", _e)
                        # 热重载失败：尝试强制重新 import（兜底）
                        try:
                            for _key in list(sys.modules):
                                if _key == "eval_checker" or _key.startswith("eval_checker."):
                                    del sys.modules[_key]
                            import importlib as _il
                            eval_checker = _il.import_module("eval_checker")
                            _run_eval = eval_checker.run_eval_check
                            _eval_checker_mtime = eval_checker_mtime
                            LOGGER.info("eval_checker 强制重新 import 成功")
                        except Exception as _e2:
                            LOGGER.error("eval_checker 强制重新 import 也失败: %s", _e2)
                try:
                    r = _run_eval()
                    if r.get("failed", 0):
                        LOGGER.warning("eval_check 失败: %d/%d 未通过 (blockers=%d, notices=%d)",
                                      r["failed"], r["checked"], r.get("blockers", 0), r.get("notices", 0))
                    elif r.get("passed", 0):
                        LOGGER.info("eval_check: %d/%d 通过 ✓", r["passed"], r["checked"])
                except Exception as e:
                    LOGGER.error("eval_check 异常: %s", e)
                    METRICS.inc("daemon_errors_total")
                last_eval = now

            # P0 豁免超时检测（每 30 分钟）
            if now - last_p0_check >= P0_CHECK_INTERVAL:
                try:
                    from p0_exemption import P0Exemption
                    _p0 = P0Exemption("coordinator")
                    violations = _p0.check_timeouts()
                    if violations:
                        LOGGER.warning("P0 超时违规: %d 个任务", len(violations))
                        for v in violations:
                            LOGGER.info("  P0 violation: %s (%s) elapsed %.1fh",
                                        v["task_id"], v["title"], v["elapsed_hours"])
                except Exception as e:
                    LOGGER.error("P0 超时检测异常: %s", e)
                    METRICS.inc("daemon_errors_total")
                last_p0_check = now

            # G3 治理提案扫描（每 30 分钟，与 P0 同频，避免新增定时器）
            if now - last_g3_scan >= P0_CHECK_INTERVAL:
                try:
                    # 采用后台线程执行，避免 tmux capture-pane 同步阻塞主循环
                    # 参考 daemon 既有 _survival_monitor 缓存范式：仅提交任务，不等待结果
                    import threading
                    def _run_g3_scan():
                        try:
                            from conversation_monitor import scan_and_propose
                            n = scan_and_propose()
                            if n:
                                LOGGER.info("G3 治理提案扫描: %d 个提案", n)
                        except Exception as e:
                            LOGGER.error("G3 扫描异常: %s", e)
                            METRICS.inc("daemon_errors_total")
                    threading.Thread(target=_run_g3_scan, daemon=True).start()
                except Exception as e:
                    LOGGER.error("G3 任务提交异常: %s", e)
                    METRICS.inc("daemon_errors_total")
                last_g3_scan = now

            # 定时角色输出漂移检测（每小时）
            if now - last_drift >= DRIFT_INTERVAL:
                try:
                    from drift_detector import detect_all_drift, report_drift
                    _drift = detect_all_drift()
                    if _drift:
                        n = report_drift(_drift)
                        LOGGER.warning("drift 检测: %d 个角色有输出偏离", n)
                    else:
                        LOGGER.info("drift 检测: 无偏离 ✓")
                except Exception as e:
                    LOGGER.error("drift 检测异常: %s", e)
                    METRICS.inc("daemon_errors_total")
                last_drift = now

            # 存活检测 tick（L1 进程存活 + 哨兵状态）
            if now - last_survival >= SURVIVAL_INTERVAL:
                try:
                    from routing.survival_monitor import SurvivalMonitor
                    _sm = getattr(main, '_survival_monitor', None)
                    if _sm is None:
                        _sm = SurvivalMonitor()
                        main._survival_monitor = _sm
                    result = _sm.tick()
                    stalled = [r for r, h in result.items() if h.get('overall') in ('stale', 'dead')]
                    if stalled:
                        LOGGER.warning("存活告警: %s", stalled)
                except ImportError:
                    pass  # must-silent: survival_monitor optional component
                except Exception as e:
                    LOGGER.error("survival_check 异常: %s", e)
                    METRICS.inc("daemon_errors_total")
                last_survival = now

            # R2-4 生长接入: workflow_metrics 快照写 bus（每小时一次）
            # 8.6MB workflow-metrics.jsonl 长期无消费者（D6 发现），
            # 由 task_generator heal 写入，这里消费输出供监控审计
            if now - last_metrics >= 3600:
                try:
                    from workflow_metrics import snapshot as _wf_snapshot
                    import io, contextlib
                    _buf = io.StringIO()
                    with contextlib.redirect_stdout(_buf):
                        _wf_snapshot()
                    _snap_text = _buf.getvalue()
                    _rate_line = next((l for l in _snap_text.splitlines() if "完成率" in l), "完成率: N/A")
                    import subprocess as _sp
                    _sp.run(
                        [sys.executable, str(Path.home() / ".hermes/scripts/bus_client.py"), "write", "monitor_wf_health",
                         f"[wf-metrics] {_rate_line.strip()}",
                         "--evidence", _snap_text[:800],
                         "--src", "routing_daemon"],
                        capture_output=True, timeout=10)
                except Exception as _me:
                    LOGGER.debug("workflow_metrics 快照写 bus 失败（可选组件）: %s", _me)
                last_metrics = now

            time.sleep(INTERVAL + error_backoff)
    finally:
        try:
            _pid_lock_fd.close()
            _pid_file.unlink(missing_ok=True)
        except Exception:
            pass  # must-silent: PID lock cleanup in finally block


if __name__ == "__main__":
    main()
