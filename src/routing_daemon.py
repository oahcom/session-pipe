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
import fcntl, importlib, importlib.util, json, os, subprocess, sys, time
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
INVESTIGATOR_INTERVAL = 900  # 缺陷移交分派间隔（每 15 分钟），生长 dispatch_investigator
CONSISTENCY_INTERVAL = 3600  # 路由一致性校验间隔（每 1 小时），生长 consistency.py
CONTRACT_INTERVAL = 3600  # 合约自改进闭环间隔（每 1 小时），生长 contract_updater.py
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
    
    增强版：增加死角色检测，防止盲发导致 ccs_send_fallback 风暴。
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

    def _is_role_alive(role: str) -> bool:
        """检查角色是否存活（alive=true），防止盲发死角色。"""
        try:
            _sr = subprocess.run(
                [sys.executable, str(launcher), "status-role", role],
                capture_output=True, text=True, timeout=10,
            )
            if _sr.returncode != 0:
                LOGGER.debug("status-role 返回非零: role=%s, rc=%d", role, _sr.returncode)
                return False
            import json
            try:
                status = json.loads(_sr.stdout)
                return status.get("alive", False) is True
            except json.JSONDecodeError:
                LOGGER.warning("status-role 返回非 JSON: role=%s, stdout=%s", role, _sr.stdout[:200])
                return False
        except subprocess.TimeoutExpired:
            LOGGER.warning("status-role 超时: role=%s", role)
            return False
        except Exception as e:
            LOGGER.warning("status-role 检查异常: role=%s, err=%s", role, e)
            return False

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
                # 种子用 now（非 now-60）：get_prev 返回严格早于种子的调度点。
                # 旧写法 seed=now-60 使 prev <= now-60，而 now_minute > now-60 恒成立
                # → `now_minute <= prev` 恒假 → 本函数永不触发（实测 * * * * * 触发 0 次，
                # subprocess.run 调用 0 次）。判据改为“调度点落在当前这一分钟内”。
                cron = croniter.croniter(cron_exp, now)
                prev = cron.get_prev(float)
                if now_minute <= prev < now_minute + 60:
                    # 去重：同一角色同一分钟不重复触发
                    key = f"{role}:{cron_exp}"
                    last = triggered.get(key, 0)
                    if now - last > 60:
                        triggered[key] = now
                        # 强化死角色检测：防止盲发导致 ccs_send_fallback 刷屏
                        if not _is_role_alive(role):
                            LOGGER.info("cron_schedule 跳过死角色: %s (not running)", role)
                            continue
                        LOGGER.info("cron_schedule 触发: %s (%s)", role, cron_exp)
                        # 必须显式 --from：ccs.py 的 --from 默认值是 "cli"，
                        # 而 core.send 把 "cli"/"user" 当人类来源 → 触发意图提醒注入
                        # 并**刷新该角色的用户请求痕迹 ts**。痕迹是意图守护 daemon 的
                        # 触发源（ts 即"最近一次用户请求"），被定时任务刷新后，同一请求
                        # 会被当成"新一轮请求"→ 预算重置 → 提醒无限重复。
                        subprocess.run(
                            [sys.executable, str(launcher), "send", role,
                             f"定时任务触发 (cron: {cron_exp})",
                             "--from", "routing_daemon"],
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
    last_consistency = 0
    last_contract = 0
    last_investigator = 0
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
                        _ec_rc = _ec_main(["--json"])
                    # 结果必须可见：曾用 LOGGER.debug，而运行期有效级别是 INFO
                    # → 输出永不落盘（实测 30 天 journal 里 0 条），且 _ec_main 的
                    # 返回值被丢弃。该预检的全部意义就是"发现不兼容项"，用不可见
                    # 级别打印等于没做——正是它自己要防的那种静默。
                    if _ec_rc:
                        LOGGER.warning("eval_consistency 预检发现不兼容项 (rc=%s): %s",
                                       _ec_rc, _ec_buf.getvalue()[:400])
                    else:
                        LOGGER.info("eval_consistency 预检通过")
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
                    _old_mtime = 0.0  # Fix: initialize to avoid UnboundLocalError on exception path
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
                        # Fix: ensure _old_mtime is defined even on fallback failure
                        _old_mtime = 0.0
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
                    # P0_draft 阶梯扫描：1h 升级、4h 降级、50min 预警
                    _p0.p0_audit_scan()
                    # P0 逾期升级链：6h→supervisor、12h→@everyone（已去重，可定时调用）
                    _p0._check_escalation_chain()
                except Exception as e:
                    LOGGER.error("P0 超时/阶梯检测异常: %s", e)
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
                            proposals = scan_and_propose()
                            if proposals:
                                LOGGER.info("G3 治理提案扫描: %d 个提案", len(proposals))
                                # 自动执行层：仅对 idle/violation 调用 ccs.py stop (僵尸回收)，
                                # content_void 仅提醒不杀 (安全边界: content_void 噪音风险 > 僵尸)
                                for p in proposals:
                                    action = p.get("_action")
                                    role = p.get("role")
                                    if action in ("idle", "violation") and role:
                                        try:
                                            subprocess.run(
                                                [sys.executable, str(_paths_mod.CCS_CLI), "stop", role],
                                                capture_output=True, timeout=30)
                                            LOGGER.warning("G3 治理自动执行 stop: %s (%s)", role, action)
                                        except Exception as _e:
                                            LOGGER.error("G3 stop 失败 %s: %s", role, _e)
                            elif proposals == []:
                                LOGGER.debug("G3 治理提案扫描: 无提案")
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

            # 路由一致性校验（每小时，生长 consistency.py — 防路由漂移）
            # 复用 D6 生长接入范式：延迟导入 + try/except 不阻塞 daemon + bus 写 architecture。
            # 仅检查 in-memory 已信任路由表，不扩大 trust boundary（安全审查确认）。
            if now - last_consistency >= CONSISTENCY_INTERVAL:
                try:
                    from routing.router import get_router, CATEGORY_PRIORITY
                    from routing.consistency import (
                        check_routing_links, check_category_consistency)
                    _rt = get_router()
                    _routing = _rt._routing  # 已加载的路由表（route_all 同源）
                    _cats = set(CATEGORY_PRIORITY.keys())
                    _link_res = check_routing_links(_routing, _cats)
                    _cat_res = check_category_consistency(
                        str(Path.home() / "hermes-session-roles" / "personas" / "session-roles"),
                        _cats)
                    if not _link_res["ok"] or _cat_res:
                        _msg = (f"[routing] 一致性异常: links={_link_res} "
                                f"undef_cat={_cat_res[:3]}")
                        import subprocess as _sp
                        _sp.run(
                            [sys.executable, str(Path.home() / ".hermes/scripts/bus_client.py"),
                             "write", "architecture", _msg,
                             "--evidence", _msg, "--src", "routing_daemon"],
                            capture_output=True, timeout=10)
                        LOGGER.warning("路由一致性异常: %s", _msg)
                    else:
                        LOGGER.info("路由一致性校验通过 ✓")
                except Exception as _ce:
                    LOGGER.warning("路由一致性校验异常（不阻塞）: %s", _ce)
                last_consistency = now

            # 复用 D6 生长接入范式：subprocess 调用 contract_updater（避免循环导入），
            # 抽取最近 review_gate history 喂入自改进闭环，异常 LOG warning 不阻塞 daemon。
            if now - last_contract >= CONTRACT_INTERVAL:
                try:
                    import subprocess as _sp
                    _hist_dir = Path.home() / ".hermes" / "review_gate" / "history"
                    _feed = []
                    if _hist_dir.exists():
                        _files = sorted(_hist_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)[:3]
                        for _hf in _files:
                            _bad = 0
                            for _line in _hf.read_text(errors="replace").splitlines()[-5:]:
                                try: _feed.append(json.loads(_line))
                                except (json.JSONDecodeError, ValueError):
                                    _bad += 1
                            if _bad:
                                LOGGER.warning("contract_updater: %d malformed review_gate history lines skipped", _bad)
                    if _feed:
                        # 生成建议 → 写 bus 待 session-roles 处置。**不在此自动 --apply。**
                        # 原因：--apply 直接改写 hermes-session-roles/personas/*.json，
                        # 而本项目 CLAUDE.md:51 明令「❌ 修改角色定义（那是 session-roles 的事）」。
                        # 原实现每轮都跑 --apply 且恒失败（applied:0 failed:3，实测每小时复现），
                        # 根因是 suggest_improvements 从不产出 verification_command 而
                        # _apply_suggestion 要求它非空——即便补上，闭环也会变成"越界改角色定义"，
                        # 是把一条红线违规做成自动化。故改为把建议交给有权的角色。
                        _feed_path = Path("/tmp/contract_feed.json")
                        _feed_path.write_text(json.dumps(_feed, ensure_ascii=False))
                        _cu = _sp.run(
                            [sys.executable, str(Path(__file__).resolve().parent / "contract_updater.py"),
                             "--from-file", str(_feed_path)],
                            capture_output=True, text=True, timeout=60)
                        _sug = _cu.stdout.strip()
                        LOGGER.info("contract_updater 自改进建议: rc=%s %s",
                                    _cu.returncode, _sug[:200])
                        if _cu.returncode == 0 and _sug:
                            try:
                                _items = json.loads(_sug)
                                _summary = "; ".join(
                                    f"{s.get('role')}/{s.get('type')}"
                                    for s in (_items if isinstance(_items, list) else []))[:300]
                            except (json.JSONDecodeError, ValueError):
                                _summary = _sug[:300]
                            _sp.run(
                                [sys.executable, str(Path.home() / ".hermes/scripts/bus_client.py"),
                                 "write", "architecture",
                                 f"[contract_updater] {len(_feed)} 条 review 记录 → 自改进建议待处置",
                                 "--evidence",
                                 f"建议: {_summary}\n"
                                 f"（本项属 session-roles 职责，pipeline 不自动改角色定义）",
                                 "--src", "routing_daemon"],
                                capture_output=True, timeout=10)
                except Exception as _cu_e:
                    LOGGER.warning("contract_updater 接入异常（不阻塞）: %s", _cu_e)
                last_contract = now

            # 缺陷移交分派：把 code_fix 分类里带 needs_investigation/needs_triage 标记的
            # 条目派给 investigator。dispatch_investigator 此前只挂在 CLI（auto.py:206），
            # daemon 从不调用 → 该逻辑从未在生产推行。
            # WHY code_fix：maintainer 等重点角色用 code_fix 承载两种语义 —— 已完成回报
            # （"[deploy] wf_x 产出已部署"）与待办缺陷移交（"建议交 engineer 改 X"）。
            # routes.py:37 因前者的污染把整个 code_fix 排除出建任务集合，后者随之被丢弃，
            # 缺陷发现无人接手。此处只拾取**显式带标记**的条目，不碰已完成回报。
            if now - last_investigator >= INVESTIGATOR_INTERVAL:
                try:
                    from routing.routes import dispatch_investigator
                    _res = dispatch_investigator(category="code_fix")
                    if _res.get("dispatched"):
                        LOGGER.info("investigator 分派: %s 条（总 %s）",
                                    _res["dispatched"], _res.get("total"))
                except Exception as _iv_e:
                    LOGGER.warning("investigator 分派异常（不阻塞）: %s", _iv_e)
                last_investigator = now

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

            time.sleep(INTERVAL + error_backoff)
    finally:
        try:
            _pid_lock_fd.close()
            _pid_file.unlink(missing_ok=True)
        except Exception:
            pass  # must-silent: PID lock cleanup in finally block


if __name__ == "__main__":
    main()
