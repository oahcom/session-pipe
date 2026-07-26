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
import fcntl, json, os, sys, time
from pathlib import Path
# Ensure pipeline src is first in sys.path (overrides .pth file adding .hermes/scripts)
_PIPELINE_SRC = str(Path(__file__).resolve().parent)
if _PIPELINE_SRC not in sys.path or sys.path[0] != _PIPELINE_SRC:
    sys.path.insert(0, _PIPELINE_SRC)
# Pre-load pipeline's config_loader before .hermes/scripts shadows it
import config_loader  # noqa: F401 (caches correct version in sys.modules)

from routing.auto import route_all, route_all_to_ccs, health_check
from reliability import setup_logging, LOGGER, METRICS

SENTINEL_DIR = Path("/tmp/ccs-sentinels")
INTERVAL = 60       # route-all 间隔
CCS_INTERVAL = 300  # route-all-to-ccs 间隔
EVAL_INTERVAL = 600 # eval_criteria 检查间隔（每 10 分钟）
SURVIVAL_INTERVAL = 120  # 存活检测间隔（每 2 分钟）
CRON_SCHEDULE_TRACKER = Path("/tmp/cron_schedule_triggered.json")  # 去重跟踪


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
    launcher = Path.home() / "session-launcher" / "src" / "ccs.py"

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

    # ── 单实例 PID 锁 ──
    _pid_file = Path(f"/tmp/routing_daemon.pid")
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

    try:
        while True:
            if not _any_live_targets():
                LOGGER.warning("无存活 CCS 目标，休眠 300s")
                time.sleep(300)
                continue

            try:
                r = route_all()
                LOGGER.info("route_all: %d/%d routed", r.get("routed", 0), r.get("total", 0))
            except Exception as e:
                LOGGER.error("route_all 异常: %s", e)

            now = time.time()

            # 定时推送给 CCS + cron_schedule 消费
            if now - last_ccs >= CCS_INTERVAL:
                try:
                    r = route_all_to_ccs()
                    LOGGER.info("route_all_to_ccs: %d/%d routed", r.get("routed", 0), r.get("total", 0))
                except Exception as e:
                    LOGGER.error("route_all_to_ccs 异常: %s", e)
                try:
                    _check_cron_schedules()
                except Exception as e:
                    LOGGER.error("cron_schedule 检查异常: %s", e)
                last_ccs = now

            # 定时执行 eval_criteria 检查
            if now - last_eval >= EVAL_INTERVAL:
                if _run_eval is None:
                    try:
                        from eval_checker import run_eval_check
                        _run_eval = run_eval_check
                    except Exception:
                        _run_eval = lambda: {"checked": 0}
                try:
                    r = _run_eval()
                    LOGGER.info("eval_check: %d 条通过 / %d 条失败",
                               r.get("passed", 0), r.get("failed", 0))
                except Exception as e:
                    LOGGER.error("eval_check 异常: %s", e)
                last_eval = now

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
                    pass  # survival_monitor 可选组件
                except Exception as e:
                    LOGGER.error("survival_check 异常: %s", e)
                last_survival = now

            time.sleep(INTERVAL)
    finally:
        try:
            _pid_lock_fd.close()
            _pid_file.unlink(missing_ok=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
