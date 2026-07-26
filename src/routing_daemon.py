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
sys.path.insert(0, str(Path(__file__).resolve().parent))

from routing.auto import route_all, route_all_to_ccs, health_check
from reliability import setup_logging, LOGGER, METRICS

SENTINEL_DIR = Path("/tmp/ccs-sentinels")
INTERVAL = 60       # route-all 间隔
CCS_INTERVAL = 300  # route-all-to-ccs 间隔
EVAL_INTERVAL = 600 # eval_criteria 检查间隔（每 10 分钟）
SURVIVAL_INTERVAL = 120  # 存活检测间隔（每 2 分钟）


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

            # 定时推送给 CCS
            if now - last_ccs >= CCS_INTERVAL:
                try:
                    r = route_all_to_ccs()
                    LOGGER.info("route_all_to_ccs: %d/%d routed", r.get("routed", 0), r.get("total", 0))
                except Exception as e:
                    LOGGER.error("route_all_to_ccs 异常: %s", e)
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
