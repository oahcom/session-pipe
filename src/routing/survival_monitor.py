#!/usr/bin/env python3
"""survival_monitor.py — L1/L2/L3 三层存活检测（可选组件）。

L1: 进程存活（tmux has-session + 哨兵文件）
L2: 思考存活（pane 最后活动时间 + token 消耗）[预留]
L3: 产出存活（bus 产出时间戳对比）[预留]

集成方式：routing_daemon 每 120s 调 tick()。
独立于 CCS 侧代码，纯外部观测。
"""
import json, logging, subprocess, time
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("session-pipeline.survival_monitor")

SENTINEL_DIR = Path("/tmp/ccs-sentinels")


class SurvivalMonitor:
    """三层存活检测器。"""

    def __init__(self):
        self._cache: dict[str, dict[str, Any]] = {}

    def _l1_check(self, role: str) -> dict:
        """L1: 检查 tmux session 和哨兵文件。"""
        tmux_name = f"ccs-{role}"
        # tmux session 存活
        alive = False
        try:
            r = subprocess.run(
                ["tmux", "has-session", "-t", tmux_name],
                capture_output=True, timeout=3,
            )
            alive = r.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        # 哨兵文件存在
        sentinel = SENTINEL_DIR / f"{role}.json"
        has_sentinel = sentinel.exists()
        if alive and has_sentinel:
            status = "ALIVE"
        elif has_sentinel and not alive:
            status = "ORPHAN"  # 哨兵残留但进程死
        elif not has_sentinel:
            status = "UNKNOWN"
        else:
            status = "DEAD"
        return {"status": status, "tmux_alive": alive, "has_sentinel": has_sentinel}

    def _l2_check(self, role: str) -> dict:
        """L2: 检查 pane 最后活动时间。[预留]"""
        tmux_name = f"ccs-{role}"
        last_activity = 0.0
        try:
            r = subprocess.run(
                ["tmux", "list-panes", "-t", tmux_name, "-F", "#{pane_activity}"],
                capture_output=True, text=True, timeout=3,
            )
            if r.returncode == 0 and r.stdout.strip():
                last_activity = float(r.stdout.strip().split("\n")[-1])
        except (FileNotFoundError, ValueError, subprocess.TimeoutExpired):
            pass
        now = time.time()
        age = now - last_activity if last_activity > 0 else -1
        return {
            "last_activity_epoch": last_activity,
            "age_seconds": age,
            "thinking": age < 300 if age >= 0 else None,
        }

    def _l3_check(self, role: str) -> dict:
        """L3: 检查角色最近是否有 bus 产出或文件变更。

        通过 bus_client 读该角色最近 bus 消息的时间戳，
        若无产出且 L2 显示活跃则认为角色在空转。
        """
        producing = None
        detail = "L3 未启用 — bus_client 不可用"
        try:
            r = subprocess.run(
                [sys.executable, str(Path.home() / ".hermes" / "scripts" / "bus_client.py"),
                 "read", "--cat", "code_fix", "--limit", "1", "--json"],
                capture_output=True, text=True, timeout=10,
            )
            if r.returncode == 0 and r.stdout.strip():
                import json as _json
                facts = _json.loads(r.stdout)
                if isinstance(facts, list) and facts:
                    last_ts = facts[0].get("created_at", 0) or facts[0].get("timestamp", 0)
                    age = time.time() - float(last_ts)
                    producing = age < 600  # 10 分钟内有过产出
                    detail = f"最近产出 {age:.0f}s 前" if producing else f"无产出 {age:.0f}s"
                else:
                    producing = None
                    detail = "bus 无匹配消息"
            else:
                producing = None
                detail = "bus_client 返回空"
        except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError,
                OSError, IndexError, ValueError) as _e:
            producing = None
            detail = f"L3 检查异常: {_e}"
        return {"producing": producing, "detail": detail}

    def _overall(self, l1: dict, l2: dict, l3: dict) -> str:
        if l1.get("status") == "DEAD":
            return "dead"
        if l1.get("status") == "ORPHAN":
            return "orphan"
        if l1.get("status") != "ALIVE":
            return "unknown"
        if l2.get("thinking") is False:
            return "stale"
        if l3.get("producing") is False:
            return "idle"
        return "healthy"

    def tick(self) -> dict[str, dict]:
        """对所有哨兵角色执行一轮存活检测。"""
        if not SENTINEL_DIR.is_dir():
            return {}
        roles = []
        for f in SENTINEL_DIR.glob("*.json"):
            try:
                d = json.loads(f.read_text())
                roles.append(d.get("role", f.stem))
            except (json.JSONDecodeError, OSError):
                continue
        now = time.time()
        results = {}
        for role in roles:
            prev = self._cache.get(role, {})
            l1 = self._l1_check(role)
            l2 = (self._l2_check(role) if l1["status"] == "ALIVE"
                  and now - prev.get("l2_ts", 0) > 120 else prev.get("l2", {}))
            l3 = (self._l3_check(role) if l2.get("thinking")
                  and now - prev.get("l3_ts", 0) > 300 else prev.get("l3", {}))
            result = {
                "l1": l1, "l1_ts": now,
                "l2": l2, "l2_ts": now if "thinking" in l2 else prev.get("l2_ts", 0),
                "l3": l3, "l3_ts": now if "producing" in l3 else prev.get("l3_ts", 0),
                "overall": self._overall(l1, l2, l3),
            }
            self._cache[role] = result
            results[role] = result
        LOGGER.debug("survival tick: %d roles checked, %d stalled",
                     len(results), sum(1 for r in results.values() if r["overall"] in ("stale", "dead")))
        return results
