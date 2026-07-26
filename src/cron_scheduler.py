#!/usr/bin/env python3
"""cron_scheduler.py — Cron 调度器，读取角色 JSON 的 cron_schedule 字段并触发。

让 maintainer/curator/optimizer/knowledge_curator 的 drive=cron 真正工作。
集成方式：routing_daemon 每 60s 调 tick()。
"""
import json, logging, subprocess, sys, time
from pathlib import Path

LOGGER = logging.getLogger("session-pipeline.cron_scheduler")

PERSONAS_DIR = Path.home() / "hermes-session-roles" / "personas" / "session-roles"
BUS_SCRIPT = Path.home() / ".hermes" / "scripts" / "bus_client.py"


def _parse_interval(cron_expr: str) -> int | None:
    """解析 cron 表达式为秒数。支持 */N * * * * 和固定分钟。"""
    if not cron_expr:
        return None
    try:
        parts = cron_expr.strip().split()
        if len(parts) == 5:
            if parts[0].startswith("*/"):
                return int(parts[0][2:]) * 60
            if parts[0].isdigit():
                return int(parts[0]) * 60
        return 900  # 默认 15 分钟（*/15 * * * *）
    except (ValueError, IndexError):
        return 900


class CronScheduler:
    """从 persona JSON 读取 drive=cron 角色，按 cron_schedule 触发。"""

    def __init__(self):
        self._last_fired: dict[str, float] = {}
        self._roles: list[dict] = []
        self._load_roles()

    def _load_roles(self):
        """加载所有 drive=cron 且有 cron_schedule 的角色。"""
        if not PERSONAS_DIR.is_dir():
            LOGGER.warning("personas dir not found: %s", PERSONAS_DIR)
            return
        for f in sorted(PERSONAS_DIR.glob("persona_*.json")):
            try:
                d = json.loads(f.read_text())
                if d.get("drive") == "cron" and d.get("cron_schedule"):
                    self._roles.append({
                        "name": d["name"],
                        "schedule": d["cron_schedule"],
                    })
            except Exception as e:
                LOGGER.warning("skip %s: %s", f.name, e)
        if self._roles:
            LOGGER.info("cron scheduler: loaded %d cron roles: %s",
                        len(self._roles), [r["name"] for r in self._roles])

    def tick(self) -> list[str]:
        """检查哪些角色的 cron 到期了。返回触发的角色名列表。"""
        now = time.time()
        fired = []
        for role in self._roles:
            last = self._last_fired.get(role["name"], 0)
            interval = _parse_interval(role["schedule"])
            if interval and (now - last) >= interval:
                self._fire(role["name"])
                self._last_fired[role["name"]] = now
                fired.append(role["name"])
        if fired:
            LOGGER.info("cron 触发: %s", fired)
        return fired

    def _fire(self, role: str):
        """写 bus 消息触发角色巡检。"""
        try:
            subprocess.run(
                [sys.executable, str(BUS_SCRIPT), "write", "scheduler",
                 f"scheduled_tick:{role}", "--src", "cron_scheduler"],
                capture_output=True, timeout=10,
            )
        except Exception as e:
            LOGGER.warning("cron fire %s fail: %s", role, e)
