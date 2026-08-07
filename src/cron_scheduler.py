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


_IDLE_PATTERNS = ("积压清零", "无操作项", "无异常", "无待办", "无卡住", "无风险",
                   "无僵尸", "积压 0", "积压:0", "积压为零", "no backlog",
                   "无新增", "无变化", "一切正常", "all clear")

_IDLE_STATE_PATH = Path.home() / ".hermes" / "run" / "cron_idle_state.json"
_IDLE_COOLDOWN = 24 * 3600  # 24小时冷却


class CronScheduler:
    """从 persona JSON 读取 drive=cron 角色，按 cron_schedule 触发。"""

    def __init__(self):
        self._last_fired: dict[str, float] = {}
        self._roles: list[dict] = []
        self._idle_counts: dict[str, int] = {}
        self._last_idle_value: dict[str, float] = {}
        self._load_idle_state()
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

    def _load_idle_state(self):
        """加载空转状态：连续空转次数 + 冷却截止时间。"""
        try:
            if _IDLE_STATE_PATH.exists():
                d = json.loads(_IDLE_STATE_PATH.read_text())
                self._idle_counts = d.get("counts", {})
                self._last_idle_value = d.get("cooldown_until", {})
        except Exception as e:
            LOGGER.warning("idle state load failed: %s", e)

    def _save_idle_state(self):
        try:
            _IDLE_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            _IDLE_STATE_PATH.write_text(json.dumps(
                {"counts": self._idle_counts, "cooldown_until": self._last_idle_value},
                ensure_ascii=False))
        except Exception as e:
            LOGGER.warning("idle state save failed: %s", e)

    def report_idle(self, role: str):
        """角色巡检产出空报告 → 记录空转，累计达 2 次触发 24h 冷却。

        冷却触发后重置计数：否则计数永远 >= 2，冷却结束后的下一次 idle
        会立即再次触发冷却（角色 24h 只有一次触发窗口）。
        """
        role = role.replace("scheduled_tick:", "")
        self._idle_counts[role] = self._idle_counts.get(role, 0) + 1
        if self._idle_counts[role] >= 2:
            self._last_idle_value[role] = time.time() + _IDLE_COOLDOWN
            self._idle_counts[role] = 0
            LOGGER.info("cron idle: %s 连续 %d 次空转 → 冷却 %d h",
                        role, self._idle_counts[role] + 2, _IDLE_COOLDOWN // 3600)
        self._save_idle_state()

    def _in_cooldown(self, role: str) -> bool:
        until = self._last_idle_value.get(role, 0)
        if until and time.time() < until:
            LOGGER.info("cron skip: %s 空转冷却中（%d min 后解除）",
                        role, int((until - time.time()) / 60))
            return True
        return False

    def tick(self) -> list[str]:
        """检查哪些角色的 cron 到期了。返回触发的角色名列表。"""
        now = time.time()
        fired = []
        for role in self._roles:
            name = role["name"]
            last = self._last_fired.get(name, 0)
            interval = _parse_interval(role["schedule"])
            if interval and (now - last) >= interval:
                if self._in_cooldown(name):
                    continue  # 空转冷却中不触发
                self._fire(name)
                self._last_fired[name] = now
                fired.append(name)
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
