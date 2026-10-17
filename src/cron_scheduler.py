#!/usr/bin/env python3
"""cron_scheduler.py — Cron 调度器，读取角色 JSON 的 cron_schedule 字段并触发。

让 maintainer/curator/optimizer/knowledge_curator 的 drive=cron 真正工作。
集成方式：step_handler 空转检测调用 report_idle()，无到期触发调度逻辑。
"""
import json, logging, sys, time
from pathlib import Path

LOGGER = logging.getLogger("session-pipeline.cron_scheduler")

PERSONAS_DIR = Path.home() / "hermes-session-roles" / "personas" / "session-roles"


_IDLE_PATTERNS = ("积压清零", "无操作项", "无异常", "无待办", "无卡住", "无风险",
                   "无僵尸", "积压 0", "积压:0", "积压为零", "no backlog",
                   "无新增", "无变化", "一切正常", "all clear",
                   # DESIGN §7 S1：no-op 健康报告措辞（maintainer "什么都不用做"的巡检产出）。
                   # 不加这些词，"无职责内修复动作"类报告绕过 step_handler 空转判据 →
                   # 被路由 fan-out 成新 WL-01 实例，要求为"无需动作"写技术方案（2026-10-17）。
                   # "零僵尸"(非"服务健康零僵尸")：措辞变体多（服务健康/全绿/健康全绿零僵尸），
                   # 用不变的核心子串；"服务健康零僵尸" 匹配不到 "服务全绿零僵尸"（全绿≠健康，2026-10-17）。
                   "无职责内修复", "无需维护动作", "零僵尸")

# DESIGN §4：路由层 mint 抑制专用子集。**不**复用完整 _IDLE_PATTERNS ——
# "无异常"/"无变化"/"无待办" 等泛词合法任务标题常见（如"修复无异常处理逻辑"），
# 用整表做 mint 门会误杀。只认明确表达"无需动作"的措辞。
_NOOP_PATTERNS = ("无职责内修复", "无需维护动作", "零僵尸")

_IDLE_STATE_PATH = Path.home() / ".hermes" / "run" / "cron_idle_state.json"
_IDLE_COOLDOWN = 24 * 3600  # 24小时冷却


def _is_noop_report(title: str, evidence: str = "") -> bool:
    """no-op 判据（DESIGN §4）：标题或证据命中"无需动作"措辞 → True。

    路由层 mint 前调用：no-op 报告是状态而非任务请求，为它建 workflow
    只会产出"为不做的事写方案"的空转实例。真修复报告含变更 delta 措辞，
    不命中本表 → 正常放行。
    """
    _t = f"{title} {evidence}".lower()
    return any(pat in _t for pat in _NOOP_PATTERNS)


class CronScheduler:
    """从 persona JSON 读取 drive=cron 角色，按 cron_schedule 触发。"""

    def __init__(self):
        self._roles: list[dict] = []
        self._idle_counts: dict[str, int] = {}
        self._last_idle_value: dict[str, float] = {}
        # DESIGN §7 S2：per-role 上次报告指纹。与 _last_idle_value（冷却截止
        # 时间戳，语义不同）分开存，避免改冷却语义。
        self._last_report: dict[str, str] = {}
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
                self._last_report = d.get("last_report", {})
        except Exception as e:
            LOGGER.warning("idle state load failed: %s", e)

    def _save_idle_state(self):
        try:
            _IDLE_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            _IDLE_STATE_PATH.write_text(json.dumps(
                {"counts": self._idle_counts, "cooldown_until": self._last_idle_value,
                 "last_report": self._last_report},
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

    def _noop_delta(self, role: str, report_summary: str) -> bool:
        """状态差分判据（DESIGN §4）：与上次报告指纹相同 → True（无变化，抑制）。

        emit-on-change：健康检查本身幂等（状态不变恒判绿），非幂等的是引擎
        每轮新建实例。指纹相同即"状态无 delta" → 上游不 mint。指纹不同（真变化）
        → False 放行。指纹取报告正文；调用方传 no-op 报告的可比对摘要。
        """
        _fp = (report_summary or "").strip()
        _changed = self._last_report.get(role) != _fp
        self._last_report[role] = _fp
        self._save_idle_state()
        return not _changed

    # ponytail: tick()/_fire() 已删除 — 原 cron 到期触发逻辑零生产调用方
    # (仅 test_cron_scheduler.py 覆盖)；step_handler 仅用 report_idle 做空转冷却，
    # 不触发 bus 调度。如需恢复到期触发，应接入 daemon 主循环而非遗留 test-propped 方法。
