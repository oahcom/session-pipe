#!/usr/bin/env python3
"""
Session Pipeline Router — 角色间消息路由。
基于 Sister Bus (Blackboard) 实现，路由规则从项目 A 角色 JSON 自动生成。

职责：
1. 从角色 JSON 的 output_targets 自动推导 produce/consume 关系
2. 优先级路由：security > code_fix > architecture > 其他
3. 消费联动：consume 时自动递减其他相关角色的待消费计数

"""
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

# ── 路径自动发现 ──
_HERMES_SCRIPTS = Path(os.environ.get(
    "HERMES_SCRIPTS_DIR",
    str(Path.home() / ".hermes" / "scripts")
))

# 项目 A 角色 JSON 目录
SESSION_ROLES_DIR = Path(os.environ.get(
    "SESSION_ROLES_ROOT",
    Path.home() / "hermes-session-roles" / "personas" / "session-roles"
))

# 路由 DB
from routing.rdb import RoutingDB as routing_db

# ── 分类优先级（数字越小越优先）──
CATEGORY_PRIORITY = {
    "security": 1,
    "security_audit": 1,
    "code_fix": 2,
    "threat_model": 2,
    "bug_report": 2,
    "code_review": 2,
    "root_cause_analysis": 2,
    "architecture": 3,
    "test_report": 3,
    "deployment_report": 3,
    "tech_decision": 3,
    "prd": 4,
    "test_plan": 4,
    "system_design": 5,
    "deployment_plan": 5,
    "task_spec": 6,
    "user_story": 6,
    "performance": 7,
    "sprint_report": 7,
    "evolution_report": 8,
    "reflexion_lesson": 9,
    "feedback": 9,
    "documentation": 9,
    "changelog": 9,
    "deception": 10,
    "standup": 10,
    "retrospective": 10,
    # ── 补充缺失的角色产出分类 ──
    "blocker": 2,
    "design_issue": 3,
    "product_design": 4,
    "scheduler": 6,
    "ccs_health": 6,
    "cleanup": 9,
    "skill_audit": 9,
    "optimization": 7,
    "monitor_dashboard": 9,
    "knowledge_distill": 9,
    "memory_store": 9,
    "verification": 9,
    "notice": 9,
    "workflow": 6,
    "report": 9,
    "vuln_report": 3,
    "debate": 9,
}
_DEFAULT_PRIORITY = 11

# ── 分类描述 ──
CATEGORY_DESC: dict[str, str] = {
    "reflexion_lesson": "经验教训（消费者沉淀）",
    "code_fix": "代码修复（维护者/开发者产出）",
    "architecture": "架构决策/新发现（侦察兵/管理者产出）",
    "prd": "产品需求文档（架构师产出）",
    "system_design": "系统设计文档（架构师产出）",
    "task_spec": "任务分解规范（架构师产出）",
    "product_design": "产品设计触发（需架构师消费）",
    "evolution_report": "进化轮次报告（侦察兵产出）",
    "security": "安全告警",
    "performance": "性能发现",
    "deception": "欺骗检测",
    "monitor_audit": "CCS 监控审计记录（LLM 决策追踪）",
    "notice": "系统通知消息（可终局检测）",
    "blocker": "阻塞问题（任何人可发，需升级处理）",
    "design_issue": "设计问题（开发者/消费者发，架构师消费）",
    "ops": "运维事故（关闭者产出）",
    "user_story": "用户故事（PM产出）",
    "code_review": "PR 代码审查（engineer产出，reviewer消费）",
    "test_plan": "测试计划（qa产出）",
    "test_report": "测试报告（qa产出）",
    "bug_report": "缺陷报告（qa产出）",
    "deployment_plan": "部署计划（devops产出）",
    "deployment_report": "部署结果（devops产出）",
    "security_audit": "安全审计报告（security产出）",
    "threat_model": "威胁模型（security产出）",
    "documentation": "技术文档（writer产出）",
    "changelog": "变更日志（writer产出）",
    "standup": "每日站会简报（coordinator产出）",
    "retrospective": "迭代复盘（coordinator产出）",
    "sprint_report": "迭代报告（coordinator产出）",
    "feedback": "用户反馈（pm产出）",
    "root_cause_analysis": "根因分析报告（investigator产出）",
    "tech_decision": "技术选型决策（lr产出）",
    "skill_audit": "技能审计报告（curator产出）",
    "cleanup": "清理报告（curator产出）",
    "scheduler": "排期分配（coordinator产出）",
    "ccs_health": "CCS 健康报告（coordinator产出）",
    "optimization": "优化建议（optimizer产出）",
    "monitor_dashboard": "监控仪表盘（devops产出）",
    "knowledge_distill": "知识蒸馏报告（knowledge_curator产出）",
    "memory_store": "记忆存储报告（knowledge_curator产出）",
    "verification": "验证报告（debate_verifier产出）",
    "report": "通用报告（browser-harness产出）",
    "vuln_report": "漏洞报告（security产出）",
    "debate": "辩论记录（debate_verifier产出）",
    "workflow": "工作流事件（pipeline路由）",
}


def priority(category: str) -> int:
    """返回分类优先级（数字越小越优先）。"""
    return CATEGORY_PRIORITY.get(category, _DEFAULT_PRIORITY)


def _parse_produce_categories(output_targets: list[str]) -> list[str]:
    """从 output_targets 提取产出分类。

    "bus cat=code_fix 修复方案" → ["code_fix"]
    "git commit" → []（忽略非 bus 目标）
    "bus consume" → []（消费动作，非产出）
    """
    cats: list[str] = []
    for target in output_targets:
        m = re.search(r"bus cat=(\w+)", target)
        if m:
            cat = m.group(1)
            if cat not in cats:
                cats.append(cat)
    return cats


def _parse_consume_categories(input_signals: list[dict]) -> list[str]:
    """从 input_signals 提取消费分类。

    新格式：
      {"type": "bus", "spec": {"category": "security"}} → ["security"]
      {"type": "bus", "spec": {"category": "*"}} → ["*"]
    旧格式（兼容）：
      {"source": "bus cat=security"} → ["security"]
      {"source": "bus_client.py read --cat security"} → ["security"]
      {"source": "bus_client.py unread --all"} → ["*"]
      {"source": "systemctl ..."} → []（非 bus 信号）
    """
    cats: list[str] = []
    for signal in input_signals:
        # 新格式：type == "bus" 且 spec.category 存在
        sig_type = signal.get("type", "")
        if sig_type == "bus":
            spec = signal.get("spec", {})
            category = spec.get("category", "")
            if category:
                if category == "*":
                    if "*" not in cats:
                        cats.append("*")
                elif category not in cats:
                    cats.append(category)
                continue

        # 旧格式：source 字段（type为空 或 type=bus但category为空时 fallback）
        source = signal.get("source", "")
        # 全量消费标记
        if "unread --all" in source:
            if "*" not in cats:
                cats.append("*")
            continue
        # bus cat=xxx 模式（\w+ 不匹配 *，单独处理）
        if "bus cat=*" in source:
            if "*" not in cats:
                cats.append("*")
            continue
        for m in re.finditer(r"bus cat=(\w+)", source):
            cat = m.group(1)
            if cat not in cats:
                cats.append(cat)
        # bus_client.py read --cat xxx 模式
        for m in re.finditer(r"--cat (\w+)", source):
            cat = m.group(1)
            if cat not in cats:
                cats.append(cat)
    return cats


class Router:
    """路由表：优先从 SQLite 加载，fallback 到角色 JSON 目录。"""

    def __init__(self, roles_dir: Path = SESSION_ROLES_DIR):
        self.roles_dir = roles_dir
        self._routing = self._load_from_db_or_build()

    def _load_from_db_or_build(self) -> dict:
        """优先从 DB 加载路由表（仅默认目录），fallback 到角色 JSON 目录。"""
        if str(self.roles_dir.resolve()) == str(SESSION_ROLES_DIR.resolve()):
            try:
                db_routing = routing_db.load_routing()
                if db_routing:
                    return db_routing
            except Exception:
                pass
        return self._build_routing()

    def load_from_db(self) -> dict:
        """重新从 DB 加载路由表，更新 self._routing。"""
        try:
            db_routing = routing_db.load_routing()
            if db_routing:
                self._routing = db_routing
        except Exception:
            pass
        return self._routing

    def register_role(self, role: str, produce: list[str], consume: list[str], changed_by: str = "") -> bool:
        """注册/更新角色路由（同时写入 DB 和内存）。"""
        self._routing[role] = {"produce": produce, "consume": consume}
        return routing_db.save_routing(role, produce, consume, changed_by)

    def _build_routing(self) -> dict:
        """从角色 JSON 文件构建路由表。

        回退：若目录不存在或为空，返回空 dict（由调用方决定后续行为）。
        """
        if not self.roles_dir.exists():
            return {}

        routing: dict = {}
        for json_file in sorted(self.roles_dir.glob("*.json")):
            try:
                with open(json_file) as f:
                    persona = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue

            name = persona.get("name")
            if not name:
                continue

            produce = _parse_produce_categories(persona.get("output_targets", []))
            consume = _parse_consume_categories(persona.get("input_signals", []))

            routing[name] = {"produce": produce, "consume": consume}

        # Extend routing from Browser Harness config
        routing = self._extend_from_bh_config(routing)

        return routing

    def _extend_from_bh_config(self, routing: dict) -> dict:
        """Extend routing with Browser Harness profiles.

        Loads _bh_route_config.json (relative to roles_dir parent/personas/browser-harness/)
        and adds produce categories for each BH profile mapped to a session role.
        """
        # Try roles_dir-relative path first, fallback to global
        local_bh = self.roles_dir.parent / "browser-harness" / "_bh_route_config.json"
        bh_config_path = Path(os.environ.get("BH_ROUTE_CONFIG", str(local_bh)))
        if not bh_config_path.exists():
            return routing
        try:
            bh_data = json.loads(bh_config_path.read_text())
        except (json.JSONDecodeError, OSError):
            return routing

        profiles = bh_data.get("bh_profiles", {})
        for profile_name, profile_data in profiles.items():
            if not profile_data.get("enabled", True):
                continue
            sr_mapping = profile_data.get("sr_mapping")
            if not sr_mapping:
                continue
            bh_produce = profile_data.get("route", {}).get("produce", [])
            if not bh_produce:
                continue
            if sr_mapping not in routing:
                routing[sr_mapping] = {"produce": [], "consume": []}
            existing_produce = routing[sr_mapping].get("produce", [])
            for cat in bh_produce:
                if cat not in existing_produce:
                    existing_produce.append(cat)
            routing[sr_mapping]["produce"] = existing_produce

        return routing

    @property
    def routing(self) -> dict:
        return self._routing

    def get_producers(self, category: str) -> list[str]:
        """返回某类消息的生产者列表。"""
        return [
            role for role, r in self._routing.items()
            if category in r.get("produce", [])
        ]

    def get_consumers(self, category: str) -> list[str]:
        """返回某类消息的消费者列表。"""
        return [
            role for role, r in self._routing.items()
            if "*" in r.get("consume", []) or category in r.get("consume", [])
        ]

    def get_consumers_prioritized(self, category: str) -> list[str]:
        """返回按优先级排序的消费者列表。

        规则：
        1. 消费该分类的消费者
        2. 按优先级排序：security > code_fix > architecture > 其他
        3. consume="*"（通吃一切）的角色排最后
        """
        consumers = self.get_consumers(category)
        cat_prio = priority(category)
        # 先按分类优先级，通吃角色排后
        consumers.sort(key=lambda r: (
            self._routing.get(r, {}).get("consume", []).count("*"),  # 通吃角色排后
            cat_prio,  # 同类按分类优先级
        ))
        return consumers

    def role_produce_categories(self, role: str) -> list[str]:
        """返回指定角色可产出的分类列表。"""
        return self._routing.get(role, {}).get("produce", [])

    def role_consume_categories(self, role: str) -> list[str]:
        """返回指定角色可消费的分类列表（* 展开为全量分类）。"""
        cats = self._routing.get(role, {}).get("consume", [])
        if "*" in cats:
            return list(CATEGORY_DESC.keys())
        return cats

    def unconsumed_by_role(self, role: str, limit: int = 20) -> list[dict]:
        """返回指定角色应消费的未消费消息。改用 Blackboard 直接 API。"""
        from bus_protocol import Blackboard

        bb = Blackboard()
        cats = self.role_consume_categories(role)
        try:
            facts = bb.unconsumed()
            messages: list[dict] = []
            for f in facts:
                if not cats or f.cat in cats or "*" in cats:
                    messages.append({
                        "id": f.id,
                        "category": f.cat,
                        "text": f.t[:100],
                        "evidence": f.e[:120] if f.e else "",
                        "role": role,
                        "priority": priority(f.cat),
                    })
            # 按优先级排序
            messages.sort(key=lambda m: m["priority"])
            return messages[:limit]
        except Exception as e:
            return [{"error": str(e)}]

    def routing_summary(self) -> dict:
        """返回当前路由拓扑摘要。"""
        result: dict = {}
        for role, r in self._routing.items():
            result[role] = {
                "produce": r.get("produce", []),
                "consume": r.get("consume", []),
                "consumers": [
                    cr for cr in self._routing
                    if ("*" in self._routing[cr].get("consume", [])
                        or any(c in self._routing[cr].get("consume", [])
                               for c in r.get("produce", [])))
                ],
            }
        return result

    def format_pipeline(self) -> str:
        """格式化流水线图（人类可读）。"""
        lines = ["=== Session 角色产出流水线（自动生成）===\n"]
        for role, r in self._routing.items():
            produce = r.get("produce", [])
            consume = r.get("consume", [])
            produce_str = ", ".join(CATEGORY_DESC.get(c, c) for c in produce) if produce else "（无）"
            if "*" in consume:
                consume_str = "所有分类"
            else:
                consume_str = ", ".join(CATEGORY_DESC.get(c, c) for c in consume) if consume else "（无）"
            lines.append(f"  {role}: 产出={produce}  消费={consume}")

        return "\n".join(lines)

    def consume_linkage(self, fact_id: int, category: str) -> list[str]:
        """消费一条消息后，返回其他受影响的消费者列表。

        消费联动逻辑：
        角色 A consume 了分类 X 的消息后，
        其他也消费分类 X 的角色计数自动递减（由 caller 实现标记）。
        """
        consumers = self.get_consumers(category)
        return consumers


# ── 全局单例（懒加载） ──
_router: Optional[Router] = None


def get_router() -> Router:
    """获取（或创建）全局 Router 单例。"""
    global _router
    if _router is None:
        _router = Router()
    return _router


# ── 兼容旧接口（路由函数） ──

def get_producers(category: str) -> list[str]:
    return get_router().get_producers(category)


def get_consumers(category: str) -> list[str]:
    return get_router().get_consumers(category)


def role_produce_categories(role: str) -> list[str]:
    return get_router().role_produce_categories(role)


def role_consume_categories(role: str) -> list[str]:
    return get_router().role_consume_categories(role)


def unconsumed_by_role(role: str, limit: int = 20) -> list[dict]:
    return get_router().unconsumed_by_role(role, limit=limit)


def routing_summary() -> dict:
    return get_router().routing_summary()


def format_pipeline() -> str:
    return get_router().format_pipeline()


def category_priority(cat: str) -> int:
    """返回分类的优先级分数（数字越小越优先）。"""
    return priority(cat)


def main():
    """CLI 入口：register / list / audit。"""
    import argparse

    parser = argparse.ArgumentParser(description="Session Pipeline Router CLI")
    sub = parser.add_subparsers(dest="cmd")

    reg = sub.add_parser("register", help="注册/更新角色路由")
    reg.add_argument("role", help="角色名称")
    reg.add_argument("--produce", required=True, help="产出分类，逗号分隔")
    reg.add_argument("--consume", required=True, help="消费分类，逗号分隔")
    reg.add_argument("--by", default="cli", help="变更来源标识")

    sub.add_parser("list", help="列出路由表")

    aud = sub.add_parser("audit", help="显示审计历史")
    aud.add_argument("role", nargs="?", help="按角色过滤")
    aud.add_argument("-n", "--limit", type=int, default=20, help="条目上限")

    args = parser.parse_args()

    if args.cmd == "register":
        produce = [c.strip() for c in args.produce.split(",") if c.strip()]
        consume = [c.strip() for c in args.consume.split(",") if c.strip()]
        r = get_router()
        r.register_role(args.role, produce, consume, changed_by=args.by)
        print(json.dumps({"ok": True, "role": args.role, "produce": produce, "consume": consume},
                         ensure_ascii=False, indent=2))

    elif args.cmd == "list":
        routing = routing_db.load_routing()
        if not routing:
            print("(empty — no roles in DB)")
        else:
            for role, data in sorted(routing.items()):
                print(f"  {role}: produce={data['produce']}  consume={data['consume']}")

    elif args.cmd == "audit":
        logs = routing_db.audit_log(args.limit)
        if args.role:
            logs = [l for l in logs if l["role"] == args.role]
        if not logs:
            print("(no audit records)")
        else:
            for l in logs:
                ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(l["changed_at"]))
                print(f"  [{ts}] {l['role']}.{l['field']}: {l['old_value']!r} -> {l['new_value']!r} (by {l['changed_by']})")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
