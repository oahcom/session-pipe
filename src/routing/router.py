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
import sys
import time
from pathlib import Path
from typing import Optional

from paths import HERMES_STATE, SESSION_ROLES_ROOT

# ── 角色路由数据：使用 shared_loader 的导出结果 ──
# 不再直接 parse 角色 JSON（hermes-session-roles 的 shared_loader 负责解析）
_ROLES_EXPORT_PATH = HERMES_STATE.parent / "data" / "roles_export.json"

def _load_roles_export() -> dict:
    """加载 shared_loader 导出的角色路由数据。"""
    if _ROLES_EXPORT_PATH.exists():
        try:
            data = json.loads(_ROLES_EXPORT_PATH.read_text())
            return {r["name"]: r for r in data.get("roles", [])}
        except (json.JSONDecodeError, OSError) as _e:
            print(f"  [router] WARNING: roles_export 加载失败: {_e}", file=sys.stderr)
    return {}

# ── 路由 DB（全局单例） ──
from routing.rdb import RoutingDB
routing_db = RoutingDB()

# ── 规范Bus分类注册表 ──
_CANONICAL_PATH = Path(os.environ.get(
    "BUS_CANONICAL_PATH",
    str(HERMES_STATE.parent / "data" / "bus_canonical.json")
))
if _CANONICAL_PATH.exists():
    try:
        _CANONICAL = json.loads(_CANONICAL_PATH.read_text())["categories"]
    except (json.JSONDecodeError, KeyError) as _e:
        print(f"  [router] WARNING: bus_canonical 解析失败: {_e}", file=sys.stderr)
        _CANONICAL = {}
else:
    _CANONICAL = {}

CATEGORY_PRIORITY: dict[str, int] = {
    cat: info["priority"]
    for cat, info in _CANONICAL.items()
}
_DEFAULT_PRIORITY = 11

CATEGORY_DESC: dict[str, str] = {
    cat: info["description"]
    for cat, info in _CANONICAL.items()
}


def priority(category: str) -> int:
    """返回分类优先级（数字越小越优先）。"""
    return CATEGORY_PRIORITY.get(category, _DEFAULT_PRIORITY)


# 完整性阈值：DB 路由角色数低于此值视为被污染（如误清空只剩 test_qa），
# 回退到 shared_loader 导出，防止"DB 非空即信任"导致 route_all 瘫痪。
_DB_MIN_ROLES = 5


class Router:
    """路由表：优先从 SQLite 加载，fallback 到 shared_loader 导出。"""

    def __init__(self):
        self._routing = self._load_from_db_or_build()

    def _db_ok(self, db_routing: dict) -> bool:
        """DB 路由完整：非空且角色数不低于阈值。"""
        return bool(db_routing) and len(db_routing) >= _DB_MIN_ROLES

    def _load_from_db_or_build(self) -> dict:
        """优先从 DB 加载路由表，fallback 到 shared_loader 导出。

        添加完整性检查: DB 路由角色数 < _DB_MIN_ROLES 时回退到导出，
        防止路由.db 只剩 test_qa 导致 route_all 瘫痪。
        """
        try:
            db_routing = routing_db.load_routing()
            if self._db_ok(db_routing):
                return db_routing
        except Exception as _e:
            print(f"  [router] WARNING: DB 加载路由失败: {_e}", file=sys.stderr)
        return self._build_from_export()

    def load_from_db(self) -> dict:
        """重新从 DB 加载路由表，更新 self._routing。"""
        try:
            db_routing = routing_db.load_routing()
            if self._db_ok(db_routing):
                self._routing = db_routing
        except Exception as _e:
            print(f"  [router] WARNING: DB 加载路由失败: {_e}", file=sys.stderr)
        return self._routing

    def register_role(self, role: str, produce: list[str], consume: list[str], changed_by: str = "") -> bool:
        """注册/更新角色路由（同时写入 DB 和内存）。"""
        self._routing[role] = {"produce": produce, "consume": consume}
        return routing_db.save_routing(role, produce, consume, changed_by)

    def _build_from_export(self) -> dict:
        """从 shared_loader 导出的 roles_export.json 构建路由表。"""
        roles = _load_roles_export()
        if not roles:
            return {}

        routing: dict = {}
        for name, r in roles.items():
            produce = r.get("routing", {}).get("produce", [])
            consume = r.get("routing", {}).get("consume", [])
            routing[name] = {"produce": produce, "consume": consume}

        # Extend routing from Browser Harness config
        routing = self._extend_from_bh_config(routing)
        return routing

    def _extend_from_bh_config(self, routing: dict) -> dict:
        """Extend routing with Browser Harness profiles. (from _bh_route_config.json)"""
        bh_config_path = Path(os.environ.get(
            "BH_ROUTE_CONFIG",
            str(SESSION_ROLES_ROOT / "personas" / "browser-harness" / "_bh_route_config.json")
        ))
        if not bh_config_path.exists():
            return routing
        try:
            bh_data = json.loads(bh_config_path.read_text())
        except (json.JSONDecodeError, OSError) as _e:
            print(f"  [router] WARNING: BH config 加载失败: {_e}", file=sys.stderr)
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
        """返回某类消息的消费者列表。

        优先从路由表获取；路由表为空时使用默认映射（dashboard 告警→
        maintainer，verification→investigator）确保告警不被遗弃。
        """
        consumers = [
            role for role, r in self._routing.items()
            if "*" in r.get("consume", []) or category in r.get("consume", [])
        ]
        if consumers:
            return consumers
        # 路由表为空时：维护告警始终路由给 maintainer（闭环保证）
        # ponytail: 路由表填充后移除此 fallback
        _FALLBACK = {
            "monitor_dashboard": ["maintainer"],
            "monitor_wf_health": ["maintainer"],
            "verification+notice": ["coordinator"],
        }
        return _FALLBACK.get(category, [])

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

    def consume_linkage(self, category: str) -> list[str]:
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
