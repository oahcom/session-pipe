#!/usr/bin/env python3
"""
Routing Database — SQLite 持久化路由表。

路由表结构：
- routing 表：角色 -> {produce: [...], consume: [...]}
- routing_audit 表：变更审计日志

数据库路径：默认 ~/.hermes/state/routing.db，可用 ROUTING_DB_PATH 环境变量覆盖。
"""

import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


def _get_db_path() -> Path:
    """获取数据库路径，支持环境变量覆盖。"""
    env_path = os.environ.get("ROUTING_DB_PATH")
    if env_path:
        return Path(env_path).expanduser()
    return Path.home() / ".hermes" / "state" / "routing.db"


DB_PATH = _get_db_path()


def get_db_path() -> Path:
    """获取当前数据库路径。"""
    return DB_PATH


class RoutingDB:
    """路由表 SQLite 数据库。"""

    def __init__(self, db_path: str | Path = DB_PATH):
        self._lock = threading.RLock()
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), timeout=10, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _init_schema(self):
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS routing (
                role TEXT PRIMARY KEY,
                produce TEXT NOT NULL,      -- JSON array
                consume TEXT NOT NULL,      -- JSON array
                updated_at REAL NOT NULL,
                updated_by TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS routing_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                role TEXT NOT NULL,
                field TEXT NOT NULL,        -- 'produce' / 'consume'
                old_value TEXT,
                new_value TEXT,
                changed_by TEXT DEFAULT '',
                changed_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_audit_role ON routing_audit(role);
            CREATE INDEX IF NOT EXISTS idx_audit_changed_at ON routing_audit(changed_at);
        """)
        self._conn.commit()

    def load_routing(self) -> dict:
        """从 DB 加载全部路由表。

        Returns:
            dict: {role: {"produce": [...], "consume": [...]}}
        """
        rows = self._conn.execute(
            "SELECT role, produce, consume FROM routing"
        ).fetchall()
        routing = {}
        for row in rows:
            routing[row["role"]] = {
                "produce": json.loads(row["produce"]),
                "consume": json.loads(row["consume"]),
            }
        return routing

    def _save_routing_impl(self, role: str, produce: list[str], consume: list[str], changed_by: str = "") -> bool:
        """save_routing 的 SQL 体（不含 commit），供单事务批量复用。"""
        now = time.time()
        produce_json = json.dumps(produce, ensure_ascii=False)
        consume_json = json.dumps(consume, ensure_ascii=False)

        # 检查现有记录
        row = self._conn.execute(
            "SELECT produce, consume FROM routing WHERE role=?", (role,)
        ).fetchone()

        if row:
            old_produce = row["produce"]
            old_consume = row["consume"]
            if old_produce == produce_json and old_consume == consume_json:
                return False  # 无变更

            # 写入 audit 日志
            if old_produce != produce_json:
                self._conn.execute("""
                    INSERT INTO routing_audit (role, field, old_value, new_value, changed_by, changed_at)
                    VALUES (?, 'produce', ?, ?, ?, ?)
                """, (role, old_produce, produce_json, changed_by, now))
            if old_consume != consume_json:
                self._conn.execute("""
                    INSERT INTO routing_audit (role, field, old_value, new_value, changed_by, changed_at)
                    VALUES (?, 'consume', ?, ?, ?, ?)
                """, (role, old_consume, consume_json, changed_by, now))

            self._conn.execute("""
                UPDATE routing SET produce=?, consume=?, updated_at=?, updated_by=?
                WHERE role=?
            """, (produce_json, consume_json, now, changed_by, role))
        else:
            # 新插入
            self._conn.execute("""
                INSERT INTO routing (role, produce, consume, updated_at, updated_by)
                VALUES (?, ?, ?, ?, ?)
            """, (role, produce_json, consume_json, now, changed_by))
            # audit 日志
            self._conn.execute("""
                INSERT INTO routing_audit (role, field, old_value, new_value, changed_by, changed_at)
                VALUES (?, 'produce', '', ?, ?, ?)
            """, (role, produce_json, changed_by, now))
            self._conn.execute("""
                INSERT INTO routing_audit (role, field, old_value, new_value, changed_by, changed_at)
                VALUES (?, 'consume', '', ?, ?, ?)
            """, (role, consume_json, changed_by, now))

        return True

    def save_routing(self, role: str, produce: list[str], consume: list[str], changed_by: str = "") -> bool:
        """插入或更新单个角色路由，并写入 audit 日志。

        Returns:
            bool: True if inserted/updated, False if no change.
        """
        changed = self._save_routing_impl(role, produce, consume, changed_by)
        self._conn.commit()
        return changed

    def _delete_routing_impl(self, role: str, changed_by: str = "") -> bool:
        """delete_routing 的 SQL 体（不含 commit），供单事务批量复用。"""
        row = self._conn.execute(
            "SELECT produce, consume FROM routing WHERE role=?", (role,)
        ).fetchone()
        if not row:
            return False

        now = time.time()
        self._conn.execute("""
            INSERT INTO routing_audit (role, field, old_value, new_value, changed_by, changed_at)
            VALUES (?, 'produce', ?, '', ?, ?)
        """, (role, row["produce"], changed_by, now))
        self._conn.execute("""
            INSERT INTO routing_audit (role, field, old_value, new_value, changed_by, changed_at)
            VALUES (?, 'consume', ?, '', ?, ?)
        """, (role, row["consume"], changed_by, now))

        self._conn.execute("DELETE FROM routing WHERE role=?", (role,))
        return True

    def delete_routing(self, role: str, changed_by: str = "") -> bool:
        """删除角色路由并记录 audit。"""
        changed = self._delete_routing_impl(role, changed_by)
        self._conn.commit()
        return changed

    def set_routing_table(self, table: dict, changed_by: str = "") -> int:
        """批量写入整个路由表（替换模式）。

        Args:
            table: {role: {"produce": [...], "consume": [...]}}
            changed_by: 变更来源标识

        Returns:
            int: 变更数量（新增/更新/删除总和）
        """
        now = time.time()
        changes = 0

        # 获取现有角色
        existing = set(row["role"] for row in self._conn.execute("SELECT role FROM routing"))
        new_roles = set(table.keys())

        # 单事务批量写入：全部 SQL 体执行完一次 commit，异常时 rollback
        try:
            for role in existing - new_roles:
                if self._delete_routing_impl(role, changed_by):
                    changes += 1
            for role, data in table.items():
                produce = data.get("produce", [])
                consume = data.get("consume", [])
                if self._save_routing_impl(role, produce, consume, changed_by):
                    changes += 1
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

        return changes

    def audit_log(self, limit: int = 50) -> list[dict]:
        """查看最近的变更记录。"""
        rows = self._conn.execute("""
            SELECT id, role, field, old_value, new_value, changed_by, changed_at
            FROM routing_audit
            ORDER BY changed_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        self._conn.close()


# ────────────────────────── 模块级便捷函数 ──────────────────────────

def init_db() -> RoutingDB:
    """初始化数据库（建表），返回 DB 实例。"""
    return RoutingDB()


def load_routing() -> dict:
    """从 DB 加载全部路由表。"""
    with RoutingDB() as db:
        return db.load_routing()


def save_routing(role: str, produce: list[str], consume: list[str], changed_by: str = "") -> bool:
    """保存单个角色路由。"""
    with RoutingDB() as db:
        return db.save_routing(role, produce, consume, changed_by)


def delete_routing(role: str, changed_by: str = "") -> bool:
    """删除单个角色路由。"""
    with RoutingDB() as db:
        return db.delete_routing(role, changed_by)


def set_routing_table(table: dict, changed_by: str = "") -> int:
    """批量写入整个路由表。"""
    with RoutingDB() as db:
        return db.set_routing_table(table, changed_by)


def audit_log(limit: int = 50) -> list[dict]:
    """查看最近变更记录。"""
    with RoutingDB() as db:
        return db.audit_log(limit)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m routing_db <load|save|delete|set|audit> [args...]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "load":
        print(json.dumps(load_routing(), ensure_ascii=False, indent=2))
    elif cmd == "save" and len(sys.argv) >= 5:
        role = sys.argv[2]
        produce = json.loads(sys.argv[3])
        consume = json.loads(sys.argv[4])
        by = sys.argv[5] if len(sys.argv) > 5 else ""
        print(save_routing(role, produce, consume, by))
    elif cmd == "delete" and len(sys.argv) >= 3:
        role = sys.argv[2]
        by = sys.argv[3] if len(sys.argv) > 3 else ""
        print(delete_routing(role, by))
    elif cmd == "set":
        # 从 stdin 读 JSON
        table = json.load(sys.stdin)
        by = sys.argv[2] if len(sys.argv) > 2 else ""
        print(set_routing_table(table, by))
    elif cmd == "audit":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50
        print(json.dumps(audit_log(limit), ensure_ascii=False, indent=2))
    else:
        print("Unknown command")
        sys.exit(1)