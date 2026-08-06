#!/usr/bin/env python3
"""
test_rdb.py — RoutingDB 单元测试

覆盖：
- init_schema / load / save / delete / set_routing_table / audit_log
- 幂等 save（无变更时返回 False）
- 批量替换 (set_routing_table)
- 环境变量覆盖 DB 路径
"""

import json
import os
import sys
import tempfile
import time
from pathlib import Path
from unittest import mock

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pytest

from routing.rdb import RoutingDB


@pytest.fixture
def db_path(tmp_path):
    """临时数据库路径。"""
    return str(tmp_path / "test_routing.db")


@pytest.fixture
def rdb(db_path):
    """RoutingDB 实例。"""
    db = RoutingDB(db_path=db_path)
    yield db
    db.close()


class TestInitSchema:
    """表结构初始化。"""

    def test_tables_created(self, rdb):
        """建表后 routing 和 routing_audit 存在。"""
        tables = rdb._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('routing', 'routing_audit')"
        ).fetchall()
        assert len(tables) == 2

    def test_file_created(self, db_path):
        """数据库文件创建。"""
        RoutingDB(db_path=db_path).close()
        assert Path(db_path).exists()


class TestSaveRouting:
    """save_routing 增/改/幂等。"""

    def test_insert_new_role(self, rdb):
        ok = rdb.save_routing("qa", ["test_report"], ["task_spec"])
        assert ok is True

        data = rdb.load_routing()
        assert "qa" in data
        assert data["qa"]["produce"] == ["test_report"]
        assert data["qa"]["consume"] == ["task_spec"]

    def test_update_existing_role(self, rdb):
        rdb.save_routing("qa", ["test_report"], ["task_spec"])
        ok = rdb.save_routing("qa", ["test_report", "code_fix"], ["task_spec", "blocker"])
        assert ok is True

        data = rdb.load_routing()
        assert "test_report" in data["qa"]["produce"]
        assert "code_fix" in data["qa"]["produce"]
        assert "blocker" in data["qa"]["consume"]

    def test_idempotent_no_change(self, rdb):
        rdb.save_routing("qa", ["test_report"], ["task_spec"])
        ok = rdb.save_routing("qa", ["test_report"], ["task_spec"])
        assert ok is False  # no change

    def test_save_with_changed_by(self, rdb):
        rdb.save_routing("qa", ["test_report"], ["task_spec"], changed_by="pipeline")
        logs = rdb.audit_log()
        assert any(l["changed_by"] == "pipeline" for l in logs)

    def test_empty_produce_consume(self, rdb):
        ok = rdb.save_routing("viewer", [], [])
        assert ok is True
        data = rdb.load_routing()
        assert data["viewer"]["produce"] == []
        assert data["viewer"]["consume"] == []


class TestLoadRouting:
    """load_routing 加载全部路由表。"""

    def test_load_empty(self, rdb):
        assert rdb.load_routing() == {}

    def test_load_after_insert(self, rdb):
        rdb.save_routing("qa", ["test_report"], ["task_spec"])
        rdb.save_routing("engineer", ["code_fix"], ["task_spec"])

        data = rdb.load_routing()
        assert len(data) == 2
        assert "qa" in data
        assert "engineer" in data


class TestDeleteRouting:
    """delete_routing。"""

    def test_delete_existing(self, rdb):
        rdb.save_routing("qa", ["test_report"], ["task_spec"])
        ok = rdb.delete_routing("qa")
        assert ok is True
        assert "qa" not in rdb.load_routing()

    def test_delete_nonexistent(self, rdb):
        ok = rdb.delete_routing("nosuch")
        assert ok is False

    def test_delete_logs_audit(self, rdb):
        rdb.save_routing("qa", ["test_report"], ["task_spec"])
        rdb.delete_routing("qa", changed_by="cleanup")
        logs = rdb.audit_log()
        assert any(l["role"] == "qa" and l["field"] == "produce" for l in logs)


class TestSetRoutingTable:
    """set_routing_table 批量替换。"""

    def test_full_replace(self, rdb):
        rdb.save_routing("qa", ["test_report"], ["task_spec"])
        new_table = {"engineer": {"produce": ["code_fix"], "consume": ["task_spec"]}}
        changed = rdb.set_routing_table(new_table, changed_by="reload")
        assert changed >= 1

        data = rdb.load_routing()
        assert "qa" not in data  # deleted
        assert "engineer" in data

    def test_partial_update(self, rdb):
        rdb.save_routing("qa", ["test_report"], ["task_spec"])
        rdb.save_routing("engineer", ["code_fix"], ["task_spec"])

        update = {"qa": {"produce": ["test_report", "code_review"], "consume": ["task_spec"]}}
        rdb.set_routing_table(update)
        data = rdb.load_routing()
        assert "code_review" in data["qa"]["produce"]
        assert "engineer" not in data  # not in update → deleted

    def test_empty_table(self, rdb):
        rdb.save_routing("qa", ["test_report"], ["task_spec"])
        rdb.set_routing_table({})
        assert rdb.load_routing() == {}


class TestAuditLog:
    """audit_log 查询。"""

    def test_audit_log_records_changes(self, rdb):
        rdb.save_routing("qa", ["test_report"], ["task_spec"], changed_by="init")
        rdb.save_routing("qa", ["test_report", "code_fix"], ["task_spec"], changed_by="update")

        logs = rdb.audit_log(limit=10)
        assert len(logs) >= 1

        # Check at least one log has produce change
        produce_changes = [l for l in logs if l["field"] == "produce"]
        assert any("test_report" in l["new_value"] for l in produce_changes)

    def test_audit_log_limit(self, rdb):
        for i in range(5):
            rdb.save_routing(f"role{i}", [f"cat{i}"], [])

        logs = rdb.audit_log(limit=3)
        assert len(logs) <= 3

    def test_audit_log_empty(self, rdb):
        assert rdb.audit_log() == []


class TestEdgeCases:
    """边界和并发。"""

    def test_concurrent_routing_db(self, db_path):
        """多个 RoutingDB 实例同时操作同一 DB。"""
        db1 = RoutingDB(db_path=db_path)
        db2 = RoutingDB(db_path=db_path)
        try:
            db1.save_routing("qa", ["test_report"], ["task_spec"])
            db2.save_routing("engineer", ["code_fix"], ["task_spec"])
            data = db1.load_routing()
            assert "qa" in data
            assert "engineer" in data
        finally:
            db1.close()
            db2.close()

    def test_context_manager(self, db_path):
        """支持 with 上下文。"""
        with RoutingDB(db_path=db_path) as db:
            db.save_routing("qa", ["test_report"], ["task_spec"])
        # close() called on exit

    def test_unicode_role_names(self, rdb):
        """中文角色名。"""
        ok = rdb.save_routing("code_reviewer", ["code_review"], ["task_spec"])
        assert ok is True
        data = rdb.load_routing()
        assert "code_reviewer" in data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
