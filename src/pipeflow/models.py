"""
composite_models.py — CompositeRun 持久化。

SQLite: 默认 ~/.hermes/state/composite_runs.db
"""
import json
import os
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


DB_PATH = Path.home() / ".hermes" / "state" / "composite_runs.db"


@dataclass
class LoopConfig:
    """工作流循环控制配置。"""
    schedule: str = ""
    max_iterations: int = 0
    rest_if_no_work: bool = True
    idle_timeout_minutes: int = 60


@dataclass
class CompositeRun:
    run_id: str
    name: str
    context: dict = field(default_factory=dict)
    status: str = "running"
    current_step_id: str = ""
    step_statuses: dict = field(default_factory=dict)
    sub_runs: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)
    created_at: float = 0.0
    updated_at: float = 0.0

    def to_row(self) -> tuple:
        return (
            self.run_id, self.name, json.dumps(self.context, ensure_ascii=False),
            self.status, self.current_step_id,
            json.dumps(self.step_statuses, ensure_ascii=False),
            json.dumps(self.sub_runs, ensure_ascii=False),
            json.dumps(self.errors, ensure_ascii=False),
            self.created_at, self.updated_at,
        )

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "CompositeRun":
        return cls(
            run_id=row["run_id"],
            name=row["name"],
            context=json.loads(row["context"]),
            status=row["status"],
            current_step_id=row["current_step_id"],
            step_statuses=json.loads(row["step_statuses"]),
            sub_runs=json.loads(row["sub_runs"]),
            errors=json.loads(row["errors"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


class CompositeRunDB:
    """CompositeRun SQLite 数据库。"""

    def __init__(self, db_path: str | Path = DB_PATH):
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), timeout=10)
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
            CREATE TABLE IF NOT EXISTS composite_runs (
                run_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                context TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'running',
                current_step_id TEXT DEFAULT '',
                step_statuses TEXT NOT NULL DEFAULT '{}',
                sub_runs TEXT NOT NULL DEFAULT '{}',
                errors TEXT NOT NULL DEFAULT '[]',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_cr_status ON composite_runs(status);
            CREATE INDEX IF NOT EXISTS idx_cr_name ON composite_runs(name);
            CREATE INDEX IF NOT EXISTS idx_cr_created ON composite_runs(created_at);
        """)
        self._conn.commit()

    def save(self, run: CompositeRun) -> None:
        """插入或更新。"""
        run.updated_at = time.time()
        now = run.updated_at
        row = run.to_row()

        self._conn.execute("""
            INSERT OR REPLACE INTO composite_runs
            (run_id, name, context, status, current_step_id,
             step_statuses, sub_runs, errors, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, row)
        self._conn.commit()

    def load(self, run_id: str) -> Optional[CompositeRun]:
        row = self._conn.execute(
            "SELECT * FROM composite_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        return CompositeRun.from_row(row) if row else None

    def load_by_name(self, name: str, status: str = "") -> list[CompositeRun]:
        if status:
            rows = self._conn.execute(
                "SELECT * FROM composite_runs WHERE name=? AND status=? ORDER BY created_at DESC",
                (name, status)
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM composite_runs WHERE name=? ORDER BY created_at DESC",
                (name,)
            ).fetchall()
        return [CompositeRun.from_row(r) for r in rows]

    def list_runs(self, status: str = "", limit: int = 50) -> list[CompositeRun]:
        if status:
            rows = self._conn.execute(
                "SELECT * FROM composite_runs WHERE status=? ORDER BY created_at DESC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM composite_runs ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [CompositeRun.from_row(r) for r in rows]

    def close(self):
        self._conn.close()


# ── 模块级便捷函数 ────────────────────────────────────────────────

def get_db() -> CompositeRunDB:
    return CompositeRunDB()


def save(run: CompositeRun) -> None:
    with CompositeRunDB() as db:
        db.save(run)


def load(run_id: str) -> Optional[CompositeRun]:
    with CompositeRunDB() as db:
        return db.load(run_id)


if __name__ == "__main__":
    # 自测
    with CompositeRunDB() as db:
        run = CompositeRun(
            run_id="test1", name="dev_pipeline",
            context={"project": "test"}, created_at=time.time(), updated_at=time.time()
        )
        db.save(run)
        r = db.load("test1")
        print(f"status: {r.status}, name: {r.name}, step_statuses: {r.step_statuses}")