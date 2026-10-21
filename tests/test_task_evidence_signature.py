"""task_evidence 签名落库：跨 daemon re-exec 去重。"""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import task_evidence as te  # noqa: E402


def _db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("CREATE TABLE workflow_logs (id INTEGER PRIMARY KEY AUTOINCREMENT,"
               " workflow_instance_id TEXT, task_id TEXT, action TEXT NOT NULL,"
               " actor TEXT NOT NULL, detail TEXT, ts REAL NOT NULL)")
    return db


def test_signature_survives_process_restart():
    db = _db()
    te._record_assessed(db, "task_x1", "neutral|none")
    # 模拟 os.execv：内存签名清空，只留库
    te._LAST_ASSESSMENT.clear()
    te._SEEN_LOADED = False
    te._load_assessed(db)
    assert te._LAST_ASSESSMENT["task_x1"] == "neutral|none"


def test_load_keeps_newest_signature():
    db = _db()
    te._record_assessed(db, "task_x2", "neutral|none")
    te._record_assessed(db, "task_x2", "positive|file")
    te._LAST_ASSESSMENT.clear()
    te._SEEN_LOADED = False
    te._load_assessed(db)
    assert te._LAST_ASSESSMENT["task_x2"] == "positive|file"
