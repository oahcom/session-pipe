#!/usr/bin/env python3
"""TE-05: 角色产出 ≥24h 连续性稳定性守门。

直接读取生产 Sister Bus blackboard.db, 断言过去 24 小时内**真实角色推理产出**(非 daemon
脉冲)连续分布在每个 6h 桶且有最小体积, 以及至少 1 天的 bus 历史存在。

此测试替身'fake', 运行于 TE 每日 cron (te-daily-test)。它不创造 24h, 而验证
生产数据已在过去 24h 连续维持真实输出 —— 把 goal "≥1天稳定产出" 变为可守护的可测量。

环境: 可选 HERMES_BLACKBOARD_DB(默认生产 ~/.hermes/sister_bus/blackboard.db)。
"""
import os
import sqlite3
import sys
import time
import unittest
from pathlib import Path

DEFAULT_DB = Path.home() / ".hermes" / "sister_bus" / "blackboard.db"
DB_PATH = Path(os.environ.get("HERMES_BLACKBOARD_DB", str(DEFAULT_DB)))

# daemon/cron 脉冲类, 排除 —— 仅统计真实角色推理产出
PULSE_CATS = {
    "ccs_health", "notice", "scheduler", "monitor_audit", "standup",
    "blocker", "feedback", "workflow", "task_evidence", "ops", "reflexion_lesson",
    "design_issue", "scheduler",
}
# 仅真人/角色推理 source(去掉纯 daemon 推送)
DAEMON_SRCS = {
    "ccs-monitor-daemon", "ccs-monitor", "workflow_engine", "loop_gate",
    "survival_monitor", "contract_updater", "core.send", "coordinator-loop-timer",
    "test_engineer", "health-daemon", "claude",
}

MIN_TOTAL_24H = 100      # 24h 内至少 100 条真实推理产出
MIN_PER_6H = 8           # 每 6h 桶不少于 8 条 (防突然断流)
REAL_ROLES = {"maintainer", "coordinator", "lr", "engineer", "qa", "pm",
              "reviewer", "scout", "devops", "writer", "investigator_python",
              "investigator_senior", "knowledge_curator", "security_auditor"}


def _conn():
    return sqlite3.connect(str(DB_PATH))


def facts_last_n_days(n: int):
    cut = time.time() - n * 86400
    with _conn() as c:
        rows = c.execute(
            "SELECT cat, src, ts FROM facts WHERE ts >= ? ORDER BY ts", (cut,)
        ).fetchall()
    return rows


class TestOutputContinuity(unittest.TestCase):
    """断言生产 bus 在过去 24h 连续维持真实角色推理产出。"""

    def setUp(self):
        if not DB_PATH.exists():
            self.skipTest(f"blackboard db missing: {DB_PATH}")

    def test_24h_history_exists(self):
        rows = facts_last_n_days(1)
        self.assertGreater(len(rows), 0, "过去 24h bus 无任何 fact，数据源可能已断")

    def test_24h_real_reasoning_volume(self):
        rows = [r for r in facts_last_n_days(1)
                if r[0] not in PULSE_CATS and r[1] not in DAEMON_SRCS]
        self.assertGreaterEqual(len(rows), MIN_TOTAL_24H,
                                f"24h 真实推理产出过少: {len(rows)} < {MIN_TOTAL_24H}")

    def test_6h_buckets_no_gap(self):
        now = time.time()
        buckets = []
        for i in range(4):
            lo = now - (4 - i) * 6 * 3600
            hi = lo + 6 * 3600
            n = sum(1 for r in facts_last_n_days(1)
                    if lo <= r[2] < hi
                    and r[0] not in PULSE_CATS and r[1] not in DAEMON_SRCS)
            buckets.append(n)
        self.assertTrue(all(b >= MIN_PER_6H for b in buckets),
                        f"24h 连续性断档: 6h 桶产出={buckets}, 应≥{MIN_PER_6H}/桶")

    def test_real_roles_produce(self):
        rows = [r for r in facts_last_n_days(1) if r[1] in REAL_ROLES]
        producing = {r[1] for r in rows}
        # 至少 3 个真实角色在 24h 内有推理产出
        self.assertGreaterEqual(len(producing & REAL_ROLES), 3,
                                f"真实角色产出过少: {producing & REAL_ROLES}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
