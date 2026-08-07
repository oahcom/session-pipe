#!/usr/bin/env python3
"""Workflow Health Check — 供 maintainer 调用，检查工作流健康并写 bus。"""

import json
import sys
from pathlib import Path

_src = Path(__file__).resolve().parent.parent
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from paths import ensure_paths
ensure_paths()


def check() -> dict:
    """检查工作流健康状态，返回 {healthy, stats, issues}。"""
    from lifecycle.manager import LifecycleManager
    lm = LifecycleManager("health_check")
    stats_rows = lm.query(
        "SELECT status, COUNT(*) as c FROM workflow_instances GROUP BY status"
    )
    stats = {r["status"]: r["c"] for r in stats_rows}
    total = sum(stats.values())
    running = stats.get("running", 0)
    completed = stats.get("completed", 0)
    issues = []

    # 检查 running 中超过 1 小时的僵尸
    stale_rows = lm.query(
        "SELECT instance_id, template_id, created_at, step_results "
        "FROM workflow_instances WHERE status='running'"
    )
    stale_count = 0
    for r in stale_rows:
        d = dict(r)
        sr = json.loads(d.get("step_results", "{}"))
        max_tc = max(
            (v.get("timeout_count", 0) for v in sr.values() if isinstance(v, dict)),
            default=0,
        )
        if max_tc >= 3:
            stale_count += 1

    if stale_count > 5:
        issues.append(f"running 中有 {stale_count} 个高 timeout_count 工作流")
    if running > 20:
        issues.append(f"running 工作流数 {running} > 20，可能积压")

    completion_rate = round(completed / max(total, 1) * 100, 1)
    return {
        "healthy": len(issues) == 0,
        "stats": {
            "total": total, "running": running, "completed": completed,
            "cancelled": stats.get("cancelled", 0), "failed": stats.get("failed", 0),
            "completion_rate": completion_rate,
        },
        "issues": issues,
    }


if __name__ == "__main__":
    result = check()
    print(json.dumps(result, indent=2, ensure_ascii=False))
    sys.exit(0 if result["healthy"] else 1)
