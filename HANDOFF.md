# Session Pipeline — Handoff Summary (Updated)

> 2026-07-25 10:30 CST | 4 commits ahead of origin/main

## Status Overview

### ✅ Done
- **Template metadata**: 32/32 pass, 3 `quality_standards` still had "Codex" — fixed
- **Prompt boilerplate**: 0/105 steps have `请完成「` or `## 原始提示`
- **Recommendation system**: All 5 PRD acceptance criteria pass
- **Routing daemon**: `create_task_v2` calls in both `route_all()` and `route_to_ccs()`
- **Blackboard `facts.t` UNIQUE constraint**: Fixed via UPSERT (`ON CONFLICT(t) DO UPDATE`)
- **`composite_runner` crash**: Removed stale references in `engine.py`
- **Tests**: Updated `_load_run`/`_save_run` → SQLite; `close_wf` status parameter added

### ❌ Remaining Issues

**7 test failures (all assertion errors in engine flow):**

| # | Test | Root Cause |
|---|------|-----------|
| 1 | `test_tick_advances_on_match` | `_tick` runs on same `run_once()` as notification but `_check_exit()` doesn't find the bus message |
| 2 | `test_tick_completes_on_last_step` | Same pattern |
| 3 | `test_advance_finishes` | Manual step advance via SQLite doesn't trigger advancement |
| 4 | `test_timeout_triggers_retry` | Timeout not detected after fix |
| 5 | `test_timeout_escalates_not_fails` | Same |
| 6 | `test_run_persists_on_tick` | Persistent state check fails |
| 7 | `test_workspace_summary_in_prompt` | Workspace summary not replaced in prompt |

All 7 are the SAME root cause: `_tick()` now runs on the first `run_once()` after notification (previously it required a second `run_once()` call). But `_check_exit()` uses `created_after=last_ts` which might filter out the bus message because the bus write happened before `poll_since` is set.

**Fix remaining issues**:
1. Simplify `_tick()` to check exit condition before setting `poll_since` 
2. Remove `created_after` filter in `_check_exit` when `poll_since` is 0
3. Ensure step advancement interacts correctly with `_advance_production_wf()`

The core logic is sound — just need to fine-tune the `_tick` → `_check_exit` interaction timing.

### Test Count: 127 passed, 7 failed (of 134)
