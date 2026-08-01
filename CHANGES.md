# CHANGES.md

## 2026-08-01 — 僵尸工作流抢救修复

### 修复内容
1. `src/pipeflow/engine.py` `_tick`: 当步骤 `step_results[step].exit_messages` 已存在（旧引擎产物/产出证据已就绪）但状态未闭合时，直接调用 `complete_step` 闭合，不依赖已 seen 的 bus 消息重新匹配（bus_anchor 游标跳过旧消息导致永不推进）。
2. `src/pipeflow/engine.py` `_cleanup_stale_workflows`: 回收前先检查当前步骤 `exit_messages` → 先抢救闭合（`complete_step` 幂等），失败才 cancel；cancel 时写 `workflow_logs`（此前直接 UPDATE 无审计日志）。

### 验证
- `python3 -m py_compile` 通过
- `tests/test_tick_paths.py` 15 passed
- rescue 逻辑自检 4 用例全过（notified+exit_messages→rescue / completed→skip / no-exit→skip / normal-flow→tick）

### 未改动
- DB schema 无变更（无 migration）
- 不影响正常 tick 推进路径

## 2026-08-01 — 测试漂移清理
3. `tests/test_tick_paths.py`: 删除已废弃的 `test_tick_reminder_sent`（last_reminder 催办逻辑已于 252cbcf 移除，测试未同步）。清理 `_REMINDER_INTERVAL` 引用。14/14 测试通过。
