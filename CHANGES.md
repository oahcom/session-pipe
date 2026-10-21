# CHANGES.md

## 2026-10-22 — 闸门提示标题改为稳定去重键（747fefa）

### 问题
`_gate_prompt` 把失败详情（缺失文件名 / verify 报错原文）拼进标题，而 `_notify_role` 的
30min 冷却按 `(role, title)` 命中 → 每次文本都不同 → 冷却永不命中 → 每 tick 叫醒执行角色一次。
现场（747fefa 提交信息记载的 24h 实测，本条未复算）：qa 目录 11:30–12:05 收到 68 条 user 消息；
qa 只能回同一句 9 字正文「已完成，无新动作。」18 次（11:38–11:51，每 1–2 分钟一条）；qa 整轮零工具 45/84 = 53.6%（全角色 2.5%）。

### 修复
`src/pipeflow/step_handler.py` `_gate_prompt`（:1291-1319）：标题固定为
`[workflow] {run.workflow_name} {step.id} 闸门未通过`（例：`[workflow] engineer_feature s1 闸门未通过`）；缺失文件名与模板要求的 `text_contains`
标题契约移入 evidence 正文，详情仍随消息送达。
`tests/test_r119_gate_prompt.py` 同步断言稳定标题 + 详情在 evidence。

### 验证
- `python3 -m pytest tests/test_r119_gate_prompt.py -q` → `2 passed`（exit=0，本文档写作时实跑）
- ⚠️ 上游数字一处自相矛盾：747fefa 原文「11:30–12:05 收到 68 条 user 消息，其中 79 条提醒在 24h 内注入」——子集大于全集，不可同时成立；本条只引用 68 / 18 / 45-84 三项，未引用 79。
- 冷却口径核对：`grep -n _ALERT_COOLDOWN_S src/pipeflow/role_lifecycle.py` → :217 `_ALERT_COOLDOWN_S = 1800`，与「30min 冷却」一致
- 天花板：冷却表是进程级内存（role_lifecycle.py:215 `ponytail:` 注释），CLI 每 tick 起新进程时最坏退化为每 tick 一条

## 2026-10-22 — 投递前 CLI 忙预检

### 问题
DB 无其他活跃步时直接进入 ccs send；角色 CLI 仍在执行时，分块投递会等 pane，
实测单次 30s，整个 tick 的其他工作流全部停摆（R129 §8/§9.1）。

### 修复
1. `src/pipeflow/role_lifecycle.py` `_send_to_role`：prompt 全部校验完（未替换变量 / 空 goal）之后，
   用 `liveness.pane_cli_busy` 只看 CLI 忙态（底栏 esc/retrying）；True 即返回 `"busy"`，下 tick 再试。
   不用 300s 活动窗——刚干完活的 pane 仍有输出，活动窗会把空闲角色判忙（bus #395128 同一根因）。
   探针异常不阻断投递（沿用原有的"探测不可用则放行"语义）。
   `force=True` 不预检：timeout 重推 / dual-node / integrity 提醒是旁路，integrity 在发送前已落
   bypass flag，预检返回 busy 会永久吞掉提醒。
   首步派单 `_write_step_prompt` 补传 `wf_id/step_id`，否则新建链的第一跳绕过预检。
2. `tests/test_pipeflow_error_paths.py`：新增 2 例，锁住"CLI 忙 → busy 且不启动 ccs 子进程"与"探针异常 → 放行"。
3. `tests/test_exit_condition.py`：清空进程级 `_ALERT_TS`，避免稳定标题的 30min 冷却跨用例吞告警。

### 验证
- `python3 -m py_compile src/pipeflow/role_lifecycle.py` 通过
- `python3 -m pytest tests/test_pipeflow_error_paths.py tests/test_send_to_role_timeout.py -q`
  → 26 passed
- 现场探针：13 个角色中 9 个 CLI 忙（底栏 `esc to interrupt`），这些投递会被直接排队，
  不再进入 30s 子进程等待；pane 有输出但 CLI 空闲的角色不再被误判。

## 2026-10-22 — 闸门告警退避

### 问题
`_gate_alert` 只做固定 5 分钟节流：同一条 verify/schema 失败每 5 分钟无限重发。
实测 reviewer 四轮收到逐字相同的「缺少 REVIEW.md」告警，每轮重跑 3~14 次取证
（30 次工具调用），零新增结论；且 REVIEW.md 实际存在，归因也错。

### 修复
`src/pipeflow/step_handler.py` `_gate_alert`：同一 key+text 按 5/10/20/40/80min 退避，
第 6 次不再发给角色、改写一条 blocker 升级；text 变化 = 新信息立即播报并重置计数。
新增 `tests/test_gate_alert_backoff.py` 锁住退避、停发、升级、新文本四件事。

### 验证
- `python3 -m pytest tests/test_gate_alert_backoff.py -q` → 1 passed

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
