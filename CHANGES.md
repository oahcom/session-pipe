# CHANGES.md

## 2026-10-22 — 审批密钥通道全链路退场（841e545 + 2a8a016）

### 问题
评审步按**结论**推进，密钥没有消费者：引擎侧早已不再等审批，但 `manager` 仍签发 token，
并向审批人投递一条「[workflow] 步骤审批请求」——密钥在 bus 里被脱敏成 `***`，
角色既拿不到也 `confirm` 不了。驳回语义改由 bus 文本承担
（`CHANGES_REQUESTED` / `rejected`，由 `src/review_gate.py` 判据消费）。

### 修复
1. `841e545`：删签发侧（`_issue_approval_token` + `_bb_write_fallback` 120 行 + 2 处调用）；
   删 `tests/test_approval_pull_lane.py`，新增 `tests/test_approval_channel_retired.py`（反向验证）。
2. `2a8a016`：删消费侧 3 个读取器（`get_approval_token` / `get_approval_token_raw` /
   `approval_ttl_remaining`）+ `confirm_step` 里的密钥分支，实测 `git show --numstat` 合计 23 增/117 删 = **净删 94 行**（src 子集 17/114 = −97）。
   ⚠️ 提交标题自称「−110 行」，实测 −94，不一致——引用时以 numstat 为准。

### 验证
- `python3 -m pytest tests/test_approval_channel_retired.py tests/test_launcher_delegation_contract.py tests/test_workcycle_e2e.py -q`
  → `20 passed in 62.90s`（exit=0，本文档写作时实跑；单跑分别为 2 passed / 18 passed）
- 读取器确已删除：`grep -rn "def get_approval_token\|def get_approval_token_raw\|def approval_ttl_remaining" src/` → 无命中（exit=1）
- 存量密钥复算（`~/.hermes/state/workflows.db` 只读，本条实测）：
  1897 个 dict 步骤中 **430 个仍带 `approval_token`**（`completed` 345 / `step_done_ready` 85）；
  `approval_consumed_at` 非空者 **0** → 全部是死密钥，只能留着，不能再被拉回。
- 上游 bus #438765 记「24h 107 条审批请求」与「14 张消费延迟实测」；本次只复算上表，
  107 条无法从生产库重算（投递记录未落库），按原文转载未独立验证。

### 影响面
所有走评审步的角色的工作流契约都变了：不再等密钥、不再收「步骤审批请求」，
驳回写 bus 消息（正文含 `CHANGES_REQUESTED` + 业务/系统问题分类 + 返工对象），
`wf confirm <wf_id> <step_id> --reject -r "<理由>"` 也不再需要密钥。


## 2026-10-22 — 闸门提示标题稳定化（747fefa）已撤回（fdf4bde）

### 结论
747fefa 把标题换成稳定键，随后被 fdf4bde 撤回；现在 `_gate_prompt` 的标题重新携带失败详情。
本节保留，因为「为什么要撤回」比「当时为什么改」更值得记。

### 为什么改（747fefa）
`_gate_prompt` 把失败详情（缺失文件名 / verify 报错原文）拼进标题，而 `_notify_role` 的
30min 冷却按 `(role, title)` 命中，于是每次文本都不同，冷却永不命中，每 tick 叫醒执行角色一次。

### 为什么撤回（fdf4bde）
冷却键就是 `(role, title)`，而命中后的 `return` 发生在**写 bus 之前**（role_lifecycle.py:844-852），
同一 `(role, wf, step)` 的**另一种失败原因**在 30min 内整条被吞：既不落 bus 也不发角色。
实测复现：`tests/test_tick_paths.py::test_tick_verify_failure` 单跑过、整文件跑红
（同进程内 schema 用例先占了稳定标题）。频率控制改由 `_gate_alert` 的退避承担
（5/10/20/40/80min，见下一节），不再靠吞消息。

### 因果陈述被证伪
747fefa 称 qa 空转源于「闸门标题不稳定叫醒 qa」。fdf4bde 实测：qa 那 18 条
「已完成，无新动作。」发生在 **10-21 19:38–19:51**，驱动它的是意图提醒通道（79 条/24h）；
**7 天内 target=qa 的闸门告警为 0 条**，这条链从未对 qa 成立
（对 reviewer 成立：01:33–02:27 每 5–7 分钟一条）。
因此 747fefa 正文里的 68 条 / 18 次 / 45-84=53.6% 不能归因到闸门告警，本条不再引用它们作依据；
原文另有一处自相矛盾（子集 79 > 全集 68）。

### 当前行为（可核对）
- `step_handler.py:1308-1310`：`_notify_role(self._eff_role(...), msg, ...)`，标题即 `msg`，带失败详情
- `role_lifecycle.py:217`：`_ALERT_COOLDOWN_S = 1800`
- 验证：`python3 -m pytest tests/test_r119_gate_prompt.py tests/test_tick_paths.py tests/test_gate_escalation.py -q`
  → `32 passed in 1.55s`（exit=0，本文档写作时实跑）

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
