# CHANGES.md

## 2026-10-22 — 空闲判死接活体门 + cancel_step 终态幂等 + 文档同源

### 问题（角色转录/日志实证）
1. tmux 空闲分支 `idle_count>=5`（约 50s）直接取消步骤，而角色长思考时 pane 静默是正常态。
   两个实例的完成消息在 cancel **之后**才落 bus：`wf_e688c4fafc/s4` +27.6s、
   `wf_sub_c5620e70dd31/s1` +235s —— 产出已写好被丢弃。另有一条独立测算：这类误杀导致的父链重建子链
   多花 **35.6 分钟**（不是上面两个延迟之和，两者口径不同）。
   同仓排队分支（`_queued>=10`）早有 `not _role_producing_recently(_eff)` 活体门，两条判死路径口径不一致。
2. `cancel_step` 不判终态：`wf_ae31fd1c4c11` 一条链落 49 行 `step_cancelled`，跨 31.3 分钟，
   48 个间隔中位间隔 10.7s，但存在比一 tick 更长的断档（最大 20.9 分钟，且不止一次）—— 所以只能说
   "一 tick 一行地重试"，不能说全程每 10.7s 一行。**注意时效**：这是 10-16 的数据，近 24h
   全库仅 1 行 —— 幂等属预防性不变量，不是当下止血。
3. `cancel_step` 的幂等判断一度放在 `current_step_id` 校验之前，已终态实例收到错步骤会被
   静默吞成 `False`，掩盖调用方 bug（codex 一轮 P1）。

### 改动
- `step_handler.py`：空闲判死加活体门；按 `cancel_step` 返回值记 info「取消」/ debug「空操作」。
- `manager.py`：先校验 `current_step_id`（错步骤仍 raise）→ 终态 `cancelled/completed/failed`
  短路（`rollback` 后 `return False`，不得裸 return，否则事务挂连接）。
- `tests/`：新增 `test_cancel_step_idempotent.py`（3 例 + 三终态 subTest）、
  `test_tick_paths.py` 增 2 例；4 个既有 idle 用例显式 patch `_role_producing_recently`
  （原先隐式依赖 scout 真实转录 mtime，角色一跑就可能翻红）。反向验证：去掉活体门、
  恢复无条件 info 日志、去掉终态守卫，对应用例分别失败。
- 文档：`PUBLIC_API.md` 补 `cancel_step` 契约；`docs/TEST_WORKFLOW.md` 把"空闲取消"改为
  两道门，并按**守卫引入时间切窗**写明 rework_ids 缺口（守卫 `421f657` 引入后，母集切成
  9 例不响应取消里 0 例孤儿、引入前 10 例里 1 例；母集总数与分母口径只写在 TEST_WORKFLOW，
  这里不复制，防止两处漂移）。

### 验证
`1253 passed, 1 deselected, 2 subtests`；`scripts/doccheck.py` → CLEAN；
生效：daemon 就地重执行 `源码已更新 (mtime=1792625695)` = 07:34:55 > 文件 mtime 07:32:48。

### 未做
- `_has_live_subflow` 仍未看 `rework_ids`：实测 0.4% 且子链已死，属"可修但无需求"，
  要修就复用 `engine._derive_rework_ids` / `_rework_alive`，别新写判定。

## 2026-10-22 — 归属更正（9042df0 / a5f7c45）

- `9042df0` 真实 subject：`fix(routing): 文档/注释跟上 cursor 退役 + limit 契约在函数内
  兑现 + 删死 import`。`--stat` = 4 文件、+117/−380（ARCHITECTURE 4/4、SYSTEM_LANDSCAPE
  +48/−110、TECHNICAL_DEEP_DIVE +65/−264、`reliability_core.py` −2）——**没有 .py 逻辑改动**。
  `git blame` 会把两份文档的 47/63 行指向它，但**谁改的无法从 git 判定**（作者字段是
  `hermes-session`，与相邻提交同作者，无 session 标记），仅记"归属提交在此"。
- 一并更正我先前在本文件与文档里的三处口径错误（只读子代理逐条复核后）：
  ① 引文"文档与死码收敛"是**编造**，9042df0 从未有过该 subject；
  ② "480+ 行文档改写"应为 **487 行 diff 触达（+113/−380）**，158/329 是增删合计不是新增；
  ③ `555c3ac` 当时 `43 passed` 是**正确的**（该提交下恰 43 个 test def），49 是后续
  加用例后的现值；当时错的是"13 个常量 / 60→24 行"（实为 14 个 / 60→20）。
  详见 `~/hermes/workspace/R144-删除重复路径常量/验证与待办.md`。
- `a5f7c45` 删 6 行零引用常量（`HERMES_TEMPLATES` / `WORKFLOW_GUIDE` /
  `CODEX_SENTINEL_DIR` / `_CONTROL_WORDS`），四仓 grep 零引用 + 12 passed + 463 passed。


## 2026-10-22 — 投递预检判据收口（底栏 6 行 + 测试锁真）

### 问题（5 个只读子代理审查 eda4a38/4f02450 后实证）
1. `pane_cli_busy` 扫整块 pane 末 2000 字符：角色正文里讨论 "esc to interrupt"、贴日志里的
   "Retrying … attempt" 会被当忙态，该角色所有投递恒返 busy → queued 累加 → 队列预算到期被回收。
2. 预检用例并不锁预检：同文件前一个用例已把同一 (role,wf,step,prompt) 写进进程级
   `_LAST_SEND`，删掉预检后同 prompt 去重分支照样返回 busy，三条断言全绿。
3. 无用例覆盖「预检在 goal 校验之后」与「force 旁路」：把顺序改回去、去掉 force 条件，测试仍全绿。
4. 首步补 `wf_id` 也没有断言：链路契约用例的 _record 丢掉了 wf_id。

### 修复
- `liveness`：判据只看底部 6 行（`_PANE_TAIL_LINES`）；拆出 `_pane_busy_markers`，
  `tmux_pane_active` 走内部函数，测试打桩 `pane_cli_busy` 不影响活跃判定。
- `tests/conftest.py`：autouse 夹具默认把探针桩成 None，并清 `_LAST_SEND` / `_ALERT_TS`，
  进程级冷却不再跨用例泄漏；单个用例要忙态自己 patch。
- 用例：defers/proceeds 断言探针被调用；新增「非法 prompt 不被忙吞」与「force 不被拦」；
  链路契约记录并断言首跳 wf_id。
- 闸门退避：文本规范化、闸门通过即清零、4h 裁剪（见上一节）。

### 验证
- 8 个相关测试文件 → 150 passed
- 变异验证：把 `if wf_id and not force:` 改成 `if False:` 后 proceeds 用例变红
  （pane_cli_busy 未被调用），恢复后转绿。
- `python3 scripts/doccheck.py` → CLEAN（TREE 现在也校验「（N 行，…」形态；TABLE 要求反引号成对）。

### 未采纳（附理由）
- 删 `wf_id and` 并回退首步 wf_id：该条件在测试/直调路径上仍是有效闸门；
  首步写 TASKS.md 与其他派单一致，未观察到害处。
- 删预检的 try/except：`pane_cli_busy` 未捕获 UnicodeDecodeError 等非常规异常，
  投递失败的代价高于保留 4 行。
- 给 step_results 的 COALESCE 加 NULLIF：生产库该列为空串的行数为 0（实测），无证据，
  且文件正被其他 session 改，先不动。

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

### 修复（归属更正）
退避实现来自 c02cdc1（`_gate_alert` 的 5/10/20/40/80min 档、第 6 次停发并写 blocker），
不是 eda4a38 的改动；eda4a38 只补了回归测试。本条最初把它记在本提交名下，现更正。

### 2026-10-22 追加加固
- 失败文本先规范化（数字/十六进制 → `#`）再作为退避身份：verify 的 stderr 每次带时长/行号，
  按原文比较会把同一条失败当新原因，退避永远从第 1 档重来。
- 闸门通过时清掉该步的退避键：原来只在文本变化时归零，同一条确定性失败到第 6 次后
  永久静默，原因消失又复发也不再有提示。
- 记录表超 1000 条时按 4h 时间裁剪，不再整体 clear（clear 会把已停发的键重新武装）。

### 验证
- `python3 -m pytest tests/test_gate_alert_backoff.py -q` → 3 passed（退避/停发/升级 + 数字噪声不重置 + 通过即清零）

## 2026-08-01 — 僵尸工作流抢救修复
> 注：本条写于 `engine.py` 拆分前——`_tick` 现在 `src/pipeflow/step_handler.py`，`_cleanup_stale_workflows` 在 `src/pipeflow/wf_lifecycle.py`。

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