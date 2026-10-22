# CHANGES.md

## 2026-10-22 — 空闲判死接活体门 + cancel_step 终态幂等 + 文档同源

### 问题（角色转录/日志实证）
1. tmux 空闲分支 `idle_count>=5`（约 50s）直接取消步骤，而角色长思考时 pane 静默是正常态。
   两个实例的完成消息在 cancel **之后**才落 bus：`wf_e688c4fafc/s4` +27.6s、
   `wf_sub_c5620e70dd31/s1` +235s —— 产出已写好被丢弃。另有一条独立测算：这类误杀导致的父链重建子链
   多花 **35.6 分钟**（不是上面两个延迟之和，两者口径不同）。
2. `cancel_step` 不判终态：`wf_ae31fd1c4c11` 一条链落 49 行 `step_cancelled`，跨 31.3 分钟，
   48 个间隔中位间隔 10.7s，但存在比一 tick 更长的断档（最大 20.9 分钟，且不止一次）—— 所以只能说
   "一 tick 一行地重试"，不能说全程每 10.7s 一行。**注意时效**：这是 10-16 的数据，近 24h
   全库仅 1 行 —— 幂等属预防性不变量，不是当下止血。
   同仓排队分支（`_queued>=10`）早有 `not _role_producing_recently(_eff)` 活体门，两条判死路径口径不一致。
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
