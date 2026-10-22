# CHANGES.md

## 2026-10-22 — task_evidence：真实产物被「回复≠产出」判死（71% 假中性）

### 问题（可观测证据）
1. bus feedback #441960/#441971 把 pm 判成「exit_messages 存在（回复≠产出）」，
   同一条 fact 的 `output_files` 却列着 19 个文件 / 84KB（INTAKE.md、PRD-wf_sub_*.md）。
2. 根因是判定链顺序：`exit_messages` 判在最前，而几乎每条完成的链都有该字段 ——
   唯一据实产物判 positive 的规则（≥3 个 ≥1KB 或 ≥1 个 >2KB）成了不可达代码。
3. 近 7 天最近 120 条 completed 任务：positive 只有 20 条，修正后 105 条（假中性 85 条 = 71%）。
   评分会回写 role TASKS.md「历史教训」，等于教角色「发消息比写产物重要」。

### 改动
- `src/task_evidence.py`：抽出 `_classify_outputs(n, size, evidence, bus_evidence)`，顺序即判据
  （真实产物 > 量小产出/回话/bus > 纯模板）；调用点 6 段 if/elif → 4 行 detail。21 增 22 删。
- `tests/test_task_evidence_scoring.py`：6 条断言锁住顺序语义，首条即本次回归。

### 验证
- `pytest tests/test_task_evidence_scoring.py` → 6 passed。
- `pytest tests/` 全量 → 1384 passed / 0 failed（--junit-xml 记录在 /tmp/jr160f.xml）。
- `python3 scripts/doccheck.py` → CLEAN（exit 0）。

### 已知边界
- 历史 bus 里已写的假中性 fact 不会被覆盖（标题含 detail、签名不同）：消费方取最近一条即可，
  旧 fact 保留作修正前基线，不回填删除。

## 2026-10-22 — temp-pr-branch 合入 main（漂移收敛，35 文件 / 4 处 modify-delete）## 2026-10-22 — 全量套件从未跑完：测试真拉起活体角色 + 巡检预算在单角色上失效

### 问题（可观测证据）
1. `python3 -m pytest tests/ -q` 连续两次停在第 29 例（**28 个点**、15 分钟 0 前进、
   `wchan=hrtimer_nanosleep`）。唯一可信的定位方式 `--timeout=25 --timeout-method=thread` 抓栈
   （前两轮猜 `_wait_for_role_ready` / 派单用例都错）：
   `run_once → _ensure_guard_roles → _ensure_role_alive → _check_and_start_role → ccs start(60s) + sleep(3)`。
   测试用的 `test_role` 在注册表里不存在，一个 tick 烧 90s+。
2. 跑到 ~94% 以 **EXIT=142** 结束、无 summary：用例自带 `signal.alarm(20)` 而 pytest 下没装 handler →
   默认动作杀进程；`grep -c signal.alarm` 集中在 `test_workcycle_e2e.py`（17 处）。
3. 生产侧：`_ensure_guard_roles(deadline)` 只在**角色之间**看预算，单个 `ccs start` 仍可独占 60s ——
   「巡检不吃光 tick 预算」在单角色粒度上不成立（近 12h 日志 2 次「守护巡检超预算」）。
4. 审批通知真发：`LifecycleManager._notify_result` 走真 `ccs send`，测试夹具 id 直接进了
   product_architect 的上下文（2026-10-21 实测 13 条「工作流: wf_pull」）。

### 改动
- `tests/conftest.py`：4 个 autouse 桩 —— `_ensure_guard_roles`→`set()`、`_wait_for_role_ready`→True、
  `_ensure_role_alive`→True、`_notify_result`→None；三张模块 allowlist（`_REAL_GUARD_MODULES` /
  `_REAL_READY_MODULES` / `_REAL_ENSURE_ALIVE_MODULES`）给「被测的就是这一步」的用例留真实路径；
  SIGALRM handler 抛 `pytest.fail.Exception`（BaseException，热路径的 `except Exception`/`OSError` 吞不掉）。
- `src/pipeflow/role_lifecycle.py` + `engine.py`：`deadline` 透传到 `_check_and_start_role` ——
  `ccs start` 的 timeout 取 `max(5, min(60, deadline-now))`、轮询超预算即 break；无 deadline 时行为不变（60s）。
  预算耗尽时不盖 300s 重启冷却戳（`_RESTART_COOLDOWN_SEC`；30min 是 `_ALERT_COOLDOWN_S`，别混）；`TimeoutExpired` 进 catch 元组。
- 新增回归：`tests/test_guard_budget.py`（3 例）、`tests/test_no_live_notify.py`（全局 patch 必须
  `try/finally` 恢复 —— 泄漏的 `subprocess.run` 桩会让 `eval_checker` 的 `stat -c %W` 拿到空串而 fail-open，
  表现为 5 条与本文件无关的用例转红）。

### 验证
- `pytest tests/ -q` → **1318 passed, 2 subtests passed in 148s（exit 0）**，两次连续复现（当次快照，commit fdddcf6）；
  同日 11:40 复跑 **1321 passed / 154s**（条数随并发改动变化，门禁看 0 failed 不看条数）。
  修前**从未拿到 summary**（卡在 5%）。
- `test_chain_dispatch_contract.py` 90s+ → **1.08s**；`test_guard_budget.py` 3 passed。

### 未做
- 冷却/去重表（`_ALERT_TS` 等）仍是**进程级**：daemon 源码热重载后清空，最坏重发一次。
- 5 条"单跑绿、全量红"的历史 flaky 只验到与本轮改动无关，未逐条修根因。

## 2026-10-22 — 文档引用改成可机器校验（doccheck v2）

### 为什么合
main 与生产分支双向漂移 **100 / 94** 个提交：分支上的判据修复（盲改豁免、排队账本、
返工链证据、路由 claim）不在 main 上；main 的独立新增（REVIEW_ROLES、`conditional APPROVED`、
cat 下推、`_unknown_verdict_notified`、死测试剪枝）分支也没有。两条线各自"更正确"一部分。

### 裁决原则
文档/测试/源码取并集；同一函数取"更完整且有测试"的一侧；文档数字以合并后实测为准；
modify/delete 以"模块是否还存在、是否被引用"为判据。逐段记录见
`~/hermes/workspace/R151-主干合入temp-pr-branch/冲突裁决.md`。

### 合并时被"机械并集"漏掉、事后补回的四类（git 自动合并无冲突标记，测试/扫描才暴露）
1. `engine.py`：`_derive_rework_ids` 只剩定义、调用点被 main 侧覆盖 → "rework_ids 丢了
   就从行上反查并回填"整段消失（后果同一步重复建返工链）。**发现方式**不是读代码，是
   "src 内定义了但全仓只出现 1 次"的死助手扫描。
2. `role_lifecycle.py`：`_send_to_role` 的投递前 CLI 忙预检（`pane_cli_busy` → `"busy"`）
   丢失 → 忙 pane 单次投递要等 30s，整 tick 被拖住。由合并后的 2 个红用例暴露。
3. `role_lifecycle.py`：`_transcript_progress_after_error` 退回旧判据（撞到错误轮直接
   `return False`）→ 分支修过的"尾部本就没错误轮 = 可以清退避"丢失。
4. `engine.py:1836`：`_ensure_guard_roles` 的 `_ensure_role_alive(_role)` 丢了
   `deadline=deadline` → 巡检预算夹逼全链恒 0，分支的 ccs start 超时夹逼在生产永不触发
   （与 task_generator 丢 `import re` 同一类；由并行审查指出、补回一行）。

### 不补回的两处
1. `cron_scheduler._noop_delta`：分支里也 0 调用方（只有测试在测），main 的剪枝是对的 →
   删掉对应 2 个用例，并去掉 `test_real_fix_passes` 里的该断言。
2. `LeverageAccumulationChecker`（分支 90 行 + 8 行接线）：整块没进合并，19 维审查指出后
   决定**不补回**——它是观测型（恒 passed），`leverage_count` 全仓唯一消费点是测试里的
   一个 mock 字段；代价是每次 eval（600s）新建一个 Blackboard 连接并对 3 个仓库各跑一次
   `git log --since`。主线 docstring 里"4 个通用检查器"的表述同步改成 3 个。

### 删除
`src/pipeflow/db.py` 与 `tests/test_workflow_db.py`：分支已连模块一起删，全仓
`class WorkflowDB` 0 处、`pipeflow.db` 引用 0 处，main 侧对该测试只有 1 行路径改动。

### 验证
`grep -rl '^<<<<<<<' /tmp/mg-r150` 空；`compileall src scripts` 0；`doccheck` CLEAN（三仓
`--root` 均 CLEAN）；死助手扫描 0 条；合并改动涉及的 16 个测试文件单跑全绿；全套
`pytest -q` = 1280 passed / 67 failed，同条件 main 基线（`9ee89e5`）= 1190 passed /
109 failed —— 67 红全部是测试间共享状态污染（单文件全绿），合并没有新增这类缺陷，
单列需求推进。

## 2026-10-22 — bus 写超时贴着实测持锁峰值

`survival_monitor._write_bus` 的 10s、`eval_checker._write_bus` 的 15s 都低于实测
bus 事务持锁（40min 内 5 次，最长 9.8s）→ 10:57 连续 3 次写失败，存活/死亡事实没落盘。
两处改 30s，不加重试、不改 bus 协议。依据与复跑命令见
`~/hermes/workspace/R153-bus写超时低于持锁时间/需求.md`。

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
