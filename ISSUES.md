# session-pipeline 问题单

审查时间：2026-10-16 | HEAD：`e2fd23c` | 生产 DB：`~/.hermes/data/state/workflows.db`

标注说明：**[已验证]** = 本机实际执行复现；**[代码确认]** = 读码确认逻辑缺陷，未构造运行时复现。

生产 DB 现状：`workflow_instances` 87 条（cancelled=45 / completed=41 / delivery_blocked=1），无积压；freelist ratio 0.0%（无膨胀）。以下缺陷多为"尚未踩中"的潜伏缺口，非当前故障。

---

## G1 [P0] `run.workspace` 属性不存在 → codex review 质量闸门恒抛异常 [已验证]

- 位置：`src/pipeflow/step_handler.py:877`
- 证据：
  ```python
  capture_output=True, text=True, timeout=300, cwd=run.workspace
  ```
  `WorkflowRun`（`engine.py:147-163`）字段仅 `['id','workflow_name','context','current_step','step_retries','status','created_at','updated_at','step_results']`，全仓无 `.workspace` 赋值。实测：
  ```
  hasattr(WorkflowRun, 'workspace') → False
  ```
- 失败场景：任何 `dual_node` 步骤（生产模板 `food_chain_loop.json` s1/s2）第二节点触发时，`_run_codex_review` 在 try 内首行抛 `AttributeError`，被 `:892` `except Exception` 捕获后返回 `{"success": False, "error": "codex review 异常: ..."}`。`subprocess.run` 从未执行 —— codex 三方审查这一整条质量闸门形同虚设，日志只写 warning 后照发 prompt 让角色"人工审查"。
- 修复方向：传入 `_CCS_WORKSPACES / step.target_role`，或给 `WorkflowRun` 补 `workspace` 字段。

## G2 [P0] `survival_monitor._l3_check` 调用 bus_client 不存在的参数 → 产出信号恒 False [已验证]

- 位置：`src/routing/survival_monitor.py:216-221`
- 证据：
  ```python
  [sys.executable, str(BUS_SCRIPT), "read", "--src", role, "--limit", "5", "--json"],
  ```
  `bus_client.py read` 只注册 `--cat/--limit/--consume/--watch/--json`（`--src` 仅属 `write`）。实测：
  ```
  bus_client.py read --src qa ... → error: unrecognized arguments: --src qa
  SurvivalMonitor(engine=None)._l3_check('qa') → {'producing': False, ..., 'detail': 'bus 无匹配产出'}
  ```
- 失败场景：`returncode != 0` → 恒返回 `{"producing": False}`。连带：`_overall()` 在 L1=ALIVE 且 L2 无 token 计数时靠 L3 定结论，现恒走 `producing is False → "idle"`；`tick()` 的 idle 回收判据 `_producing = _l3.get("producing") is True` 恒假 → ondemand 角色只要 L2 拿不到计数就被连判 6 次 idle 击杀。
- 修复方向：改用进程内 `Blackboard().read(src=role, limit=5)`（同 `role_lifecycle.py:374` 的 `_recently_producing` 修法）。

## G3 [P1] `_cleanup_stale_workflows` 的 SELECT 漏 `current_step_id` → stale rescue 与超时条件 4 恒失效 [代码确认]

- 位置：`src/pipeflow/wf_lifecycle.py:84-86`（SELECT）与消费点 `:137`、`:148`
- 证据：
  ```python
  "SELECT instance_id, template_id, created_at, step_results, status "
  "FROM workflow_instances WHERE status IN ('running', 'step_done_ready')"
  ```
  随后 `_cur = d.get("current_step_id", "")`（`:137`、`:148`）—— 查询未选该列，`d.get(...)` 恒为 `""`。
- 失败场景：回收前"抢救已产出步骤"分支 `sr.get(_cur, {})` 恒取 `{}`，`exit_messages` 判据永假 → 有产出的工作流不走 `complete_step` 闭合，直接 `status='cancelled'`；条件 4（running 且当前步超时 >1h）同样恒不触发。
- 注意：本条与 `REVIEW_R7_regression.md` 的结论冲突 —— 该报告逐条核对了条件 4 的 `timeout_count` 读写路径，但未发现 SELECT 根本未取 `current_step_id`，故其"条件 4 已正确实现"的判定无法成立。
- 修复方向：SELECT 补 `current_step_id`。

## G4 [P1] `_check_gate_condition` 用 `Path(p).exists()` 直判 `output_exists` → gate 步骤永久阻塞 [代码确认]

- 位置：`src/lifecycle/manager.py:918-922`
- 证据：
  ```python
  output_exists = check.get("output_exists", [])
  if output_exists:
      missing = [p for p in output_exists if not Path(p).exists()]
  ```
  实测 `Path('~/ccs-workspaces/*/DESIGN.md').exists()` → False；`Path('变更代码文件').exists()` → False；`Path('deploy_success').exists()` → False。生产模板 `技术实现.json`、`需求流转.json` 均使用该键。
- 失败场景：任一 `type=gate` 步骤 `complete_step` 即返回 `gate_blocked`，步骤 status 写 `"blocked"`。加重项：`"blocked"` **不在** `STEP_STATUSES`（`manager.py:65`）、**不在** `_ACTIVE_STEP_STATUS`（`liveness.py:40`），`engine.run_once` 扫描条件（`engine.py:648`）与 `_cleanup_stale_workflows`（`wf_lifecycle.py:86`）均不含它 → 步骤与实例都收不到回收，只走 24h 全局阈值；`escalate_step` 全仓无生产调用点。
- 未踩中：DB 实测 blocked 计数为 0。
- 修复方向：`output_exists` 支持 glob/目录匹配；把 `blocked` 纳入活跃步骤状态集。

## G5 [P1] `test_harness` 正则误伤真实 QA 模板 → 引擎主动取消真实 QA 工作流 [已验证]

- 位置：`src/pipeflow/task_generator.py:207-210`（同判据亦见 `:300`）
- 证据：
  ```python
  _is_test_harness = bool(re.match(
      r"^(test_|.*_test|exit_schema|role_interact|drill)", inst.get("template_id","") or ""))
  ```
  实测 `re.match(..., 'qa_test_execute').group(0)` → `'qa_test'`；`qa_test_plan` 同理。两者是**生产模板**（`~/.hermes/workflows/qa_test_execute.json`、`qa_test_plan.json`），且被 `full_dev_pipeline.json`、`architect_review.json`、`food_chain_loop.json` 作为 `subflow_template` 引用。
- 失败场景：`timed_out_steps >= 3` 时被 `UPDATE workflow_instances SET status='cancelled'` 静默取消，且 `_generate_tasks_from_state` 永久拒绝为 QA 生成自动任务。DB 实有 4 条 `qa_test_execute` 实例，其中 2 条 cancelled（`wf_sub_2e5075f94768`、`wf_60228246`）—— 这 2 条是否由本路径取消**未证实**（未回溯其 `timed_out_steps`），但取消通道已确认可用且判据必命中。
- 修复方向：改显式白名单集合，或把正则锚到 `^test_`/`^drill` 并去掉 `.*_test`。

## G6 [P1] `_validate_exit_schema` 的 `mustContainAnyOf`/`mustNotContain` 未列入"缺产出"属性 → 契约门可绕过 [代码确认]

- 位置：`src/pipeflow/step_handler.py:967`
- 证据：
  ```python
  for prop_key in ("minLength", "mustContain", "mustContainUrl", "checksum", "maxAgeMinutes"):
  ```
  实测 `Step(exit_schema={"properties":{"MISSING_FILE.md":{"mustContainAnyOf":["A","B"]}}})` + 文件不存在 → 返回 `ok=True, errs=[]`。
- 失败场景：以 `mustContainAnyOf` 作唯一内容判据的 exit_schema，在文件完全不存在时校验通过，步骤直接盖戳完成。（`food_chain_loop/s2` 的 `PRD.md` 同时带 `minLength`/`mustContain`，故未踩中。）
- 修复方向：把 `mustContainAnyOf`、`mustNotContain` 一并加入该元组。

## G7 [P1] `_check_anomalies` 自愈重投无冷却 → 每 10s 重推同一 prompt [代码确认]

- 位置：`src/pipeflow/task_generator.py:228-229`
- 证据：`_heal_send` 唯一写入的 `last_heal` 字段**全仓无读取点**（`grep -rn "last_heal" src/` → 仅 `task_generator.py:131` 写入、`:174` 注释、`:227` 注释）；对比 `_kick_stalled_roles` 有 `_KICK_COOLDOWN_SEC = 600` + `kick_stalled_at` 落库冷却。
- 失败场景：实例 `timed_out_steps >= 3` 后，daemon 每 10s（`daemon.py:43 INTERVAL`）一轮 `run_once → _scan_tasks → _check_anomalies`，每轮满足即重推；`_heal_send` 内 `_send_to_role` 未传 `force=True`，角色 busy 时被并发守卫拦下返回 False 不盖戳 → 下一轮继续重推，无退避无上限。
- 修复方向：`_heal_send` 读 `last_heal`，距上次 < 600s 直接 return。

## G8 [P2] `delivery_blocked` 达恢复上限后成为永久孤儿（DB 中已存在实例）[已验证]

- 位置：`src/pipeflow/engine.py:986-990`，配合 `:648` 扫描条件
- 证据：
  ```python
  _rc = step_sr.get("recover_count", 0)
  if _rc >= 3:
      LOGGER.debug("recover skip: %s/%s 已达恢复上限 %d 次", ...)
      continue
  ```
  DB 实测 `wf_sub_ee176a2b55a1`（`reviewer_pr_review`/s1，created 10-16 15:57）：`step_results={'s1': {'delivery_fail_count': 3, 'status': 'delivery_blocked', 'recover_count': 3}}`。
- 失败场景：`run_once` 只查 `status IN ('pending','running','step_done_ready')`，`_cleanup_stale_workflows` 只查 `('running','step_done_ready')` → 该实例永久停在行级 `delivery_blocked`，既不推进也不回收、不进任何回收阈值；`LOG.debug` 级别让"已达上限"完全不可见。
- 修复方向：达恢复上限时转 `failed`/`cancelled` 终态并写 blocker；日志升 warning。
- 与既有问题单的关系：`~/.hermes/notes/audit/THREE_PROJECT_ISSUES_2026-10-16.md` 的 **TP-004** 记录了同一实例 `wf_sub_ee176a2b55a1`，角度是 `p0_exemption.py:155,404` 的终态列表漏 `delivery_blocked`（被反复扫成"超时"）。两条互补：TP-004 修"谁扫到它"，本条修"它为何永远停在这"。**建议一并修**，只修一处该实例仍是孤儿。

## G9 [P2] `wf_starve.json` 中 `last_active_ts` 是幽灵字段 [已验证]

- 位置：`daemon.py:162-166`（唯一写入点）；`~/.hermes/run/wf_starve.json`
- 证据：实测文件 `{"last_fire": 1792132065.97, "last_active_ts": 1786159984.85, "last_wf": "wf_ae31fd1c4c11"}`，`last_active_ts` 比 `last_fire` 早约 69 天；全仓无写入/读取该键的代码（`grep -rn "last_active_ts"` 仅命中 `production_dispatcher.py` 与 `stability_test_runner.py` 中**另一套 schema**）。
- 失败场景：消费方（运维/看板/后续维护者）若按字段名理解为"最近活跃时间"，得到已停更 69 天的陈旧值，误判引擎长期停摆。
- 修复方向：删除该键，或显式写为 `state["last_active_ts"] = now`。
- 备注：`last_fire` 当前值健康（2026-10-16 14:27），未复现历史"last_fire 损坏"。

## G10 [P2] `check_gate_timeouts` / `get_workflow_progress` / `get_workflow_context` 用 `step["step_id"]` 直取 DB 模板 [代码确认]

- 位置：`src/lifecycle/manager.py:999`、`:1050`、`:1076`
- 证据：
  ```python
  (tpl["template_id"], step["step_id"])
  ```
  而 `manager.py:194` 已注释"DB 存储时用 id，但 legacy 可能存 step_id —— 两者都兼容"。DB 实测全部 34 条模板步骤键均为 `step_id`，故当前不触发。
- 失败场景：删除 `_upsert_template` 的 `step_id` 键转换（`engine.py:543`）或引入 `id` 键模板即抛 `KeyError` → `check_gate_timeouts` 中断整轮 gate 超时扫描，`get_workflow_progress` 使进度视图整体报错。
- 修复方向：与 `:194` 一致改 `step.get("step_id") or step.get("id")`。

## G11 [P2] 每日 13 次服务重启，无自动拉起（部署健壮性）[已验证]

- 位置：`/etc/systemd/system/session-pipeline.service`
- 证据：`journalctl -u session-pipeline.service --since today | grep -c "Workflow Daemon 启动"` → **13 次**，全部为 `Stopped` + `Started` 成对出现（手动 restart，非崩溃）；其中 14:27:11-14:27:52 有 3 次间隔 ≤40s 的连续重启。
- 失败场景：重启期间工作流停摆（无 daemon 推进）；无自动拉起意味着崩溃后需人工介入。这是"引擎频繁重启"现象本身，非代码缺陷，但会放大 G3/G5/G8 的回收窗口问题。
- 修复方向：确认重启原因（当前 0 次锁告警、0 次崩溃退出，指向人工/脚本操作）；如为部署脚本，考虑合并为单次 restart。

---

## 跨项目

- **G2** 的修法参照 `role_lifecycle.py:374`。
- **G4** 的 `blocked` 状态收口需同步 `src/liveness.py:40`。**G6** 影响 launcher 侧 exit_schema 契约。**G11** 与 `session-launcher/ISSUES.md` L5 的 `KillMode=process` 同属部署层。
