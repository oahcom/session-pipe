# CHANGES.md

## 2026-10-22 — temp-pr-branch 合入 main（漂移收敛，29 文件 / 38 段冲突）

### 为什么合
main 与生产分支双向漂移 **100 / 94** 个提交：分支上的判据修复（盲改豁免、排队账本、
返工链证据、路由 claim）不在 main 上；main 的独立新增（REVIEW_ROLES、`conditional APPROVED`、
cat 下推、`_unknown_verdict_notified`、死测试剪枝）分支也没有。两条线各自"更正确"一部分。

### 裁决原则
文档/测试/源码取并集；同一函数取"更完整且有测试"的一侧；文档数字以合并后实测为准；
modify/delete 以"模块是否还存在、是否被引用"为判据。逐段记录见
`~/hermes/workspace/R151-主干合入temp-pr-branch/冲突裁决.md`。

### 合并时被"机械并集"漏掉、事后补回的三处
1. `engine.py`：`_derive_rework_ids` 只剩定义、调用点被 main 侧覆盖 → "rework_ids 丢了
   就从行上反查并回填"整段消失（后果同一步重复建返工链）。**发现方式**不是读代码，是
   "src 内定义了但全仓只出现 1 次"的死助手扫描。
2. `role_lifecycle.py`：`_send_to_role` 的投递前 CLI 忙预检（`pane_cli_busy` → `"busy"`）
   丢失 → 忙 pane 单次投递要等 30s，整 tick 被拖住。由合并后的 2 个红用例暴露。
3. `role_lifecycle.py`：`_transcript_progress_after_error` 退回旧判据（撞到错误轮直接
   `return False`）→ 分支修过的"尾部本就没错误轮 = 可以清退避"丢失。

### 不补回的一处
`cron_scheduler._noop_delta`：分支里也 0 调用方（只有测试在测），main 的剪枝是对的 →
删掉对应 2 个用例，并去掉 `test_real_fix_passes` 里的该断言。

### 删除
`src/pipeflow/db.py` 与 `tests/test_workflow_db.py`：分支已连模块一起删，全仓
`class WorkflowDB` 0 处、`pipeflow.db` 引用 0 处，main 侧对该测试只有 1 行路径改动。

### 验证
`grep -rl '^<<<<<<<' /tmp/mg-r150` 空；`compileall src scripts` 0；`doccheck` CLEAN；
死助手扫描 0 条；合并后全量 pytest 见提交说明。

## 2026-10-22 — 空闲判死接活体门 + cancel_step 终态幂等 + 文档同源

