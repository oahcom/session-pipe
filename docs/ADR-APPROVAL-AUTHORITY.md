# ADR-APPROVAL-AUTHORITY: 审批权威性高于文本关键词判驳

## Status: Accepted (2026-10-20)

## Context

驳回语义由 `step_results[step].exit_messages` 的文本关键词判定。词表自 R105 起
收敛为单一声明 `src/review_gate.py:REJECT_KEYWORDS`（2026-10-22 实测 7 词：
`changes_requested` / `rejected` / `not_approved` / `no-go` / `nogo` / `驳回` / `未通过`），
不再由 4 个消费者各写一份。

**通过侧仍是两档，不是单一 helper**（2026-10-22 复核）：`review_gate.verdict()` 是宽档
（`_PASS_WORD` 含「通过/就绪」，会把「构建通过」判 True），`pipeflow/task_generator.py`
的 `_HARD_APPROVE` 是窄档（只认 (CONDITIONAL_)?APPROVED / 验收通过 / 判定通过 / 结论通过），用于
"Do 子链驳回是否已被父步更新结论覆盖"这类**代价不对称**的判断——误放行会把被驳回的
步推成 completed，所以宁可漏放行。

`需修复` 与 `回退` 已于 R105 移除：前者是 `无需修复` 的子串（maintainer 健康
巡检固定写「无异常需修复」，全库 18 例命中全为误报），后者在生产里作动词
常态出现（「回退重投第4次」「代码回退 time.time()」，2 例皆误报）。
`confirm_step` 批准时只写 `status=completed` + `confirmed_at`，**不清除** `exit_messages`——
那是该步第一轮的真实驳回文本，被保留为产出证据（`task_evidence.py`、`wf_lifecycle` 的
stale-rescue 都读它）。

于是同一份**历史**驳回文本，在审批通过后被下游判据再次读到，把刚落的 `completed`
二次判回 `rejected`。生产事故 `wf_256be862ea86/s3`：17:32/17:36 reviewer 两次 `wf confirm`
通过，`confirmed_at` 与 `approval_consumed_at` 均已落库，`status` 却停在 `rejected` 达 5h，
父链再未推进。**后果：步骤一旦被驳回过，此后永远无法通过审批——审批通道整体失效。**

该文本判据在生产代码中有 **4 个消费者**（非 1 个）：

| # | 消费者 | 作用 | P0 修复后守卫 |
|---|--------|------|--------------|
| 1 | `lifecycle/manager._advance_unsafe` | 角色 `wf complete` 推进门禁 | ✅ `_confirmed` + 归属过滤 |
| 2 | `pipeflow/wf_lifecycle._advance_production_wf` | 引擎 tick 推进门禁 | ✅ `_confirmed` + 归属过滤 |
| 3 | `pipeflow/engine._REQUEUE_DROP` | 重投清理键（防旧确认残留放行新驳回） | ✅ |
| 4 | `pipeflow/task_generator._subflow_verdict_rejected` | 子链末步驳回检测 | ✅ `confirmed_at`（R105 补齐） |

## Decision

**审批确认（`confirmed_at`）是步骤推进状态的权威来源，优先于 `exit_messages` 文本判据。**

1. 任何据 `exit_messages` 文本判定「驳回」的消费者，必须先把 `confirmed_at` 作为前置：
   已确认的步骤不再判驳回。
2. 新一轮真实驳回由重投清理 `confirmed_at`/`confirmed_by`（`_REQUEUE_DROP` 已含）保证
   不被旧确认抑制——即「抑制」只作用于**同一轮内**的二次读取，不跨轮。
3. 驳回判据收敛为单一 helper（`review_gate.is_rejected`），杜绝第 5、第 6 个消费点
   重演同一缺陷（见 Residual）。**例外**：判断"父步是否已用更新结论覆盖子链驳回"需要区分
   「叙述」与「结论」，用 `task_generator._conclusion`（三态），不复用二值的 is_rejected。
4. **归属权威性**：替**别的实例**报工的文案不参与本步的驳回判定。判据是
   `step_handler._claims_own_step_and_foreign_instance`（消息点名别的实例、且不含本实例 id、
   且声称完成了本步），两条推进门禁（消费者 #1/#2）都要过这道滤。
   理由：软归属把兄弟实例的驳回原文存进本步后，它每 tick 否掉一次「子链判通过 → `completed`」，
   与 `task_generator.py:459` 的完成写入互抵——`wf_1ecc8/s3` 完成↔驳回互抵 13h 217 次，
   链永不可能前进。本实例自己的驳回原样拦，不受影响。
   前缀形态不限模板名：角色照抄的是提示里的**工作流标题**（`产品交付全流程/s3-complete`），
   只认模板名 `product_delivery` 时这一批消息全逃过滤。

## Consequences

**正向**
- 审批通过后步骤稳定停留 `completed`，审批通道可用。
- 消费者收敛后，新增判据无需再各自记得加守卫。

**负向 / 取舍**
- 已确认步骤若在 `exit_messages` 中真的出现了**新**驳回字样（同轮、confirm 之前已写入），
  会被抑制。经 `_REQUEUE_DROP` 清理 `confirmed_at` 后，新一轮驳回可正常触发——
  抑制窗口 = 一次审批周期，可接受。

## Residual（已由 R105 闭合）

四个消费者现已全部带 `confirmed_at` 守卫。消费者 #4 的复现命令当前返回 `False`：
```
python3 -c "import sys;sys.path.insert(0,'src');from pipeflow.task_generator import _subflow_verdict_rejected as f; \
import time;print(f({'s1':{'status':'completed','confirmed_at':time.time(),'exit_messages':[{'text':'REJECTED: 18 FINDING'}]}}))"
# → False（已确认步骤不判驳回）
```

R105 同时改变了该点的严重度定位：它不再"只写一条 blocker"，而是把父步压回
`rejected` 并阻止 advance。缺少 `confirmed_at` 守卫会变成真阻断，故本条与
词表精确化、fail-open 移除同批上线。

## 参考

- 事故取证：`~/hermes/workspace/R73-自驱链真贯通/RESULTS.md`
- 修复提交：`463a8d3`（session-pipeline）
- 回归守卫：`tests/test_confirm_not_re_rejected.py`
