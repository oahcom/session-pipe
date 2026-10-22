# ADR-APPROVAL-AUTHORITY: 评审文本三态是唯一判定（`confirmed_at` 仍优先）

## Status: Accepted (2026-10-20)

## Context

驳回语义由 `step_results[step].exit_messages` 的文本关键词判定。判据词有**两处**，
改词必须两处一起动：`review_gate.REJECT_KEYWORDS`（:48，全文回退 `_full_scan` 用，7 词）
与 `review_gate._REJECT_WORD`（:64，锚点结论用，多认「不通过」）。消费者共用 `is_rejected`，
不再各写一份词表。

**通过侧仍是两档，不是单一 helper**（2026-10-22 复核）：`review_gate.verdict()` 是宽档
（`_PASS_WORD` 含「通过/就绪」，会把「构建通过」判 True），`pipeflow/task_generator.py`
的 `_HARD_APPROVE` 是窄档（只认 (CONDITIONAL_)?APPROVED / 验收通过 / 判定通过 / 结论通过），用于
"Do 子链驳回是否已被父步更新结论覆盖"这类**代价不对称**的判断——误放行会把被驳回的
步推成 completed，所以宁可漏放行。

`需修复` 与 `回退` 已于 R105 移除：前者是 `无需修复` 的子串（maintainer 健康
巡检固定写「无异常需修复」，全库 18 例命中全为误报），后者在生产里作动词
常态出现（「回退重投第4次」「代码回退 time.time()」，2 例皆误报）。
`confirm_step` 批准时只写 `status=completed` + `confirmed_at`/`confirmed_by`，**不清除** `exit_messages`——
那是该步第一轮的真实驳回文本，被保留为产出证据（`task_evidence.py`、`wf_lifecycle` 的
stale-rescue 都读它）。

于是同一份**历史**驳回文本，在审批通过后被下游判据再次读到，把刚落的 `completed`
二次判回 `rejected`。生产事故 `wf_256be862ea86/s3`：17:32/17:36 reviewer 两次 `wf confirm`
通过，`status` 却停在 `rejected` 达 5h，父链再未推进。
**后果：步骤一旦被驳回过，此后永远无法通过审批——审批通道整体失效。**
取证局限：该实例的 `step_results` 此后被 `cancel_step` 整体覆写（现存仅 `reason`），
DB 里已无法复现 `confirmed_at` 与 `rejected` 并存，只剩当时的会话记录。

`is_rejected` 在生产代码中有 **5 个消费者**（非 1 个）：

| # | 消费者 | 作用 | 现有守卫 |
|---|--------|------|----------|
| 1 | `lifecycle/manager._advance_unsafe` | 角色 `wf complete` 推进门禁 | ✅ `_confirmed` + 子链接管 |
| 2 | `pipeflow/wf_lifecycle._advance_production_wf` | 引擎 tick 推进门禁 | ✅ `_confirmed` + 子链接管 |
| 3 | `pipeflow/task_generator._subflow_verdict_rejected` | 子链末步驳回检测 | ✅ `confirmed_at`（R105 补齐） |
| 4 | `pipeflow/task_generator._conclusion` | 段级判驳 + 全文回退（三态外壳，内部仍调 is_rejected） | ✅ 三态 |
| 5 | `pipeflow/task_generator._superseded_by_parent` | 父步更新结论是否覆盖子链驳回 | ✅ 三态 |

重投清理键 `engine._REQUEUE_DROP` 不做文本判断（见 Decision 2）。

## Decision

**评审文本三态（`approved` / `rejected` / `unknown`，由 `review_gate.review_verdict` 产出）是唯一判定：
`unknown`（review/gate 步且执行者是 reviewer）不推进、`conditional_approved` 算 `approved`；`confirmed_at` 仍优先于文本判据——已确认的步骤
不再读 `exit_messages`，等价于 `approved`。审批密钥通道 2026-10-22 全链路退场（签发/拉取/确认校验
三处已删），确认只凭「该步审批人」身份鉴权。**

1. 任何据 `exit_messages` 文本判定「驳回」的消费者，必须先把 `confirmed_at` 作为前置：
   已确认的步骤不再判驳回。
2. 新一轮真实驳回由重投清理 `confirmed_at`/`confirmed_by`（`_REQUEUE_DROP` 已含）保证
   不被旧确认抑制——即「抑制」只作用于**同一轮内**的二次读取，不跨轮。
3. 驳回判据收敛为单一 helper（`review_gate.is_rejected`），杜绝第 6、第 7 个消费点
   重演同一缺陷（见 Residual）。**不是例外**：判断"父步是否已用更新结论覆盖子链驳回"用的
   `task_generator._conclusion` 是三态外壳，段级判据仍复用 `is_rejected`（:146/:158）。
4. **子链权威性**：`step_results[step].subflow_id` 非空**且该子链已终结**（`_live_subflow`
   为假）的步骤，父步自己的 `exit_messages` 不再参与驳回判定——该步的结论由子链末步给出
   （`task_generator` 已按子链判词压 `rejected`）。子链还在跑时驳回仍然有效：那时该步尚未交付，
   父步收到的否决是实时的（codex 第 3 轮 Q1）。
   理由：软归属早先塞进父步的兄弟实例驳回原文，会每 tick 否掉一次「子链判通过 →
   `completed`」，与 `task_generator.py:460-461` 的完成写入互抵——`wf_1ecc8/s3`
   完成↔驳回互抵 13h 217 次，链永不可能前进。
   这条守卫**不解析文本**：按文本判「是否为别的实例报工」会把点名兄弟实例的真驳回一起吞掉
   （fail-open），也依赖调用方把模板名传对（`manager._advance_unsafe` 拿到的是 dict，
   `getattr(wf,"name")` 恒空）。两条推进门禁（消费者 #1/#2）都装了这条。

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
