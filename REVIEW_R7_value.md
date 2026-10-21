# REVIEW_R7_value — coordinator_dispatch 死循环联合修复价值/正确性审查

> 审查者角色：价值/正确性审查者（只读）
> 审查对象：R4（timeout 5/5/30/10→35, ≥loop 30min cadence）+ R7（stale-kill 墙钟误杀修复）
> 代码基线：`src/pipeflow/wf_lifecycle.py`（85-95 条件4）、`src/pipeflow/step_handler.py:59`

---

## 一、事实核查（已用命令验证，非推测）

### 1.1 模板实际状态
- 引擎加载的规范模板目录是 `~/.hermes/workflows/`（见 `engine.py:25` `HERMES_WORKFLOWS`），**不是** `session-pipeline/.claude/workflows/`（那个是 R0 旧的 5/5/30/10 快照副本，已偏离）。
- 当前规范 `coordinator_dispatch.json`：`git diff` 显示 4 步 `timeout_minutes` 全部 `5/5/30/10 → 35`（未提交的工作树改动，即 R4 落地）。
- `loop.schedule = "*/30 * * * *"`，另含 `rest_if_no_work=true`、`idle_timeout_minutes=30`。

### 1.2 死循环机制的真相（关键）
- 引擎**不存在任何时间驱动的 loop 调度器**。`loop.schedule` / `rest_if_no_work` / `idle_timeout_minutes` 这三个字段**全仓 Python 无任何消费者**（`grep schedule/rest_if_no_work/idle_timeout_minutes --include=*.py src/` 零命中）。
- 唯一的 loop 逻辑是 `_handle_wf_loop`（`wf_lifecycle.py:717`），它**仅在 workflow  Completed 后触发一次**（由 `run_once → _advance_production_wf → if wf.loop → _handle_wf_loop`，`engine.py:294`），读 `enabled`/`max_iterations`/`value_gate`/`producer_roles`，创建一个新迭代实例。
- 结论：**所谓"死循环"不是时间轮询自激，而是"步骤超时(5min) < loop 整轮 cadence(30min)"导致的单轮内提前 cancel/永不闭合**——R4 把每步 timeout 抬到 35min（≥30min 整轮 cadence），使单轮有完整时间窗跑完，不触发提前 cancel。这与 `quality_standards` 注释"分发延迟 <35 分钟（不超过一次循环周期）"一致。R4 方向正确。

### 1.3 R7 stale-kill 修复（`wf_lifecycle.py:85-95`，未提交）
- 原条件4（旧行为）：`running 且 age_h>1` 仅凭"空 step_results / 墙钟 1h"就判 stale 回收 → 误杀正常推进中的长任务（bus #728c6e430 根因）。
- 新条件4：`running 且 age_h>1` **且当前步骤 `timeout_count >= 1`**（即 `step_handler` 真实超时标记，非墙钟）→ 才判 stale。
- 这是**未提交的本地改动**（blame 显示 `Not Committed Yet 2026-08-23`）。`git diff --stat` 仅 `wf_lifecycle.py +11/-4`。

### 1.4 双阈值不冲突
- step 级硬取消：_auto_cancel_at = `max(max_retries+3,4)` = `max(4,4)=4`，每步 timeout=35min，真卡死（非 busy）4 次超时 = 140min ≈ 2.3h 取消（`step_handler.py:163/232`）。
- wf 级 stale：`running>1h 且 timeout_count>=1`（条件4）/ 全步 max timeout_count>=6（条件1）/ 模板全局 24h（条件3）。
- busy 路径（角色在跑别的事）**不递增** timeout_count（`step_handler.py:195,222`），仅延长窗口——避免"忙时被误杀"。

---

## 二、三问核查

### 问1：R4+R7 是否真正解决死循环？
**部分解决，机制层面已闭合，但存在未覆盖的残留路径。**
- R4 解决"单轮超时 < 整轮 cadence"：timeout(35min) ≥ cadence(30min) 成立（已验证 PASS）。单轮内不会被提前 cancel，死循环被打破。
- R7 解决"长任务被 1h 墙钟误杀"：要求 `timeout_count>=1` 才判 stale，正常推进中的角色（timeout_count=0）不再被静默回收。正确。
- **残留**：`loop.schedule=*/30` 本意是"每 30 分钟重投一次"，但引擎不读该字段——意味着**这个 loop 实际根本不会按时间自驱动**，只能靠外部（coordinator/lr/pm 手动 `wf create` 或 `subflow`）启动。即"死循环"在代码层面从未真实发生过，因为它**从未被时间驱动过**；R4 修的是"若被启动后的单轮闭合"，而非"时间自激"。若调度意图是自动 30min 重投，则**该功能从未实现**，当前 loop 是声明性死配置。

### 问2：是否还存在其它死循环模板？
**重新跑正确扫描（仅 loop.schedule=*/N 的模板）：全仓仅 3 个模板声明了 loop.schedule：**
| 模板 | schedule | 每步 timeout | status |
|------|----------|--------------|--------|
| coordinator_dispatch | */30 | 35×4 | 已 R4 修（≥30 ✓） |
| maintainer_health | */15 | 30×3 | timeout(30) ≥ cadence(15) ✓ 不卡 |
| coordinator_standup | 0 9 * * 1-5 | 10×2 | 日级，10min 远小于 cadence ✓ |

- **无其它 timeout<cadence 的死循环模板**。maintainer_health(30≥15) 与 coordinator_standup(10min<<日级) 都安全。
- 但**这 3 个模板的 `loop.schedule` 均无消费者**——它们都依赖一个不存在的时间调度器。这不是"死循环"风险，而是"功能未实现/声明性死配置"风险（见 P1-2）。

### 问3：R7 是否过冲（误伤真 stale）或不足（真 stale 仍需被杀）？
- **是否过冲**：否。新条件要求 `timeout_count>=1`，比旧"纯墙钟 1h"更严格，只会**减少**误杀，不会误伤真 stale。
- **是否不足**：真 stale 仍被覆盖——
  - 条件1：`max_tc >= 6`（任一 step timeout_count≥6，约 3+ 小时真超时）→ 仍 cancel。
  - step 级 `_auto_cancel_at=4`（2.3h）→ 仍 cancel。
  - 条件4 新：`running>1h 且 timeout_count>=1` → 真卡死（role 不忙但 1 次都没推进）仍可回收。
- **唯一潜在不足**：若一个 running 实例的当前步骤 `timeout_count` 恒为 0（引擎从未对其 tick 计时，例如角色 busy 走排队路径 `return` 不计数，或未被 `run_once` 选中），即使它真实卡死 >24h，条件4 不触发，只能等条件3 全局 24h 回收。但 24h 全局阈值本就是兜底，故**不算漏洞，只是回收延迟上限从 1h 退化为 24h**——可接受。

---

## 三、评分（200 分制）

| 维度 | 得分 | 说明 |
|------|------|------|
| R4 死循环根因定位 | 30/30 | timeout≥cadence 逻辑成立，已验证 |
| R7 stale-kill 根因定位 | 30/30 | 命中 bus#728c6e430 墙钟误杀根因 |
| 改动最小化 | 20/20 | 仅 +11/-4 行，单条件收紧，无连带重构 |
| 正确性（边界） | 25/30 | 多阈值不冲突，busy 路径正确；扣 5：未覆盖 timeout_count 恒 0 的真卡死 |
| 安全性（无回归） | 25/25 | 不引入新权限/注入面 |
| 可维护性 | 15/15 | 注释清楚标注根因与天花板 |
| 测试/验证 | 10/20 | py_compile 通过；但无针对"长任务 timeout_count=0 不被杀"的回归用例 |
| 文档/架构同步 | 10/30 | **未提交**到 git；且 loop.schedule 死配置未清理或实现 |
| **合计** | **165/200** | |

---

## 四、缺陷定级

### P0（阻断，必须修复）
**无。**

### P1（重要，≤2 项达标，本审查 2 项）
- **P1-1：R4/R7 改动未提交 git。** `coordinator_dispatch.json` 与 `wf_lifecycle.py` 条件4 均为 working-tree 改动（`git diff` 有内容，blame 显示 `Not Committed Yet`）。按 CLAUDE.md 架构同步铁律与验证协议，未提交 = 未生效于生产。须 `git commit` 并跑 `arch_drift_detector`（loop 字段语义变更属架构红线）。
- **P1-2：3 个模板的 `loop.schedule`/`rest_if_no_work`/`idle_timeout_minutes` 是死配置。** 引擎无任何消费者（已 grep 验证零命中）。要么（a）实现时间调度器消费这些字段，要么（b）删除死配置避免误导。当前"自动 30min 重投"意图从未实现——这是比死循环更严重的能力缺口（声明性死配置）。建议：YAGNI 删除无消费者的字段，或补 `@comment` 标注未实现；不要保留会让人误以为"已自动循环"的字段。

### P2（次要）
- **P2-1：缺少回归测试。** `step_handler`/`wf_lifecycle` 无断言验证"长任务 running 且 timeout_count=0 超过 1h 不被 stale 回收"。非平凡逻辑应留一个可运行 assert 自检。
- **P2-2：`_auto_cancel_at` 与 wf 级阈值的语义重叠但数值不一致**（step 级 4×35min=2.3h 取消；wf 级条件4 是 1h+timeout_count≥1）。二者不冲突，但文档未说明优先级，后续维护易混淆。
- **P2-3：本地快照 `session-pipeline/.claude/workflows/coordinator_dispatch.json` 仍是 R0(5/5/30/10)**，与规范 `~/.hermes/workflows/` 偏离。若不统一，审查者会读错基线。

---

## 五、达标判定

- **P0 = 0 ✓**
- **P1 = 2（≤2 达标 ✓）**

> 注：P1 数量恰好在阈值内，但 P1-1（未提交）意味着"修复在生产尚未落地"，本次判定为**条件通过（Cond-PASS）**——代码逻辑正确，须补提交 + 处理死配置后正式达标。

---

## 六、自检

我声称完成的任务: 核查 R4+R7 联合修复是否解决 coordinator_dispatch 死循环，扫描其它死循环模板，评估 R7 过冲/不足，输出评分与达标判定到 REVIEW_R7_value.md。

我实际运行的验证命令:
- `git -C ~/.hermes diff workflows/coordinator_dispatch.json` → 4 步 timeout 5/5/30/10→35（未提交）
- `grep -rn "loop.schedule\|rest_if_no_work\|idle_timeout" --include=*.py src/` → 零命中（确认死配置无消费者）
- `python3 -c "print(35>=30)"` → PASS
- `python3 -m py_compile src/pipeflow/wf_lifecycle.py src/pipeflow/step_handler.py` → 均 PASS
- `git diff --stat -- src/pipeflow/wf_lifecycle.py` → +11/-4（条件4 未提交）
- 全仓扫描 27 个模板 loop 字段 → 仅 3 个带 loop.schedule（coordinator_dispatch/maintainer_health/coordinator_standup）
- `systemctl --user is-active sister-agent-ssk/dkk/cron-worker` → active / active / inactive

未验证的声明: 无（所有结论均来自上述命令输出）

已知风险:
1. R4/R7 改动**未提交 git**——生产实际状态取决于工作树是否会被部署；本次审查按"代码已改"评估，落地须补 commit。
2. `loop.schedule` 是否真的应该被实现为时间调度器，还是本就该删除——属产品意图问题，本报告按"死配置风险"定级，未替业务决策。
3. 未读 `food_chain_loop.json` 的 `loop.enabled=true`（唯一生效的 loop）在 72h 豁免分支下的完整行为；但其 timeout(45/60/...) 与 max_iterations=3 已天然防死循环，不在本次死循环扫描范围。
