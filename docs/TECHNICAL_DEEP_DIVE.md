# Session Pipeline — Technical Deep Dive

> 更新: 2026-10-22 | 受众: 高级开发者 | 相关: [DOCS.md](DOCS.md)

## 1. 项目定位

Session 生态的**路由层**：自动感知 bus 新消息，按优先级分发给对应 CCS 消费；同时运行工作流引擎（pipeflow）。

架构图见 [AGENTS.md](../AGENTS.md) §整体架构（roles 是两个消费者，不是流水线的第一级）。

## 2. 代码布局（行数见 [CLAUDE.md](../CLAUDE.md)，由 `scripts/doccheck.py` 校验）

```
src/
  routing/            router.py 路由表/优先级 · auto.py 自动消费 · routes.py 分发 ·
                      rdb.py 路由持久化 · polling.py 拉取(去重靠 consume 记录) · survival_monitor.py 三层存活
  pipeflow/           engine.py 编排+CLI（导入 4 个 mixin）
                      · step_handler.py 步骤处理 · role_lifecycle.py 投递与守护 ·
                      task_generator.py 任务/异常/自愈 · wf_lifecycle.py 推进/回收/loop ·
                      daemon.py 守护循环（10s） · integrity.py 完整性提醒（DB 在 workflow/db.py，pipeflow 无 db.py）
  lifecycle/manager.py  工作流状态机（RLock + WAL）
  workflow/             client.py 角色客户端 · gateway.py 门禁 · db.py · sync.py
  routing_daemon.py     路由守护（含 P0 阶梯扫描）
  conversation_monitor.py 对话增量信号
  reliability.py / reliability_core.py / config_loader.py / template_registry.py / paths.py
```

## 3. 工作流引擎

### 3.1 模板与数据模型

- 规范模板目录 `~/.hermes/workflows/*.json`：见 `template_registry list`（数量不写死）。
  `python3 -m pipeflow.engine list` 打印引擎实际加载的名字。
- `WorkflowDef(name, title, description, steps, loop)`；`WorkflowRun(id, workflow_name, context, status, current_step, step_retries, step_results)`。
- `Step` 关键字段：`type`(single/handoff/review/gate/notify)、`target_role`、`exit_condition`、`exit_schema`、`max_retries`、`verify`、`subflow_template`、`guard`、`wait_roles`。
  模板里多写的键不会报错也不会生效（加载器按 `Step` 字段过滤）：`condition`/`rollback_to` 已于 2026-10-22 从模板删除；`approval_prompt` 仍留在 WL-09 与 product_delivery，无读者。

### 3.2 subflow（编排 → 子链）

2026-10-22 扫描（复算：解析 `workflow_templates.steps_json` 统计 `subflow_template` 非空的模板——
`like`/`grep` 会命中所有模板的空值键，会算多）：**28 个模板里 6 个带子链**：
`architect_review`、`coordinator_schedule`、`dev_implement`、`food_chain_loop`、
`full_dev_pipeline`、`产品交付全流程`（DB `workflow_templates` 42 行，其中 10 行带子链）。
DB 里仍有 `WF-*`/`WL-*` 重名副本（如 `WL-full_dev_pipeline`、`WF-architect_review`）；`WF-product_delivery` 只是"产品交付全流程"的 workflow_id。
名单会随模板增删变，别把这份当一个不变量。

### 3.3 步骤状态机（`lifecycle/manager.py`）

| type | 完成策略 | 自动推进 |
|------|---------|---------|
| single / notify | 直接完成 | 是 |
| handoff | 置 step_done_ready，引擎 tick 推进 | 是 |
| review | 等 target_role 经 bus 文本给出结论（`CHANGES_REQUESTED`/`rejected`，判据在 `review_gate.py`） | 否 |
| gate | `completion_check`/exit_schema 校验通过才放行 | 条件满足时 |

**审批权威性**：`confirmed_at` 优先于 `exit_messages` 文本判驳（[ADR](ADR-APPROVAL-AUTHORITY.md)）。审批密钥通道已于 2026-10-22 全链路退场（见 CHANGES.md）。

### 3.4 状态落盘：SQLite 是唯一真相

`_tick()` 的中间状态（`timeout_count`/`poll_since`/`last_reminder`/`last_heal`）通过 `_sync_step_results()` 直接 UPDATE `workflow_instances.step_results`。
若只写内存/临时对象，超时计数会在下一轮 `run_once()` 被重置为 0，超时提醒、升级、自愈都不会触发。

## 4. 路由

### 4.1 数据源

1. 先读 `routing.db`（持久化路由表）
2. 回退 `~/.hermes/data/roles_export.json`（`shared_loader.py --export` 生成）

改角色定义后必须重新 export；Router 是进程内单例，改动需重启进程。

### 4.2 优先级

`CATEGORY_PRIORITY` 由 `routing/router.py` 载入的 `~/.hermes/data/bus_canonical.json`（`_CANONICAL`）推导：分类数随 `~/.hermes/data/bus_canonical.json` 变（复算 `jq length`）；优先级 `security`/`security_audit`=1、`code_fix`/`code_review`/`bug_report`/`blocker`=2、`architecture`=3，未列出的默认 11（`router.py` 的 `_CANONICAL` 推导处，按名检索）。

### 4.3 路由即建链

`route_to_ccs()` / `route_all()` 推送成功后调用 `WorkflowClient.create_task_v2(..., bus_category=category)` → `TemplateRegistry.recommend()`（角色过滤 → bus 分类 +5 → trigger_scene 2-gram）选模板建实例。找不到模板则跳过，退化为原始推送（best-effort，不阻塞）。

### 4.4 对话增量信号（`conversation_monitor.py`）

`capture_role_pane(role, lines=80)` 用 `tmux capture-pane -S -80` 只读抓视口；`parse_conversation_signals(pane_text, role) -> ConversationSignal` 正则判定 完成 / idle / 越界；`get_conversation_signal(role)` 一行取结果，`scan_and_propose(roles)` 批量出建议。
信号是 `exit_condition` 的补充（可提前推进尝试），不替代 bus 判定。测试：`pytest tests/test_conversation_monitor*.py`。

## 5. 可靠性层（`reliability.py` + `reliability_core.py`）

`config/config.yaml` 的 8 个配置块：`bus` `retry` `circuit_breaker` `heartbeat` `ttl_pruner` `logging` `graceful_shutdown` `health`（无 `priority`/`routing` 块——优先级表在 `~/.hermes/data/bus_canonical.json`，由 `router.py` 载入）。

| 机制 | 配置键 | 语义 |
|------|--------|------|
| 指数退避重试 | `retry.max_retries/base_delay/max_delay` | 退避重试 |
| 熔断器 | `circuit_breaker.failure_threshold/recovery_timeout/half_open_max_calls` | 连续失败开路，30s 后半开 |
| 心跳 | `heartbeat.stale_threshold/cleanup_interval` | 5 分钟无心跳判 stale |
| 消息 TTL | `ttl_pruner.max_age_days/max_facts/interval` | 每小时清理 |
| 去重 | bus `log` 表 `(fid, 'pipeline')` consume 记录（`routes._do_consume_msg` 末尾） | 每轮不重投；`pipeline_cursor.db` 已于 2026-10-22 退役 |
| ACK 跟踪 | `ack_tracker.db` | 消费确认状态 |

## 6. 配置加载（**无**热重载）

`config_loader.py` 只有 `get_config()`（懒加载缓存读，:158）——**没有** `reload_config`，也没有 mtime 热重载；`reconfigure()` 在 `src/reliability.py:reconfigure`（重建 CIRCUIT_BREAKER/HEARTBEAT/TTL_PRUNER/DEFAULT_RETRY/GRACEFUL_SHUTDOWN 单例）。改 `config.yaml` 需重启进程。

### 6.1 eval 模块的热重载契约

热重载覆盖两个文件、一个副作用：

- `eval_checker.py` **和** `eval_shared_loader.py` 任一 mtime 变化（600s 比对）就清缓存重新 import；`routing_daemon.py` 自身改动只认重启。
- `eval_consistency.py` 每 600s 预检前都会被 `_forget_eval_modules()` 清掉再导入，改它下一周期即生效——但它只在**预检**里被刷新，运行期判据仍由 `_run_eval` 执行。

- 新增**跨模块常量**（如 `eval_shared_loader.CRITERIA_WINDOW`）时，热重载与预检都必须先调用 `routing_daemon._forget_eval_modules()`。常驻进程缓存着旧模块对象，只重载 `eval_checker` 会在首轮导入撞 `ImportError`（2026-10-22 09:56 与 10:27 各一次，都是这段代码静默退化）。
- 预检失败只记 WARNING、不阻塞 eval —— 所以它静默退化的样子和"通过"一样。改这块必须看 journal 里有没有 `可执行判据` 那一行，而不是只看进程没报错。

同一段里 `_check_cron_schedules()` 复用 `CCS_INTERVAL=300`（与 `route_all_to_ccs` 同分支，主循环 tick 是 60s），且 `_any_live_targets()` 为假时整轮 `sleep(300)` 跳过 —— 所以「每 300s」是常规路径，不是硬保证（journal 实测 +303~+308s，命令超时时到 +357s）。

### 6.2 判据口径（改 `eval_checker.py` 前先读）

- 轨迹判据只看近 24h（`_TRAJ_WINDOW_S`），按记录的 `timestamp` 过滤，不看 jsonl 文件 mtime；缺时间戳的记录按新鲜保留（不静默丢证据）。理由：一条历史 orphan 会让角色永久 FAIL，当轮改进翻不了绿。
- 文本判据按 **ASCII 词边界**比对（`_has_word`）：`inactive` 不点亮 `active`、`5xx-error` 不被 `error` 拆词；CJK 无词边界，按子串命中（`检查通过` 命中 `通过`）；否定语义写成负向 expected（`不通过`），不要用正向词去判可能带否定的中文（否定是开放集合）。「全部返回 X」要求 stdout 每一行都满足。
- 盲改只豁免三类：本次写调用新建的文件（`created_by_call`）、角色自己 `ccs-workspaces/<role>` 下的 Write、同一 transcript 内先写后 Edit（`own_written`）。`/tmp` 草稿靠「本次新建」豁免，不按目录放过——覆盖预存脚本仍应报。
- bus 写超时 30s：事务持锁实测 5~23s（11:57 为 19.1s、12:11 为 23.4s），15s 会周期性丢 blocker/notice。

## 7. 与 Sister Bus 的集成

协议走 `bus_protocol.Blackboard` 直接 API（不 subprocess 解析字符串）：

```python
from bus_protocol import Blackboard
bb = Blackboard()
facts = bb.unconsumed()                      # 拉取未消费
bb.read(cat="security", limit=10)            # 按分类读
bb.write("code_fix", "消息", src="pipeline")  # 写入
```

依赖：`~/.hermes/sister_bus/blackboard.db`、`~/.hermes/scripts/bus_protocol.py`。
`src/paths.py` 把 `~/.hermes/scripts` 注入 `sys.path`，使模块不依赖解释器的 `.pth` 安装状态。

## 8. 角色存活探测：一条判据（2026-10-22）

- 判据只有一条：`liveness.tmux_snapshot()` 里 pane 是否存在（一次 `list-panes -a` 拿全部 `ccs-*`，缓存 2s，**失败也缓存**——否则卡死时每次调用各付满 5s）。`_is_agent_alive` 三态：`True` 活 / `False` 判死 / `None` 探测不可用（不判死、不拉起）。
- 同一事实只付一次往返。曾并存三条探针：`_check_and_start_role` 先 `tmux has-session` 再 `_is_agent_alive`，`_wait_for_role_ready` 每 5s 再付一次 `has-session`，而三条问的是同一件事。19 个角色 × 每 tick 就是 19 次多余子进程，tmux 卡死时每次顶到 5s（实测 12:45 单次 `has-session` 101.8s，巡检 480s、单 tick 399s）。现在两条调用点都只读快照：`grep -c '"tmux", "has-session"' src/pipeflow/role_lifecycle.py` 为 0（裸 `has-session` 仍出现 4 次，全是解释这段历史的注释）。
- 判死 = 没有这个 pane，因此**没有东西可 kill**：原「`sleep(1)` + `fresh=True` 二次确认 + `kill-session`」是空动作，每个死角色白付 1s 和一次绕过缓存的快照（19 个角色 ≈ 19s/tick），已整段删除。拉起后仍然复判：轮询里 `True` → `sleep(3)` 等 agent 起来 → `fresh=True` 再确认，`None`（tmux 卡死）→ `break` 交给下一轮，不再 30×1s 空转。
- 不要给 `_is_agent_alive` 加进程名白名单：agent 执行 `/goal` 时 `pane_current_command` 是子进程 wrapper（bash/sleep/node），白名单会误杀正在干活的角色（实测 pm 被杀 8 次）。
- 投递侧（`_wait_for_role_ready`）只认 `is True`：拉起函数是 bool 契约，`None`（探测不可用）与「已拉起」共用 `True`，所以拉起后必须再复核一次 `_is_agent_alive(role) is True` 才发任务；顺带删掉这里「busy 真假都 return True」的空探测（忙闲判定归 `_send_to_role` 的 `pane_cli_busy`），睡眠用 `min(check_interval, 剩余预算)`，不再越过 deadline。
- 同一份快照也供 `routing/survival_monitor._l1_check` 用（它原先每角色一次 `tmux has-session`，3s 超时）：「探不到」按未知处理，语义以 `_l1_check` docstring 为准。旧实现还叠了一层「连续 3 次失败才翻态」，但那层读 `self._cache[role]` 顶层，而 `tick()` 写的是 `{"l1": {...}}` → `prev.get("tmux_alive")` 恒 None，计数从未累加（单测也照抄了扁平形状），一次探针超时即直判 ORPHAN——红线 3 的连击护栏实际从未生效。fail-streak 记账已删；**破坏性动作**的连击护栏（`dead_checks_since`/`orphan_checks_since`，连续 3 tick 才 kill）保留。
- 回归：`test_role_alive_cache.py`（正向缓存不重复探）/ `test_ensure_role_alive_race.py`（探测失败不 kill、真死只 kill 一次）/ `test_guard_budget.py`（`ccs start` 超时按 deadline 夹逼）/ `test_pipeflow_error_paths.py`（存活角色零子进程）。
