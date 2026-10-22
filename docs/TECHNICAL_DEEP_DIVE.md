# Session Pipeline — Technical Deep Dive

> 更新: 2026-10-22 | 受众: 高级开发者 | 相关: [DOCS.md](DOCS.md)

## 1. 项目定位

Session 生态的**路由层**：自动感知 bus 新消息，按优先级分发给对应 CCS 消费；同时运行工作流引擎（pipeflow）。

```
roles (定义层 JSON) → launcher (执行层) → pipeline (路由层)
                                              ↓
                                         Sister Bus
```

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
- `Step` 关键字段：`type`(single/handoff/review/gate/notify)、`target_role`、`exit_condition`、`exit_schema`、`max_retries`、`verify`、`condition`、`rollback_to`、`subflow_template`、`guard`、`wait_roles`。

### 3.2 subflow（编排 → 子链）

2026-10-22 扫描（复算：解析 `workflow_templates.steps_json` 统计 `subflow_template` 非空的模板——`like`/`grep` 会命中所有模板的空值键，会算多）：**10 个模板带子链**（当日实测名单，同名模板有 `WF-`/`WL-` 前后缀重复，按 id 去重后仍是这 10 条）：
`WF-architect_review`、`WF-coordinator_schedule`、`WF-dev_implement`、`WF-product_delivery`、`WL-food_chain_loop`、`WL-full_dev_pipeline`、`architect_review`、`food_chain_loop`、`full_dev_pipeline`、`产品交付全流程`。
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
