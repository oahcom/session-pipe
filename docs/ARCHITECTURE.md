# session-pipeline Architecture

> Verified: 2026-10-22（本文件不含行数；模块数用 `find src -name '*.py' -not -name '__init__.py' | wc -l`）| Audience: Architects | Related: [SYSTEM_LANDSCAPE.md](SYSTEM_LANDSCAPE.md), [DOCS.md](DOCS.md)

Session ecosystem's **routing + execution layer** -- message routing (router), auto-dispatch (auto_route), workflow engine (workflow), lifecycle state machine, reliability infrastructure.

## Position in ecosystem

```
hermes-session-roles (定义层)      ← 角色 JSON + roles_export.json
  └──→ session-launcher (执行层)   ← CCS 进程管理
  └──→ session-pipeline (本项目)   ← 消息路由 + 工作流执行
         └──→ launcher/ccs.py send ← 唯一跨项目调用（subprocess）
```

**Pipeline and launcher do NOT communicate directly.** Pipeline discovers CCS status via sentinel files (`~/.hermes/run/ccs-sentinels/`) and dispatches messages via `subprocess.run([sys.executable, CCS_CLI, "send", ...])`.

## Module inventory

| Module | Responsibility |
|--------|----------------|
| **pipeflow/engine.py** | Data-driven workflow execution engine (core, imports mixins below) |
| **pipeflow/step_handler.py** | 步骤处理 mixin：超时/排队/升级/完成 (from engine.py split) |
| **pipeflow/role_lifecycle.py** | 角色生命周期 mixin：存活检查/通知/发送 + _send_to_role (from engine.py split) |
| **pipeflow/task_generator.py** | 任务生成 mixin：任务扫描/异常检测/自动任务 (from engine.py split) |
| **pipeflow/wf_lifecycle.py** | 工作流生命周期 mixin：推进/回收/loop/部署 (from engine.py split) |
| **lifecycle/manager.py** | Workflow state machine (5 step types, approval/rollback/escalation) |
| **template_registry.py** | 模板注册中心 |
| **eval_checker.py** | 安全白名单命令执行检查 |
| **workflow/client.py** | High-level API for CCS roles to interact with workflow DB |
| **reliability_core.py** | Core implementations of retry/circuit/heartbeat/TTL/metrics |
| **reliability.py** | Init retry, circuit breaker, heartbeat, TTL pruner from config |
| **routing/routes.py** | Dispatch logic: route_all, route_to_ccs |
| **routing/router.py** | Route table: derive produce/consume from roles_export.json, priority routing |
| **routing/survival_monitor.py** | 存活监测 |
| **routing/rdb.py** | Route table SQLite persistence + audit log |
| **routing/auto.py** | Auto-consumption: poll bus, prioritize, dispatch with circuit breaker |
| **routing_daemon.py** | 路由守护进程 |
| **task_evidence.py** | 任务证据链 |
| **contract_updater.py** | 合约同步 |
| **conversation_monitor.py** | 对话监控 |
| **config_loader.py** | YAML config loading with env override and hot-reload |
| **workflow/gateway.py** | 工作流网关门禁 |
| **drift_detector.py** | 架构漂移检测 |
| **cron_scheduler.py** | cron 调度器 |
| **eval_consistency.py** | 评估一致性 |
| **routing/consistency.py** | 路由一致性 |
| **pipeflow/daemon.py** | Main loop: `while True: engine.run_once(); sleep(10)` |
| **workflow/db.py** | 工作流数据访问 |
| **output_validator.py** | 输出格式验证 |
| **routing/polling.py** | Message polling (dedup via bus consume records) |
| **paths.py** | 路径常量 |
| **workflow/sync.py** | 工作流同步 |
| **p0_exemption.py** | P0 豁免通道 |

**本表只列主要模块，不是全集**：模块数用 `find src -name '*.py' -not -name '__init__.py' | wc -l`（2026-10-22 为 37；表里没列 `liveness.py`/`review_gate.py`/`pipeflow/integrity.py`/`eval_shared_loader.py`）。`pipeflow/db.py` 已删，持久化在 `workflow/db.py`。

## Data flow

```
Bus new message (Blackboard)
    │
    ▼
polling.poll_unconsumed() ──→ router.get_consumers_prioritized()
    │                                │
    │  with consume-record dedup      │  from roles_export.json
    ▼                                ▼
routes.route_all() ──→ auto.consume_with_linkage()
    │                                │
    │  subprocess ccs.py send        │  mark_consumed for all consumers
    │                                │  无消费者（@tag 过滤为空）也标记，否则视图只增不减
    ▼                                ▼
CCS (tmux + claude) ←── Sister Bus (ACK/linkage)
    │
    ▼
pipeflow.engine.run_once() ←── pipeflow.daemon.py (10s loop)
    │
    ├── _ensure_role_alive() ──→ subprocess ccs.py start
    ├── SQLite scan workflow_instances (pending/running/step_done_ready；行级还可能是 delivery_blocked)
    ├── _send_to_role() ──→ subprocess ccs.py send
    └── _check_anomalies() ──→ timeout escalation
```

## Key dependencies

- **roles_export.json** (`~/.hermes/data/roles_export.json`) -- route table source from shared_loader.py
- **Sister Bus** (`~/.hermes/scripts/bus_protocol.py`) -- Blackboard read/write
- **CCS CLI** (`session-launcher/src/ccs.py`) -- message delivery via subprocess
- **Sentinel files** (`~/.hermes/run/ccs-sentinels/`) -- CCS alive check
- **Workflow templates** (`~/.hermes/workflows/*.json`)
- **SQLite DBs** (`~/.hermes/state/workflows.db`, `routing.db`, `ack_tracker.db`) —— `pipeline_cursor.db` 已退役（0 行，无人读）

## Route table construction

Pipeline does NOT read persona JSON directly. The router loads from:

1. **SQLite first** (`routing.db`) -- persisted route table
2. **Fallback** (`~/.hermes/data/roles_export.json`) -- from `shared_loader.py --export`

```python
# router.py
_ROLES_EXPORT_PATH = Path.home() / ".hermes" / "data" / "roles_export.json"

def _load_roles_export():
    return {r["name"]: r for r in json.loads(path.read_text()).get("roles", [])}
```

This means: **modifying persona JSON requires `shared_loader.py --export` before pipeline picks up changes.** The registry does write a bus notification on change, but pipeline's router.Singleton must be re-created to reflect changes.

## Known issues

### Engine bypasses LifecycleManager

绕过 `lifecycle/manager.py` 直接写 SQL 的不止一处（`wf_lifecycle.py` 内 `conn.execute` 实测 21 处）。engine.py 拆分后，三个典型函数分散在 mixin 里：

- `wf_lifecycle.py:_advance_production_wf()` -- updates step_results via raw conn.execute
- `step_handler.py:_tick()` -- timeout detection writes directly via `_sync_step_results()`
- `task_generator.py:_scan_tasks()` -- sub-workflow completion pushes parent workflow

These bypasses lose RLock protection and approval workflow.

### Cross-project call boundary

`route_to_ccs()` calls `ccs.py send` via subprocess. There is no documented API contract or version check between pipeline → launcher. If launcher changes CLI flags, pipeline silently fails.

### Delivery prechecks

`role_lifecycle._send_to_role` checks CLI busy (`liveness.pane_cli_busy`: bottom-bar `esc to interrupt` / `retrying`) before `ccs send`; busy returns `"busy"` for the next tick instead of starting a 30s child process that blocks the whole tick. Skipped on force=True (timeout re-push, dual-node, integrity reminder) or when wf_id is empty; a failed probe sends anyway. Only the bottom 6 lines are scanned, so marker words inside a role's own output do not pin it busy.

## Tests

- Full suite: `python3 -m pytest tests/ -q`（全 passed、skip 数固定；**条数不写死**，并发改动每天都会变）。跑法与判据见 [TEST_WORKFLOW.md](TEST_WORKFLOW.md)。
- Per-file counts are not maintained; run the suite for current numbers.
- 本文件**不写行数**（表格里的 Lines 列已删：没人维护，实测一轮就漂 6 处）。行数只在 CLAUDE.md 的代码布局树里，由 `python3 scripts/doccheck.py` 校验（`--fix` 就地刷新），pre-commit 不一致时打印 `DOC-DRIFT` 提示、不拦提交。

## Red lines

- Do NOT create CCS directly -- lifecycle is launcher's responsibility
- Do NOT modify bus_protocol core logic
- Route mapping auto-generated from roles_export.json -- no hardcoding
- Check CCS alive before dispatching
- 审批确认（`confirmed_at`）优先于 `exit_messages` 文本判驳，判据收敛在 `review_gate.py`（见 `docs/ADR-APPROVAL-AUTHORITY.md`）
- Zero external dependencies (stdlib + hermes_bus only)
- No eval() -- use safe regex pattern matching
