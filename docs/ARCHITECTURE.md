# session-pipeline Architecture

> Updated: 2026-07-26 | Audience: Architects | Related: [SYSTEM_LANDSCAPE.md](SYSTEM_LANDSCAPE.md), [DOCS.md](DOCS.md)

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

| Module | Lines | Responsibility |
|--------|-------|----------------|
| **pipeflow/engine.py** | 1852 | Data-driven workflow execution engine (core, imports mixins below) |
| **pipeflow/step_handler.py** | 1681 | 步骤处理 mixin：超时/排队/升级/完成 (from engine.py split) |
| **pipeflow/role_lifecycle.py** | 1197 | 角色生命周期 mixin：存活检查/通知/发送 + _send_to_role (from engine.py split) |
| **pipeflow/task_generator.py** | 874 | 任务生成 mixin：任务扫描/异常检测/自动任务 (from engine.py split) |
| **pipeflow/wf_lifecycle.py** | 1979 | 工作流生命周期 mixin：推进/回收/loop/部署 (from engine.py split) |
| **lifecycle/manager.py** | 1131 | Workflow state machine (5 step types, approval/rollback/escalation) |
| **template_registry.py** | 937 | 模板注册中心 |
| **eval_checker.py** | 1459 | 安全白名单命令执行检查 |
| **workflow/client.py** | 495 | High-level API for CCS roles to interact with workflow DB |
| **reliability_core.py** | 465 | Core implementations of retry/circuit/heartbeat/TTL/metrics |
| **reliability.py** | 434 | Init retry, circuit breaker, heartbeat, TTL pruner from config |
| **routing/routes.py** | 612 | Dispatch logic: route_all, route_to_ccs, dispatch_investigator |
| **routing/router.py** | 361 | Route table: derive produce/consume from roles_export.json, priority routing |
| **routing/survival_monitor.py** | 640 | 存活监测 |
| **routing/rdb.py** | 293 | Route table SQLite persistence + audit log |
| **routing/auto.py** | 260 | Auto-consumption: poll bus, prioritize, dispatch with circuit breaker |
| **routing_daemon.py** | 591 | 路由守护进程 |
| **task_evidence.py** | 448 | 任务证据链 |
| **contract_updater.py** | 431 | 合约同步 |
| **conversation_monitor.py** | 417 | 对话监控 |
| **config_loader.py** | 167 | YAML config loading with env override and hot-reload |
| **workflow/gateway.py** | 188 | 工作流网关门禁 |
| **drift_detector.py** | 198 | 架构漂移检测 |
| **cron_scheduler.py** | 128 | cron 调度器 |
| **eval_consistency.py** | 70 | 评估一致性 |
| **routing/consistency.py** | 105 | 路由一致性 |
| **pipeflow/daemon.py** | 420 | Main loop: `while True: engine.run_once(); sleep(10)` |
| **workflow_metrics.py** | 109 | 工作流指标 |
| **workflow/db.py** | 96 | 工作流数据访问 |
| **output_validator.py** | 93 | 输出格式验证 |
| **routing/polling.py** | 88 | Message polling with cursor (dedup across restarts) |
| **paths.py** | 105 | 路径常量 |
| **workflow/sync.py** | 65 | 工作流同步 |

| **pipeflow/db.py** | 327 | Three-layer SQLite: Template → Instance → Task |
| **p0_exemption.py** | 496 | P0 豁免通道 |

**模块与行数随树变化：以 `find src -name "*.py" | wc -l` 与 `wc -l src/**/*.py` 现场取值为准（2026-10-22 实测 39 个模块 / 20234 行，原写 38 / ~10700 已过期）**

## Data flow

```
Bus new message (Blackboard)
    │
    ▼
polling.poll_unconsumed() ──→ router.get_consumers_prioritized()
    │                                │
    │  with cursor dedup              │  from roles_export.json
    ▼                                ▼
routes.route_all() ──→ auto.consume_with_linkage()
    │                                │
    │  subprocess ccs.py send        │  mark_consumed for all consumers
    ▼                                ▼
CCS (tmux + claude) ←── Sister Bus (ACK/linkage)
    │
    ▼
pipeflow.engine.run_once() ←── pipeflow.daemon.py (10s loop)
    │
    ├── _ensure_role_alive() ──→ subprocess ccs.py start
    ├── SQLite scan workflow_instances (pending/running)
    ├── _send_to_role() ──→ subprocess ccs.py send
    └── _check_anomalies() ──→ timeout escalation
```

## Key dependencies

- **roles_export.json** (`~/.hermes/data/roles_export.json`) -- route table source from shared_loader.py
- **Sister Bus** (`~/.hermes/scripts/bus_protocol.py`) -- Blackboard read/write
- **CCS CLI** (`session-launcher/src/ccs.py`) -- message delivery via subprocess
- **Sentinel files** (`~/.hermes/run/ccs-sentinels/`) -- CCS alive check
- **Workflow templates** (`~/.hermes/workflows/*.json`)
- **SQLite DBs** (`~/.hermes/state/workflows.db`, `routing.db`, `ack_tracker.db`) — `pipeline_cursor.db` 已随 cursor 机制一并退役（49a7b9d），别再引用
- **Bus 保留与轮询**：`prune()` 只删有 `consume` 记录的事实（未读消息不按年龄/容量清掉）；`unconsumed(limit=N)` 的 LIMIT 下推到 SQL（带 `role` 过滤时不下推，role 白名单仍在 Python 侧）

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

### bus 的 `search()` 只做全文匹配

`search()` 是 FTS5 全文检索：`role:`/`cat:`/`fid:` 这类限定符**不支持，也不报错**——
2026-10-22 实测三者全部返回 0 条。照着它们写的检查会静默退化成 vacuous pass
（`eval_checker.RequirementConfirmationChecker` 就这样假绿到当天才修）。要按字段筛，
用 `unconsumed(cat=...)` / `consumed_by(fid)` 或直接 SQL。
`facts.rc`（UPSERT 冲突时被清零）与 `hc`（旧 search 命中残留，实测单条 21,541）都不是消费判据，
唯一真相是 `log(typ='consume', fid, cat=消费者)`。

### Engine bypasses LifecycleManager

`engine.py` has 3 sites that bypass `lifecycle/manager.py` and write SQL directly:

- `_advance_production_wf()` -- updates step_results via raw conn.execute
- `_tick()` -- timeout detection writes directly via `_sync_step_results()`
- `_scan_tasks()` -- sub-workflow completion pushes parent workflow

These bypasses lose RLock protection and approval workflow.

### `/loop` injection is a no-op

`_ensure_role_alive()` injects `/loop` into the CCS, but `/loop` is NOT available in tmux environments. The CCS will not error, but it will also not enter a work loop -- it idles after processing each message.

（2026-10-22 复核：这条建议已失效——`engine.py` 里 `--drive` 出现 0 次，真实注入点是 `role_lifecycle.py:671/729`，注入值是 `infinite`。原文保留为历史记录。）

### Cross-project call boundary

`route_to_ccs()` calls `ccs.py send` via subprocess. There is no documented API contract or version check between pipeline → launcher. If launcher changes CLI flags, pipeline silently fails.

### Delivery prechecks

`role_lifecycle._send_to_role` checks CLI busy (`liveness.pane_cli_busy`: bottom-bar `esc to interrupt` / `retrying`) before `ccs send`; busy returns `"busy"` for the next tick instead of starting a 30s child process that blocks the whole tick.

## Tests

- Full suite: `python3 -m pytest tests/ -q` → 1218 passed (2026-10-22).
- Per-file counts are not maintained; run the suite for current numbers.
- `python3 scripts/doccheck.py` 校验本文件与 CLAUDE.md 声明的行数；无自动钩子，代码增删后跑一次 `--fix` 刷新。

## Red lines

- Do NOT create CCS directly -- lifecycle is launcher's responsibility
- Do NOT modify bus_protocol core logic
- Route mapping auto-generated from roles_export.json -- no hardcoding
- Check CCS alive before dispatching
- 审批确认（`confirmed_at`）优先于 `exit_messages` 文本判驳，判据收敛在 `review_gate.py`（见 `docs/ADR-APPROVAL-AUTHORITY.md`）
- Zero external dependencies (stdlib + hermes_bus only)
- No eval() -- use safe regex pattern matching
