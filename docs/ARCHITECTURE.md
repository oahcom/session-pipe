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

**Pipeline and launcher do NOT communicate directly.** Pipeline discovers CCS status via sentinel files (`/tmp/ccs-sentinels/`) and dispatches messages via `subprocess.run([sys.executable, CCS_CLI, "send", ...])`.

## Module inventory

| Module | Lines | Responsibility |
|--------|-------|----------------|
| **routing/router.py** | 365 | Route table: derive produce/consume from roles_export.json, priority routing |
| **routing/routes.py** | 284 | Dispatch logic: route_all, route_to_ccs, dispatch_investigator |
| **routing/auto.py** | 303 | Auto-consumption: poll bus, prioritize, dispatch with circuit breaker |
| **routing/polling.py** | 81 | Message polling with cursor (dedup across restarts) |
| **routing/rdb.py** | 285 | Route table SQLite persistence + audit log |
| **pipeflow/engine.py** | 847 | Data-driven workflow execution engine |
| **pipeflow/daemon.py** | 88 | Main loop: `while True: engine.run_once(); sleep(10)` |
| **pipeflow/db.py** | ~420 | Three-layer SQLite: Template → Instance → Task |
| **lifecycle/manager.py** | 995 | Workflow state machine (5 step types, approval/rollback/escalation) |
| **reliability.py** | ~100 | Init retry, circuit breaker, heartbeat, TTL pruner from config |
| **reliability_core.py** | ~200 | Core implementations of retry/circuit/heartbeat/TTL/metrics |
| **config_loader.py** | ~100 | YAML config loading with env override and hot-reload |
| **workflow/client.py** | ~420 | High-level API for CCS roles to interact with workflow DB |

**Total: ~4000 lines Python across ~13 modules**

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
- **Sentinel files** (`/tmp/ccs-sentinels/`) -- CCS alive check
- **Workflow templates** (`~/.hermes/workflows/*.json`)
- **SQLite DBs** (`~/.hermes/state/workflows.db`, `routing.db`, `pipeline_cursor.db`, `ack_tracker.db`)

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

`engine.py` has 3 sites that bypass `lifecycle/manager.py` and write SQL directly:

- `_advance_production_wf()` -- updates step_results via raw conn.execute
- `_tick()` -- timeout detection writes directly via `_sync_step_results()`
- `_scan_tasks()` -- sub-workflow completion pushes parent workflow

These bypasses lose RLock protection and approval workflow.

### `/loop` injection is a no-op

`_ensure_role_alive()` injects `/loop` into the CCS, but `/loop` is NOT available in tmux environments. The CCS will not error, but it will also not enter a work loop -- it idles after processing each message.

Fix per ARCHITECTURE.md: change `"--drive", "loop"` to `"--drive", "ondemand"` in `engine.py:582`.

### Cross-project call boundary

`route_to_ccs()` calls `ccs.py send` via subprocess. There is no documented API contract or version check between pipeline → launcher. If launcher changes CLI flags, pipeline silently fails.

## Tests

| Test file | Count | Pass |
|-----------|-------|------|
| test_workflow_db.py | 36 | 36/36 |
| test_workflow_engine.py | 31 | 31/31 |
| test_cross_component.py | 17 | 17/17 |
| test_router.py | 13 | 13/13 |
| test_integration.py | 12 | 12/12 |
| test_role_interaction.py | 10 | 10/10 |
| test_engine_e2e.py | 6 | 6/6 |
| test_workflow_daemon.py | 4 | 4/4 |
| test_router_extend_bh.py | 7 | 7/7 |
| **Total** | **136** | **136/136** |

## Red lines

- Do NOT create CCS directly -- lifecycle is launcher's responsibility
- Do NOT modify bus_protocol core logic
- Route mapping auto-generated from roles_export.json -- no hardcoding
- Check CCS alive before dispatching
- Zero external dependencies (stdlib + hermes_bus only)
- No eval() -- use safe regex pattern matching
