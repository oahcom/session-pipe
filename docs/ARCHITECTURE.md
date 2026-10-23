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

**Pipeline 不 import launcher**：只用 `subprocess.run([sys.executable, CCS_CLI, <子命令>, ...])` 调它（send / start / stop / status-role），角色状态读哨兵文件 `~/.hermes/run/ccs-sentinels/`。

## Module inventory

模块清单与行数只在 [CLAUDE.md](../CLAUDE.md) 维护（`scripts/doccheck.py` 校验行数），本文件不复制一份：
一份清单两处维护，必然一处先烂（2026-10-23 删掉本表时已有 33 行、其中"主要模块/不是全集"的边界要额外解释）。
需要「哪个文件干什么」就读 CLAUDE.md 的目录树；需要「数据怎么流」读下面的 Data flow。

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
_ROLES_EXPORT_PATH = HERMES_STATE.parent / "data" / "roles_export.json"

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

### 已知缺陷：claim 早于投递（2026-10-22 取证，bus #442309）

`route_all` 对系统类（`architecture`/`code_review`/`code_fix`/`notice`/`blocker` 等 `_SYSTEM_CATEGORIES`）只写 per-role `consume` 记录、不投递；
而这条 consume 正是 `route_to_ccs` 的可见性判据（`poll_unconsumed`）→ 消息永久不进 pane。实测 #440553 写入后 **4 秒**被 claim，
devops 的 4 条派单就此消失；另一条（`@devops` + `code_review`）死在 @tag 与分类订阅求交。系统类也不 mint workflow，
而唯一会 `ccs start` 死 pane 的是 workflow 引擎 → pane 死后没有人拉起（devops 10.5 小时无 pane）。

兜底全盲：`_alert_aged_queue` 只捞「未 consume 且 >24h」；`route_to_ccs` 的 skipped 只进 `details`、journal 无痕；`STALE_AFTER_S=6h` 到点静默 claim。
修法方向（未实施）：consume 前移到投递成功处 + 未运行角色落一条 warning + 过期 claim 进告警。

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
