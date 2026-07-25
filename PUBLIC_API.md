# PUBLIC_API — session-pipeline

> 更新: 2026-07-24 | 受众: API 消费者 | 相关: [docs/DOCS.md](docs/DOCS.md)

## Direction

**session-pipeline** → session-launcher → hermes-session-roles

任何反向依赖标记为 debt。

## WorkflowEngine (`pipeflow/engine.py`)

| 函数 | 参数 | 返回 |
|------|------|------|
| `start(name, context)` | str, dict | `str` (wf_id) |
| `status(wid)` | str | `dict` |
| `cancel(wid)` | str | `bool` |
| `run_once()` | — | `None` |
| `list_workflows()` | — | `list[str]` |
| `tick()` | — | `int` |

## LifecycleManager (`lifecycle/manager.py`)

| 方法 | 参数 | 返回 |
|------|------|------|
| `start_wf(wf_id, current_step_id, template_id)` | str, str, str | `bool` |
| `complete_step(wf_id, step_id)` | str, str | `str` (result status) |
| `confirm_step(wf_id, step_id, token, approved, reason)` | str, str, str, bool, str | `bool` |
| `fail_step(wf_id, step_id, reason, allow_retry)` | str, str, str, bool | `bool` |
| `rollback_step(wf_id, step_id)` | str, str | `bool` |
| `escalate_step(wf_id, step_id, reason)` | str, str, str | `dict` |
| `reassign_step(wf_id, step_id, new_role)` | str, str, str | `bool` |
| `get_wf(wf_id)` | str | `Optional[dict]` |
| `get_step(wf_id, step_id)` | str, str | `Optional[dict]` |
| `upsert_template(template_id, name, description, steps)` | str, str, str, list | `None` |
| `get_run(wf_id)` | str | `Optional[dict]` |
| `get_assigned_workflows(role)` | str | `list[dict]` |
| `get_workflow_context(wf_id)` | str | `dict` |
| `get_workflow_progress(wf_id)` | str | `dict` |
| `check_gate_timeouts()` | — | `list[dict]` |
| `acknowledge_handoff(wf_id, step_id, role)` | str, str, str | `bool` |
| `close(wf_id)` | str | `None` |

## WorkflowDB (`pipeflow/db.py`)

| 方法 | 参数 | 返回 |
|------|------|------|
| `create_task(title, description, assigner, assignee, priority, tags, context)` | str, str, str, str, int, list, dict | `str` (task_id) |
| `get_task(task_id)` | str | `Optional[dict]` |
| `update_task(task_id, **kwargs)` | str | `bool` |
| `delete_task(task_id, actor)` | str, str | `bool` |
| `list_tasks(status, assigner, assignee, limit)` | str, str, str, int | `list[dict]` |
| `create_template(name, description, steps_json, steps_mermaid)` | str, str, dict, str | `str` (template_id) |
| `get_template(template_id)` | str | `Optional[dict]` |
| `find_template(name)` | str | `Optional[dict]` |
| `create_workflow(task_id, template_name, assigner, assignee, context)` | str, str, str, str, dict | `Optional[str]` |
| `get_workflow(instance_id)` | str | `Optional[dict]` |
| `update_workflow(instance_id, **kwargs)` | str | `bool` |
| `chain_workflows(task_id, template_names, assigner, assignee)` | str, list, str, str | `list[str]` |

## Router (`routing/router.py`)

| 函数 | 参数 | 返回 |
|------|------|------|
| `get_router()` | — | `Router` (singleton) |
| `role_produce_categories(role)` | str | `list[str]` |
| `role_consume_categories(role)` | str | `list[str]` |
| `get_consumers(category)` | str | `list[str]` |
| `get_consumers_prioritized(category)` | str | `list[str]` |
| `get_producers(category)` | str | `list[str]` |
| `routing_summary()` | — | `dict` |
| `format_pipeline()` | — | `str` |
| `unconsumed_by_role(role, limit)` | str, int | `list[dict]` |

## Reliability (`reliability.py`)

| 函数 | 参数 | 返回 |
|------|------|------|
| `poll_unconsumed(category, consumer, instance_id, limit)` | str, str, str, int | `list[dict]` |
| `route_all(consumer, dry_run, parallel, instance_id)` | str, bool, bool, str | `dict` |
| `route_to_ccs(role_name, dry_run)` | str, bool | `dict` |
| `route_all_to_ccs(dry_run)` | bool | `dict` |
| `consume_with_linkage(fact_id, category, consumer)` | int, str, str | `dict` |
| `dispatch_investigator(category, dry_run)` | str, bool | `dict` |
| `status()` | — | `dict` |

## Reliability (`reliability.py`)

内部使用，不对外暴露。核心全局单例：

| 单例 | 说明 |
|------|------|
| `CIRCUIT_BREAKER` | 熔断器 |
| `HEARTBEAT` | 消费者心跳 |
| `TTL_PRUNER` | 消息 TTL 清理 |
| `DEFAULT_RETRY` | 重试策略 |
| `GRACEFUL_SHUTDOWN` | 优雅关闭 |
| `METRICS` | Prometheus 指标 |
| `IDEMPOTENT_CONSUME` | 幂等消费 |
| `ACK_TRACKER` | ACK 跟踪 |

## 违反单向依赖的 import (debt)

| 文件 | 引用 | 说明 |
|------|------|------|
| `routing/auto.py` | `from routing import router` → sentinel | 延迟导入 |
