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
| `get_wf(wf_id)` | str | `Optional[dict]` |
| `get_step(wf_id, step_id)` | str, str | `Optional[dict]` |
| `upsert_template(template_id, name, description, steps)` | str, str, str, list | `None` |
| `get_assigned_workflows(role)` | str | `list[dict]` |
| `close()` | — | `None` |

## Router (`routing/router.py`)

`get_consumers_prioritized(category)` 是 `Router` 实例方法（`router.py:186`），不是模块级函数；调用形如 `get_router().get_consumers_prioritized(cat)`。

| 函数 | 参数 | 返回 |
|------|------|------|
| `get_router()` | — | `Router` (singleton) |
| `role_produce_categories(role)` | str | `list[str]` |
| `role_consume_categories(role)` | str | `list[str]` |
| `get_consumers(category)` | str | `list[str]` |
| `get_producers(category)` | str | `list[str]` |
| `routing_summary()` | — | `dict` |
| `format_pipeline()` | — | `str` |
| `unconsumed_by_role(role, limit)` | str, int | `list[dict]` |

## Routing (`routing/routes.py`)

| 函数 | 参数 | 返回 |
|------|------|------|
| `route_all(consumer, dry_run, parallel)` | str, bool, bool | `dict` |
| `route_to_ccs(role_name, dry_run)` | str, bool | `dict` |
| `route_all_to_ccs(dry_run)` | bool | `dict` |

## AutoRouting (`routing/auto.py`)

| 函数 | 参数 | 返回 |
|------|------|------|
| `poll_unconsumed(category=None, consumer=None, limit=100)` | str?, str?, int | `list[dict]` |
| `consume_with_linkage(fact_id, category, consumer)` | int, str, str | `dict` |

## Reliability (`reliability.py`)

| 函数 | 参数 | 返回 |
|------|------|------|
| `health_check()` | — | `dict` |

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
