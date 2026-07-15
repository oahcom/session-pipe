# PUBLIC_API — session-pipeline

## Public API

> **位置**: `~/session-pipeline/src/`

## Direction

**session-pipeline** → session-launcher → hermes-session-roles

任何反向依赖标记为 debt。

## Dependencies

## WorkflowEngine (`workflow_engine.py`)

| 函数 | 参数 | 返回 |
|------|------|------|
| `create_workflow(name, template_id)` | str, str | `dict` |
| `get_workflow(wf_id)` | str | `Optional[dict]` |
| `update_workflow(wf_id, data)` | str, dict | `bool` |
| `list_workflows(status=None)` | str? | `list[dict]` |

## WorkflowDatabase (`workflow_db.py`)

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_task(task_id)` | str | `Optional[dict]` |
| `update_task(task_id, data)` | str, dict | `bool` |
| `get_template(template_id)` | str | `Optional[dict]` |
| `query_tasks(filter_dict)` | dict | `list[dict]` |

## Reliability / ReliabilityCore (`reliability.py`, `reliability_core.py`)

内部使用，不对外暴露。核心函数：

| 函数 | 说明 |
|------|------|
| `retry_with_backoff(callable, max_retries=3)` | 指数退避重试 |
| `circuit_breaker(name, threshold=5)` | 熔断器 |

## AutoRouter / AutoRouteRouting (`auto_route.py`, `auto_route_routing.py`)

路由消息到对应角色，内部使用。

## CompositeRunner (`composite_runner.py`)

> 消费 `workflow_client.WorkflowClient` (session-launcher)

| 方法 | 参数 | 返回 |
|------|------|------|
| `list_templates()` | — | `list[str]` |
| `run_composite(template_name, args)` | str, dict | `dict` |

## PipelineDSL (`dsl.py`)

内部使用。解析 DSL 定义的工作流模板。

## ConfigLoader (`config_loader.py`)

内部使用。加载 pipeline 配置。

---

## 违反单向依赖的 import (debt)

这些是 `session-pipeline` 反向引用 `session-launcher` 的地方：

| 文件 | 引用 | 说明 |
|------|------|------|
| `composite_runner.py:27` | `_LAUNCHER_SRC = ~/session-launcher/src` | sys.path 插入 |
| `auto_route.py:29,58-61` | `sys.path.insert(0, launcher_src)` | 运行时路径注入 |
| `auto_route_routing.py:171-175` | `_LAUNCHER_SRC` → `from launcher import send_to_ccs` | sys.path 注入 |
| `pipeflow/composite.py:231` | `from workflow_client import WorkflowClient` | 运行时导入 |
