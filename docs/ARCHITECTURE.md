# session-pipeline（路由层）

Session 生态的**路由+可靠性层**——消息路由（router）、自动调度（auto_route）、工作流引擎（workflow_engine）、可靠性基础设施（reliability）。

## 在整体架构中的位置

```
角色 JSON 定义         执行引擎           路由+调度
(hermes-session-roles)  (session-launcher)  (session-pipeline)
       │                      │                     │
       ▼                      ▼                     ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────────┐
│ persona_*.json│───→│ ccs.py       │───→│ router.py         │
│ name/title   │    │ start/stop   │    │ auto_route.py     │
│ system_prompt │    │ feed_listener│    │ reliability.py    │
│ input_signals │    │ sentinel     │    │ workflow_engine.py│
│ output_targets│    │ tracker      │    │ workflow_db.py    │
└──────────────┘    └──────────────┘    │ workflow_daemon.py│
                                        └──────────────────┘
                                                │
                                                ▼
                                      ┌──────────────────┐
                                      │ Sister Bus        │
                                      │ Blackboard.db     │
                                      │ + Unix Socket     │
                                      └──────────────────┘
```

## 模块清单

| 模块 | 行数 | 职责 |
|------|------|------|
| **router.py** | 354 | 角色间消息路由：从角色 JSON 的 output_targets 自动推导 produce/consume 关系，优先级路由（security > code_fix > architecture > ...），消费联动 |
| **auto_route.py** | 599 | 自动感知 Blackboard 新消息并通知下游角色：优先级路由、重试、熔断、心跳、指标 |
| **workflow_db.py** | 418 | 三层 SQLite 存储：Workflow Template → Instance → Task。任务生命周期：created→assigned→in_progress→completed/failed/cancelled |
| **workflow_engine.py** | 363 | 工作流执行引擎：通过 Sister Bus 与 CCS 交互，支持 start/status/cancel/tick/advance，超时重试、条件步骤、workspace_summary、持久化 |
| **workflow_daemon.py** | 118 | 工作流 daemon：监控 workflow 表，通过 feed socket 推送 prompt 给运行中 CCS |
| **reliability.py** | 861 | 可靠性基础设施：指数退避重试、熔断器、心跳（5min stale）、消息 TTL 自动清理、结构化日志、Prometheus metrics |
| **config_loader.py** | 164 | 全局配置：YAML 配置加载，环境变量覆盖 |

**总计：7 模块，2877 行**

## 数据流

```
Bus 新消息
    │
    ▼
auto_route.py ──(读取角色 JSON output_targets)──→ router.py
    │                                                     │
    │  优先级路由 + 消费联动                                │ 执行可靠性
    ▼                                                     ▼
reliability.py ──(重试/熔断/心跳/metrics)──→ Workflow ←── workflow_db.py
                                                     │
                                                     ▼
                                            workflow_engine.py ──(Sister Bus)──→ CCS
                                                     │
                                                     ▼
                                            workflow_daemon.py ──(feed socket)──→ CCS prompt
```

## 关键依赖

- **Sister Bus API** (`~/.hermes/scripts/bus_protocol.py`) — Blackboard 读写
- **Sister Bus Socket** (`/tmp/sister_bus_*.sock`) — 实时推送
- **角色 JSON** (`~/hermes-session-roles/personas/session-roles/*.json`) — 路由规则来源
- **Workflow 模板** (`~/.hermes/workflows/*.json`) — 工作流定义
- **SQLite DB** (`~/.hermes/state/workflows.db`, `pipeline_cursor.db`)

## 测试

| 测试文件 | 数量 | 通过 |
|---------|------|------|
| test_router.py | 13 | 13/13 |
| test_integration.py | 12 | 12/12 |
| test_workflow_db.py | 36 | 36/36 |
| test_workflow_engine.py | 31 | 30/31 |
| test_workflow_daemon.py | 14 | 14/14 |
| test_cross_component.py | 17 | 17/17 |
| test_role_interaction.py | 10 | 10/10 |
| **总计** | **133** | **132/133** |

1 fail：`test_run_once_missing_workflow_def` — ValueError（workflow 定义缺失路径）
