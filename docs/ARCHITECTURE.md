# session-pipeline（路由层）

> 更新: 2026-07-24 | 受众: 架构师 | 相关: [DOCS.md](DOCS.md)

Session 生态的**路由+可靠性层**——消息路由（router）、自动调度（auto_route）、工作流引擎（workflow_engine）、可靠性基础设施（reliability）。

## 在整体架构中的位置

```
角色 JSON 定义         执行引擎           路由+调度
(hermes-session-roles)  (session-launcher)  (session-pipeline)
       │                      │                     │
       ▼                      ▼                     ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐
│ persona_*.json│───→│ ccs.py       │───→│ routing/router.py    │
│ name/title   │    │ start/stop   │    │ routing/auto.py      │
│ system_prompt │    │ feed_listener│    │ pipeflow/engine.py   │
│ input_signals │    │ sentinel     │    │ pipeflow/db.py       │
│ output_targets│    │ tracker      │    │ lifecycle/manager.py │
└──────────────┘    └──────────────┘    │ reliability.py       │
                                        └──────────────────────┘
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
| **routing/router.py** | 448 | 角色间消息路由：从角色 JSON 的 output_targets 自动推导 produce/consume 关系，优先级路由（security > code_fix > architecture > ...），消费联动 |
| **routing/auto.py** | 415 | 自动感知 Blackboard 新消息并通知下游角色：优先级路由、重试、熔断、心跳、指标 |
| **routing/routes.py** | 342 | 路由分发逻辑：route_all、route_to_ccs、dispatch_investigator |
| **routing/rdb.py** | 283 | SQLite 持久化路由表 + 审计日志 |
| **pipeflow/db.py** | 418 | 三层 SQLite 存储：Workflow Template → Instance → Task。任务生命周期：created→assigned→in_progress→completed/failed/cancelled |
| **pipeflow/engine.py** | 871 | 工作流执行引擎：通过 Sister Bus 与 CCS 交互，支持 start/status/cancel/tick/advance，超时重试、条件步骤、子工作流嵌套、持久化 |
| **pipeflow/daemon.py** | 92 | 工作流 daemon：驱动 engine.run_once 推进所有工作流 |
| **pipeflow/dsl.py** | 89 | YAML 声明式工作流 DSL 解析 |
| **lifecycle/manager.py** | 938 | 生命周期状态机：步骤完成/审批/回滚/升级/重分配，并发安全（RLock + WAL + BEGIN IMMEDIATE） |
| **reliability.py** | 483 | 可靠性基础设施：指数退避重试、熔断器、心跳（5min stale）、消息 TTL 自动清理、结构化日志、Prometheus metrics |
| **reliability_core.py** | 456 | 可靠性核心（重试/熔断/心跳/TTL/Metrics 的具体实现） |
| **config_loader.py** | 185 | 全局配置：YAML 配置加载，环境变量覆盖，热重载 |
| **workflow/client.py** | 420 | 工作流客户端：与 workflow DB 交互的高层 API |

**总计：13 模块，5733 行**

## 数据流

```
Bus 新消息
    │
    ▼
routing/auto.py ──(读取角色 JSON output_targets)──→ routing/router.py
    │                                                        │
    │  优先级路由 + 消费联动                                   │ 执行可靠性
    ▼                                                        ▼
reliability.py ──(重试/熔断/心跳/metrics)──→ pipeflow/engine.py ←── pipeflow/db.py
                                                     │
                                                     ▼
                                            lifecycle/manager.py ──(Sister Bus)──→ CCS
                                                     │
                                                     ▼
                                            pipeflow/daemon.py ──(run_once loop)──→ CCS prompt
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
| test_workflow_db.py | 36 | 36/36 |
| test_workflow_engine.py | 31 | 31/31 |
| test_cross_component.py | 17 | 17/17 |
| test_router.py | 13 | 13/13 |
| test_integration.py | 12 | 12/12 |
| test_role_interaction.py | 10 | 10/10 |
| test_engine_e2e.py | 6 | 6/6 |
| test_workflow_daemon.py | 4 | 4/4 |
| test_router_extend_bh.py | 7 | 7/7 |
| **总计** | **137** | **137/137** |
