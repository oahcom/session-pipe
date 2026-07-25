# Session Pipeline — Technical Deep Dive

> 更新: 2026-07-24 | 受众: 高级开发者 | 相关: [DOCS.md](DOCS.md)

## 1. 项目定位

Session 生态的**路由层**。自动感知 bus 新消息，按优先级分发给对应 CCS 消费。无需人在中间传话，不靠 AI 自主决定该听谁的消息。

```
roles (定义层 JSON) → launcher (执行层) → pipeline (路由层)
                                              ↓
                                         Sister Bus
                                      (通信基础设施)
```

## 2. 架构概览

```
session-pipeline/
├── src/
│   ├── routing/                 # 路由系统
│   │   ├── router.py            # 路由映射（从角色 JSON 推导 produce/consume）
│   │   ├── auto.py              # 自动消息分发 + daemon 模式
│   │   ├── routes.py            # 路由分发逻辑（route_all, route_to_ccs）
│   │   ├── rdb.py               # SQLite 持久化路由表 + 审计日志
│   │   └── __init__.py
│   │
│   ├── pipeflow/                # 流水线引擎
│   │   ├── engine.py            # 对话工作流执行引擎（核心循环）
│   │   ├── db.py                # SQLite 持久化（Template/Instance/Task）
│   │   ├── daemon.py            # 工作流守护进程（run_once loop）
│   │   ├── dsl.py               # 工作流 DSL 解析
│   │   └── __init__.py
│   │
│   ├── lifecycle/               # 生命周期管理
│   │   ├── manager.py           # 状态机（完成/审批/回滚/升级）
│   │   └── __init__.py
│   │
│   ├── workflow/                # 工作流客户端
│   │   ├── client.py            # 高层 API（与 DB 交互）
│   │   ├── db.py                # 工作流 DB 辅助
│   │   ├── gateway.py           # 工作流网关
│   │   └── __init__.py
│   │
│   ├── reliability.py           # 可靠性基础设施实例化
│   ├── reliability_core.py      # 可靠性核心（重试/熔断/心跳/TTL/Metrics）
│   ├── config_loader.py         # YAML 配置加载 + 热重载
│   ├── paths.py                 # 统一路径管理
│   ├── template_registry.py     # 模板注册表
│   ├── wf.py                    # 工作流 CLI 工具
│   ├── workflow_client.py       # 工作流客户端兼容入口
│   ├── p0_exemption.py          # P0 豁免机制
│   └── __init__.py
│
├── config/config.yaml           # 所有可配置参数
├── docs/
│   ├── ARCHITECTURE.md
│   ├── SYSTEM_LANDSCAPE.md
│   ├── TEST_WORKFLOW.md
│   ├── WORKFLOW_TEMPLATE_SPEC.md
│   └── TECHNICAL_DEEP_DIVE.md   # 本文档
└── tests/
    ├── test_router.py
    ├── test_workflow_db.py
    ├── test_workflow_engine.py
    ├── test_cross_component.py
    ├── test_engine_e2e.py
    ├── test_integration.py
    ├── test_role_interaction.py
    ├── test_workflow_daemon.py
    └── test_router_extend_bh.py
```

## 3. 核心模块详解

### 3.1 routing/router.py — 路由表 (448 行)

**核心能力:** 从 hermes-session-roles 的角色 JSON 自动推导 produce/consume 关系。

```python
from routing.router import get_router
router = get_router()
router.role_produce_categories("maintainer")   # ["code_fix", "architecture"]
router.get_consumers("code_fix")               # ["engineer", "closer", ...]
router.get_consumers_prioritized("code_fix")   # 高优先级在前
```

**数据源:**
1. 优先从 SQLite DB 加载（runtime 持久化）
2. fallback 到角色 JSON 目录（`SESSION_ROLES_DIR`）
3. 扩展 Browser Harness 路由配置（`_bh_route_config.json`）

**消费联动:** `consume_linkage()` 返回其他受影响的消费者列表（供 caller 做 ACK 跟踪）。

### 3.2 routing/auto.py — 自动分发 (415 行)

**核心函数:**

```python
from routing.auto import poll_unconsumed, route_all, route_to_ccs, route_all_to_ccs
poll_unconsumed(category, consumer, limit=100)  # 拉取未消费消息
route_all(consumer, dry_run, parallel)           # 路由所有未消费消息
route_to_ccs(ccs_role)                           # 为特定角色路由
route_all_to_ccs()                               # 并行路由所有角色
```

**CLI:**

```bash
python3 src/routing/auto.py                          # 查看队列状态
python3 src/routing/auto.py --route-all              # 路由所有未消费
python3 src/routing/auto.py --route-all --dry-run    # 预览分配方案
python3 src/routing/auto.py --daemon                 # 守护模式（60s 轮询）
python3 src/routing/auto.py --health                 # 健康检查
python3 src/routing/auto.py --metrics                # Prometheus 指标
```

**优先级路由:**

| 分类 | 优先级 | 说明 |
|------|--------|------|
| security | 1（最高） | 安全告警 |
| code_fix | 2 | 代码修复 |
| architecture | 3 | 架构决策 |
| performance | 4 | 性能发现 |
| evolution_report | 5 | 进化轮次报告 |
| reflexion_lesson | 6 | 经验教训 |
| deception | 7 | 欺骗检测 |
| default | 11 | 其他 |

### 3.3 reliability + reliability_core — 可靠性基础设施 (483 + 456 行)

六层可靠性保障:

#### ① 指数退避重试
```yaml
retry:
  max_retries: 3
  base_delay: 0.5s
  max_delay: 10.0s
  exponential_base: 2.0
```

#### ② 熔断器
```yaml
circuit_breaker:
  failure_threshold: 5        # 连续 5 次失败 → 开路
  recovery_timeout: 30s       # 30s 后尝试半开
  half_open_max_calls: 1      # 半开时允许 1 次试探
```

#### ③ 消费者心跳
```yaml
heartbeat:
  stale_threshold: 300s       # 5 分钟无心跳 → stale
  cleanup_interval: 3600s     # 1 小时清理一次
```

#### ④ 消息 TTL
```yaml
ttl_pruner:
  max_age_days: 90            # 保留 90 天
  max_facts: 10000            # 最多 10000 条
  interval: 3600s             # 每小时清理
```

#### ⑤ 持久化 Cursor

SQLite 存储 `pipeline_cursor.db`，每个 `(consumer, category, instance_id)` 组合记录上次处理的 fact_id。防重启重复处理。

```sql
CREATE TABLE cursors (
  consumer TEXT, category TEXT, instance_id TEXT DEFAULT '',
  last_fact_id INT, updated_at REAL,
  PRIMARY KEY(consumer, category, instance_id)
);
```

#### ⑥ ACK 跟踪

`ack_tracker.db` 记录每条消息的消费确认状态。未 ACK 的消息自动重试。

### 3.4 pipeflow/engine.py — 工作流引擎 (871 行)

数据驱动的对话工作流执行引擎，通过 Sister Bus 与 CCS 会话交互。

**数据模型:**

```python
@dataclass
class WorkflowDef:
    name: str
    title: str
    description: str
    steps: list[Step]
    loop: dict | None       # 循环控制

@dataclass
class Step:
    id: str
    title: str
    target_role: str
    prompt_template: str
    exit_condition: dict
    type: str               # single/handoff/review/gate/notify
    max_retries: int
    verify: str             # shell command, exit 0 = pass
    completion_check: dict  # gate 步骤的检查条件
    condition: str          # 条件步骤表达式
    rollback_to: str        # 回滚目标步骤
    subflow_template: str   # 子工作流模板名

@dataclass
class WorkflowRun:
    id: str
    workflow_name: str
    context: dict
    status: str             # running / completed / failed / cancelled
    current_step: str
    step_retries: dict[str, int]
    step_results: dict[str, Any]
```

**32 个可用工作流模板 + 3 个复合链:**

- 原子工作流: `~/.hermes/workflows/*.json`
- 复合链: `~/.hermes/workflows/chains/*.json` (dev_pipeline, security_hotfix, recovery_pipeline)

### 3.5 lifecycle/manager.py — 生命周期状态机 (938 行)

5 种步骤类型的完成策略:

| type | 完成策略 | 需要审批 | 自动推进 |
|------|---------|---------|---------|
| `single` | 直接完成 | 否 | 是 |
| `handoff` | 等待分配者确认 | 是 (token) | 否 |
| `review` | 等待 target_role 确认 | 是 (token) | 否 |
| `gate` | 检查条件通过/阻塞 | 否 | 条件通过时 |
| `notify` | 直接完成 | 否 | 是 |

**并发安全:** `threading.RLock` + SQLite WAL + `BEGIN IMMEDIATE`

### 3.6 routing/rdb.py — 路由表数据库 (283 行)

SQLite 持久化路由表，支持:
- `save_routing(role, produce, consume, changed_by)` — 注册/更新
- `load_routing()` — 加载全部
- `audit_log(limit)` — 变更审计历史

### 3.8 config/config.yaml — 集中配置

| 配置块 | 功能 |
|--------|------|
| bus | bus 数据库路径、poll 间隔、单次最大返回 |
| retry | 重试策略配置 |
| circuit_breaker | 熔断器参数 |
| heartbeat | 消费者心跳参数 |
| ttl_pruner | TTL 清理参数 |
| priority | 分类优先级映射（security=1, default=8） |
| routing | 默认消费者、幂等消费、乐观锁 |
| logging | 日志级别、JSON 输出、trace_id |
| graceful_shutdown | 优雅关闭超时 |
| health | 健康判定阈值 |

## 4. 路由数据流

```
1. Sister Bus 写入新消息 (bus_client.py write)
2. Session Pipeline 轮询 (poll_unconsumed)
   └─ Router.role_consume_categories() 确定消费者
   └─ Router.get_consumers_prioritized() 按优先级排序
3. AutoRoute 分发 (route_to_ccs)
   └─ 写入 bus 通知目标角色
   └─ 或调用 session-launcher send 接口
4. 消费确认 (ACK Tracker)
5. Cursor 持久化 (pipeline_cursor.db)
```

## 5. 配置热重载

`config_loader.py` 支持运行时配置热重载:
- `get_config()` — 读取配置（缓存版）
- `reload_config()` — 强制重新读取
- `reconfigure()` — 重新初始化全部全局实例
- 检测 `config.yaml` 的 mtime 变化自动触发

**重要:** 配置变更通过 `reconfigure()` 重新创建全局单例（CIRCUIT_BREAKER, HEARTBEAT, TTL_PRUNER, DEFAULT_RETRY, GRACEFUL_SHUTDOWN），不中断正在处理的消息。

## 6. 信号纯度红线

| 指标 | 定义 | 监控方式 |
|------|------|---------|
| 信号噪声比 | 有效信号数量 / 总信号数量 | poll_unconsumed 分类分布 |
| 角色相关性 | 角色 consume 列表 vs 实际收到消息的匹配率 | 路由命中率统计 |
| 优先级时效 | 高优先级消息被消费的延迟 | 时间戳差值 |

**红线:** 角色消息池中 >30% 非专业范围噪声 → 需要优化路由配置

## 7. 与 Sister Bus 的集成

Bus 协议基于 `bus_protocol.Blackboard` 直接 API（不再 subprocess 解析字符串）:

```python
from bus_protocol import Blackboard
bb = Blackboard()
facts = bb.unconsumed()              # 拉取未消费消息
bb.read(cat="security", limit=10)    # 按分类读取
bb.write("code_fix", "消息", src="pipeline")  # 写入通知
```

依赖的 bus 路径:
- `~/.hermes/sister_bus/blackboard.db` — SQLite DB
- `~/.hermes/scripts/bus_protocol.py` — Python API
