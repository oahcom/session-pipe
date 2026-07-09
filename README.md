# Session Pipeline（消息路由层）

Session 生态的**路由层**——自动感知 bus 新消息，按优先级分发给对应 CCS 消费。

> ⚠️ **本项目不直接创建 CCS，只负责消息分发。CCS 生命周期由 `session-launcher` 管理。**

---

## 在整体架构中的位置

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      Hermes Session Ecosystem                          │
│                                                                         │
│  ┌───────────────────────┐                                             │
│  │  hermes-session-roles │                                             │
│  │  身份模板 JSON          │                                            │
│  └───────────┬───────────┘                                             │
│              │                                                          │
│              ▼                                                          │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │  session-launcher（执行层）                                   │      │
│  │  CCS 创建 / 生命周期 / 协作基础设施                           │      │
│  └──────────────────────────┬───────────────────────────────────┘      │
│                              │                                          │
│              ┌───────────────┴───────────────┐                         │
│              ▼                               ▼                         │
│  ┌──────────────────────────────────────┐   │                          │
│  │  session-pipeline  ← 本项目（路由层）│   │                          │
│  │                                       │   │                          │
│  │  ┌─────────────────────────────────┐ │   │                          │
│  │  │ Router                         │ │   │                          │
│  │  │ - 自动推导 produce/consume     │ │   │                          │
│  │  │ - 分类优先级排序              │ │   │                          │
│  │  │ - 消费者列表查询              │ │   │                          │
│  │  └─────────────────────────────────┘ │   │                          │
│  │                                       │   │                          │
│  │  ┌─────────────────────────────────┐ │   │                          │
│  │  │ AutoRoute                      │ │   │                          │
│  │  │ - poll_unconsumed()            │─┼───┘                          │
│  │  │ - notify_consumers()          │ │                                │
│  │  │ - route_all()                 │ │                                │
│  │  └─────────────────────────────────┘ │                                │
│  └──────────────────────────────────────┘                                │
│                                                                          │
│                       Sister Bus (SQLite)                               │
│                       消息传递 / FTS5 全文检索                            │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 核心能力

### 1. 路由映射（Router）

从 `hermes-session-roles` 的角色 JSON 自动推导：

```
角色 output_targets → produce 分类（该角色产出什么）
角色 input_signals  → consume 分类（该角色消费什么）
```

**示例：**

```python
from router import get_router

router = get_router()

# 查看 maintainer 产出什么
router.role_produce_categories("maintainer")  # ["code_fix", "architecture"]

# 查看谁消费 code_fix 分类
router.get_consumers("code_fix")  # ["consumer", "developer", "closer"]

# 按优先级排序的消费者
router.get_consumers_prioritized("code_fix")  # ["consumer", "developer", "closer"]
```

**分类优先级：**

| 分类 | 优先级 | 说明 |
|------|--------|------|
| security | 1 | 安全告警（最高） |
| code_fix | 2 | 代码修复 |
| architecture | 3 | 架构决策 |
| performance | 4 | 性能发现 |
| evolution_report | 5 | 进化轮次报告 |
| reflexion_lesson | 6 | 经验教训 |
| deception | 7 | 欺骗检测 |

### 2. 自动分发（AutoRoute）

```bash
# 查看未消费消息状态
python3 src/auto_route.py --status

# 手动路由所有未消费消息
python3 src/auto_route.py --route-all

# 只看分配方案，不实际发送
python3 src/auto_route.py --route-all --dry-run

# 为特定角色路由消息
python3 src/auto_route.py --route-to-ccs maintainer

# 并行路由所有角色
python3 src/auto_route.py --route-all-to-ccs
```

### 3. 可靠性基础设施（Reliability）

提供生产级保障：

| 功能 | 说明 |
|------|------|
| 指数退避重试 | 可配置 max_retries, base_delay, max_delay |
| 熔断器 | 连续失败 N 次 → 短路 M 秒 |
| 消费者心跳 | 每次 poll 更新 last_seen，>5min 标记 stale |
| 消息 TTL | 自动清理 >90 天未消费消息 |
| 结构化日志 | JSON 格式 + trace_id |
| Prometheus metrics | 延迟直方图、计数器 |

```bash
# 健康检查
python3 src/auto_route.py --health

# 查看 metrics
python3 src/auto_route.py --metrics

# 查看 ACK 统计
python3 src/auto_route.py --ack-stats

# 重试失败的 ACK
python3 src/auto_route.py --ack-retry
```

---

## 与 session-launcher 的协作

```
1. CCS 启动（session-launcher）
   → 写哨兵 /tmp/ccs-sentinels/<role>.json

2. Pipeline 轮询 bus（session-pipeline）
   → poll_unconsumed() 获取未消费消息
   → router.get_consumers() 查询消费者
   → 检查消费者是否在运行（session-launcher.is_ccs_running()）

3. 消息推送（session-pipeline）
   → 调用 session-launcher.send_to_ccs() 推送给运行中的 CCS
   → 未启动的 CCS → 发 bus 警告，由 daemon 负责启动
```

---

## 守护模式

```bash
# 启动后台路由服务
python3 src/auto_route.py --daemon

# 配置轮询间隔（默认 60 秒）
# 编辑 config/config.yaml → bus.poll_interval
```

**守护进程行为：**
- 每 60 秒 poll bus 未消费消息
- 按优先级分发给对应 CCS
- 自动处理重试、熔断、心跳
- 优雅关闭（SIGTERM/SIGINT）

---

## 项目结构

```
session-pipeline/
  src/
    router.py          → ROLE_ROUTING 映射 + unconsumed_by_role()
    auto_route.py      → 自动路由：新消息 → 通知对应角色
    reliability.py     → 重试/熔断/心跳/TTL/metrics
    config_loader.py   → config.yaml 配置加载
    workflow_db.py     → 三层架构数据库（Task/Workflow/Template）
    workflow_daemon.py → 守护进程（推送 prompt 给 CCS）
  config/
    config.yaml        → 配置文件
  tests/
    test_router.py     → 路由映射测试
    test_integration.py→ 集成测试
  docs/
    TEST_WORKFLOW.md   → 测试工作流文档
```

---

## 工作流系统

### 三层架构

```
Workflow Template（可复用模板）→ Workflow Instance（具体执行）→ Task（目标）
```

- **Task** = WHAT（目标）
- **Workflow Instance** = HOW（流程步骤）
- **Workflow Template** = 可复用的流程定义

### 文件

| 文件 | 说明 |
|------|------|
| `src/workflow_db.py` | 三层架构数据库（SQLite） |
| `src/workflow_daemon.py` | 守护进程（推送 prompt 给 CCS） |
| `docs/TEST_WORKFLOW.md` | 测试工作流文档 |

### 状态流转

```
Task: created → assigned → in_progress → completed/failed/cancelled
Workflow: created → pending → running → completed/failed/cancelled
Step: pending → running → completed/failed
```

详见 `docs/TEST_WORKFLOW.md`

---

## 红线

1. **不直接创建 CCS**，CCS 生命周期由 `session-launcher` 管理
2. **不修改 bus_protocol.py 核心逻辑**，只新增路由层
3. **路由映射从角色 JSON 自动生成**，不硬编码（除回退默认值）
4. **消息推送前必须检查 CCS 是否在运行**，未运行则发警告而非尝试启动
5. 零外部依赖（stdlib + 已有 bus_protocol）
6. 每个修改必须通过测试：`python3 -m pytest tests/`
