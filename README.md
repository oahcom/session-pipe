# Session Pipeline（消息路由层）

## 定位

Session 生态的**路由层**——自动感知 bus 新消息，按优先级分发给对应 CCS 消费。

**核心职责：自动将消息按优先级路由给正确的消费者——不靠人在中间传话，不靠 AI 自主决定该听谁的消息。**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      Hermes Session Ecosystem                          │
│                                                                         │
│  ┌───────────────────────┐                                             │
│  │  hermes-session-roles │  → 角色 JSON 定义（25 角色）                │
│  └───────────┬───────────┘                                             │
│              │ 角色定义 （produce/consume 分类）                        │
│              ▼                                                          │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │  session-pipeline  ← 本项目（路由层）                         │      │
│  │                                                               │      │
│  │  Router: 自动推导 produce/consume + 优先级排序                │      │
│  │  AutoRoute: 自动分发 + daemon 模式                            │      │
│  │  Reliability: 重试/熔断/心跳/TTL/Metrics                     │      │
│  │  Workflow: 25 工作流 + 3 复合流水线                          │      │
│  └───────────────────────┬──────────────────────────────────────┘      │
│                          │                                              │
│              ┌───────────┴───────────┐                                  │
│              ▼                       ▼                                  │
│  ┌──────────────────┐    ┌──────────────────────┐                       │
│  │ Sister Bus       │    │ session-launcher     │                       │
│  │ SQLite + Socket  │    │ 执行层（CCS 管理）    │                       │
│  └──────────────────┘    └──────────────────────┘                       │
└─────────────────────────────────────────────────────────────────────────┘
```

**三个项目加起来 = 一个多 agent 协作操作系统：**

| 项目 | 类比 | 职责 |
|------|------|------|
| hermes-session-roles | /etc/passwd | 用户定义 |
| session-launcher | systemd | 进程管理、上下文隔离 |
| session-pipeline | 消息队列 + 路由表 | 通信基础设施 |

---

## 信号质量（本项目的隐藏职责）

表面职责是"消息路由到哪里"，隐藏职责是"路由过去的信号质量是否足够让角色发挥专业水平"。

| 指标 | 定义 | 监控方式 |
|------|------|---------|
| 信号噪声比 | 有效信号数量 / 总信号数量 | poll_unconsumed 分类分布 |
| 角色相关性 | 角色 consume 列表 vs 实际收到消息的匹配率 | 路由命中率统计 |
| 优先级时效 | 高优先级消息被消费的延迟 | 时间戳差值 |

### 信号纯度红线

- 角色消息池中 >30% 非专业范围噪声 → 路由配置需优化
- 角色 consume 列表含 `*`（全量消费）必须是刻意决策，不能是懒配置

---

## 项目结构

```
src/
  # ── 路由 ──
  ├── router.py             路由映射（从角色 JSON 自动推导 produce/consume）
  ├── routing_db.py         SQLite 持久化路由表 + 审计日志
  ├── auto_route.py         自动消息分发 + daemon 模式 + CLI
  ├── auto_route_routing.py 路由分发逻辑（从 auto_route 提取）
  │
  # ── 可靠性 ──
  ├── reliability.py        完整可靠性层（重试/熔断/心跳/TTL/Metrics/ACK）
  ├── reliability_core.py   可靠性核心基础设施
  │
  # ── 配置 ──
  ├── config_loader.py      YAML 配置加载 + 默认值回退 + 自动写入
  │
  # ── 工作流 ──
  ├── workflow_engine.py    数据驱动的工作流执行引擎（25 工作流）
  ├── workflow_daemon.py    工作流守护进程（推送 prompt 给 CCS）
  ├── workflow_db.py        三层架构数据库（Template/Instance/Task）
  ├── composite_models.py   复合工作流数据模型
  ├── composite_runner.py   复合工作流引擎（subflow/parallel/choice）
  │
  # ── 基础设施 ──
  ├── paths.py              统一路径管理
config/
  config.yaml               所有可配置参数
tests/
  test_router.py            路由逻辑单元测试
  test_workflow_db.py       工作流数据库测试
  test_workflow_engine.py   工作流引擎测试
  test_integration.py       端到端集成测试
  test_cross_component.py   跨组件测试
  test_role_interaction.py  角色交互测试
  test_workflow_daemon.py   工作流守护进程测试
  run.py                    集成运行入口
docs/
  TEST_WORKFLOW.md          测试工作流文档
```

---

## 核心能力```

---

## 核心能力

### 1. 路由映射（Router）

从 `hermes-session-roles` 的角色 JSON 自动推导 produce/consume 关系：

```python
from router import get_router

router = get_router()

# 查看 maintainer 产出什么
router.role_produce_categories("maintainer")  # ["code_fix", "architecture"]

# 查看谁消费 code_fix
router.get_consumers("code_fix")              # ["consumer", "engineer", "closer", ...]

# 按优先级排序的消费者
router.get_consumers_prioritized("code_fix")  # 高优先级在前
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
| (默认) | 11 | 其他分类 |

### 2. 自动分发（AutoRoute）

```bash
# 查看未消费消息状态
python3 src/auto_route.py

# 手动路由所有未消费消息
python3 src/auto_route.py --route-all

# 只看分配方案，不实际发送
python3 src/auto_route.py --route-all --dry-run

# 为特定角色路由消息
python3 src/auto_route.py --route-to-ccs maintainer

# 并行路由所有角色
python3 src/auto_route.py --route-all-to-ccs

# 守护模式（每 60 秒轮询）
python3 src/auto_route.py --daemon
```

### 3. 可靠性基础设施

| 功能 | 说明 | 配置位置 |
|------|------|----------|
| 指数退避重试 | 可配置 max_retries, base_delay, max_delay | config.yaml retry.* |
| 熔断器 | 连续失败 N 次 → 短路 M 秒 | config.yaml circuit_breaker.* |
| 消费者心跳 | 每次 poll 更新 last_seen，>5min 标记 stale | config.yaml heartbeat.* |
| 消息 TTL | 自动清理 >90 天未消费消息 | config.yaml ttl_pruner.* |
| 持久化 Cursor | 防重启重复处理 | ~/.hermes/state/pipeline_cursor.db |
| ACK 跟踪 | 消费确认 + 失败重试 | ack_tracker.db |
| 结构化日志 | JSON 格式 + trace_id | config.yaml logging.* |
| Prometheus Metrics | 延迟直方图、计数器 | /metrics 端点 |

```bash
python3 src/auto_route.py --health       # 健康检查
python3 src/auto_route.py --metrics      # Prometheus 指标
python3 src/auto_route.py --ack-stats    # ACK 统计
python3 src/auto_route.py --ack-retry    # 重试失败 ACK
```

### 4. 工作流系统

三层架构：**Workflow Template（可复用模板）→ Workflow Instance（具体执行）→ Task（目标）**

```
Task: created → assigned → in_progress → completed/failed/cancelled
Workflow: created → pending → running → completed/failed/cancelled
Step: pending → running → completed/failed
```

**25 个可用工作流：**

architect_adr, architect_full_design, architect_review, closer_close_loop, coordinator_dispatch, coordinator_schedule, coordinator_standup, design_review, dev_implement, devops_deploy_execute, devops_deploy_plan, engineer_feature, engineer_implementation, investigator_analyze, lr_tech_decision, maintainer_health_monitor, pg_implement, pm_requirements, qa_test_execute, qa_test_plan, reviewer_pr_review, scout_research_cycle, security_audit_scan, security_threat_model, writer_document

**复合工作流链（3 个）：**

| 链名 | 步骤 | 说明 |
|------|------|------|
| dev_pipeline | architect → engineer → reviewer → qa → devops | 完整开发交付流水线 |
| security_hotfix | security_auditor → engineer → reviewer → devops | 安全热修复 |
| recovery_pipeline | maintainer → engineer → coordinator | 故障恢复 |

```bash
# CLI
python3 src/workflow_engine.py list               # 列出工作流
python3 src/workflow_engine.py start <name>        # 启动工作流
python3 src/workflow_engine.py status <wf_id>      # 查看状态
python3 src/workflow_engine.py cancel <wf_id>      # 取消
python3 src/workflow_engine.py daemon --interval 10 # 守护轮询
```

### 5. 路由表持久化

路由表从角色 JSON 自动推导，也可手动管理：

```bash
python3 src/routing_db.py load               # 查看路由表
python3 src/routing_db.py save <role> [...]  # 手动注册
python3 src/routing_db.py delete <role>      # 删除
python3 src/routing_db.py audit              # 变更日志
```

---

## 工作流引擎安全性

`workflow_engine.py` 中的条件检查**不使用 eval()**，改用安全的 regex 模式匹配：

```python
# 安全：仅匹配 s<数字>.status == '<状态>' 模式
m = re.search(r"s(\d+)\.status\s*==\s*'([^']+)'", expr)
```

支持格式：`s1.status=='completed'`，不支持任意表达式求值。

---

## 配置

默认配置在 `config/config.yaml`，文件不存在时自动写入。

```yaml
bus:
  db_path: ~/.hermes/sister_bus/blackboard.db
  poll_interval: 60
  max_messages_per_poll: 100

retry:
  max_retries: 3
  base_delay: 0.5
  max_delay: 10.0

circuit_breaker:
  failure_threshold: 5
  recovery_timeout: 30.0

heartbeat:
  stale_threshold: 300
  cleanup_interval: 3600
```

环境变量覆盖：`SESSION_PIPELINE_CONFIG`（配置文件路径）、`SESSION_PIPELINE_STATE_DIR`（状态目录）、`ROUTING_DB_PATH`（路由表路径）

---

## 守护进程注册

pipeline daemon 可注册为 systemd 服务（WSL2 可能不可用）：

```bash
# service 文件位置（手动安装）
cp ~/.config/systemd/user/session-pipeline-daemon.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable session-pipeline-daemon.service
systemctl --user start session-pipeline-daemon.service

# 当 systemd 不可用时的 fallback
bash ~/session-launcher/scripts/start_daemons.sh
```

workflow-engine 同：

```bash
systemctl --user enable workflow-engine.service
systemctl --user start workflow-engine.service
```

---

## 协作红线

1. **不直接创建 CCS** — CCS 生命周期由 session-launcher 管理
2. **不修改 bus_protocol 核心逻辑** — 只新增路由层，不改底层消息存储
3. **路由映射从角色 JSON 自动生成** — 不硬编码（除回退默认值）
4. **消息推送前必须检查 CCS 是否在运行** — 未运行则发警告而非启动
5. **零外部依赖**（stdlib + 已有 bus_protocol）
6. **每个修改必须通过测试**

---

## 测试

```bash
# 路由单元测试
python3 -m pytest tests/test_router.py -v

# 工作流数据库测试
python3 -m pytest tests/test_workflow_db.py -v

# 验证导入
PYTHONPATH=src python3 -c "from router import get_router; from reliability import health_check; from workflow_engine import WorkflowEngine; print('All imports OK')"
```

---

## 依赖项目

| 项目 | 关系 | 说明 |
|------|------|------|
| hermes-session-roles | 上游定义 | 角色 produce/consume 分类自动推导 |
| session-launcher | 下游执行 | CCS 状态查询 + 消息发送 |
| Sister Bus | 基础设施 | SQLite blackboard 消息存储 |
| 9Router | 推理引擎 | HTTP API (localhost:20128) |

