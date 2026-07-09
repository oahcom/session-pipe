# Session Pipeline 项目

## 项目概述

Session 生态的**路由层**——自动感知 bus 新消息，按优先级分发给对应 CCS 消费。
依赖 hermes-session-roles（路由表自动生成）和 session-launcher（CCS 通信）。

---

## 整体架构中的位置

```
hermes-session-roles (定义层) → JSON → Router 自动路由表
        │
        ▼
session-launcher (执行层)
  - 创建 CCS（tmux + claude 进程）
  - 生命周期管理（watchdog/turn_tracker/sentinel）
  - 提供 send_to_ccs() API
        │
        ▼
session-pipeline (路由层) ← 本项目
  - Router: 从角色 JSON 推导 produce/consume 关系
  - AutoRoute: 轮询 bus → 按优先级分发 → 消费联动
  - Reliability: 重试/熔断/心跳/TTL/metrics/ACK
  - Daemon: 持续轮询（--daemon）
```

**铁律：修改本项目时必须同时考虑上下游影响。**

---

## 项目结构

```
src/
  router.py          → 路由表自动生成 + 优先级排序 + 消费联动
  auto_route.py      → 自动路由入口 + daemon 模式 + CLI
  reliability.py     → 重试/熔断/心跳/TTL/metrics/ACK
  config_loader.py   → YAML 配置加载 + 默认值回退
config/
  config.yaml        → 所有可配置参数
tests/
  test_router.py     → 路由逻辑单元测试（13 个）
  test_integration.py→ 端到端集成测试（12 个）
```

---

## Git 工作流

1. **禁止切换分支**，始终在 main 分支工作
2. 小步提交，每完成一个逻辑单元立即 commit
3. 出错用新提交修复，不要 revert
4. 本地即生产环境

---

## 关键决策

| 决策 | 选择 | 理由 |
|------|------|------|
| bus 访问 | 直接 Blackboard API | 避免 subprocess 字符串解析，直接内嵌 Python 访问 |
| 路由表源 | 自动从角色 JSON 推导 | 角色定义变更时路由表自动更新 |
| 回退策略 | 硬编码默认路由表 | 角色 JSON 目录不可读时降级 |
| 消费联动 | 联动标记已消费 | 避免多角色重复处理同一消息 |
| 重试 | 指数退避 + 熔断器 | 防止网络抖动导致级联故障 |
| metrics | 进程内 Prometheus 格式 | 零外部依赖，直接挂到监控端点 |

---

## 协作红线

1. **不直接创建 CCS** —— CCS 生命周期由 session-launcher 管理，pipeline 只做消息分发
2. **不修改 bus_protocol 核心逻辑** —— 只新增路由层，不改底层消息存储
3. **所有可靠功能必须配置化** —— 重试/熔断/心跳/TTL 全部可调
4. **优先 stdlib** —— 除了 `pyyaml` 零外部依赖

---

## 已知缺陷（全部已修复 2026-07-08~09）

| 问题 | 优先级 | 修复方式 |
|------|--------|----------|
| `sys.path.insert` 硬编码绝对路径 | ✅ CRITICAL | 环境变量覆盖 + `sys.path.insert(0, _SRC_DIR)` 确保 src/ 优先 |
| config.yaml 定义了配置但代码从不读 | ✅ HIGH | 接入 `logging.*`、`health.*`、`bus.max_messages_per_poll`、`heartbeat.cleanup_interval`、`ttl_pruner.auto_start` |
| daemon 模式无 PID 文件/无单实例锁 | ✅ CRITICAL | `/tmp/session-pipeline-daemon.pid` + `os.kill(pid, 0)` 存活检查 |
| AckTracker 进程内存存储，重启丢失 | ✅ HIGH | SQLite 持久化（`~/.hermes/state/ack_tracker.db`） |
| RetryPolicy 期望 Exception tuple，config 传字符串 | ✅ CRITICAL | `__post_init__` + `config_loader._resolve_exceptions()` 字符串→类映射 |
| 无持久化 offset/cursor | ✅ HIGH | SQLite cursor 表（`~/.hermes/state/pipeline_cursor.db`），`poll_unconsumed` 按 cursor 过滤已处理消息 |
| Codex 审查：cursor 导入但未调用（死代码） | ✅ P1 | `poll_unconsumed(consumer=)` 接受 consumer 参数过滤已处理消息 |
| Codex 审查：route_all cursor max_id 不精确 | ✅ P1 | 改用 `max(d.get("id") for d in assignments)` 取全量最大 ID |
| Codex 审查：SQLite 未启用 WAL 模式 | ✅ P2 | AckTracker._init_db() + cursor_db 初始化时执行 `PRAGMA journal_mode=WAL` |
| Codex 审查：status() 熔断器 open 时返回 idle | ✅ P2 | status() 返回 `{"status": "error", "error": ...}` 区分错误与空闲 |

---

## 工作流系统

### 三层架构

```
Workflow Template（可复用模板）
  ↓ 实例化
Workflow Instance（具体执行）
  ↓ 关联
Task（目标）
```

### 文件

| 文件 | 说明 |
|------|------|
| `src/workflow_db.py` | 三层架构数据库（SQLite） |
| `src/workflow_daemon.py` | 守护进程（推送 prompt 给 CCS） |
| `docs/TEST_WORKFLOW.md` | 测试工作流文档 |

### 数据库表

| 表 | 说明 |
|----|------|
| `workflow_templates` | 可复用的工作流模板 |
| `workflow_instances` | 具体执行的工作流实例 |
| `tasks` | 任务（目标） |
| `workflow_logs` | 操作日志 |

### 状态流转

```
Task: created → assigned → in_progress → completed/failed/cancelled
Workflow: created → pending → running → completed/failed/cancelled
Step: pending → running → completed/failed
```

### 状态同步

- Workflow 完成 → Task 状态自动更新
- Workflow 失败 → Task 状态自动更新
- 多个 Workflow → Task 状态从最新 Workflow 推导

### 测试角色

本项目设两个测试角色（参考 Google SET/TE 体系）：

| 角色 | 职责 | 维护的文件 |
|------|------|-----------|
| **SET** (Software Engineer in Test) | 测代码 — 测试框架/工具/CI | `tests/test_helpers.py`, `tests/run.py` |
| **TE** (Test Engineer) | 测产品 — 场景/探索/Bug报告 | `tests/test_*.py`, `docs/TEST_*.md` |

```bash
# 跑全部测试 (SET 维护运行器, TE 使用)
python3 tests/run.py

# 覆盖盲区分析
python3 tests/run.py --coverage
```

```bash
# 已有自动化测试（全部通过）
python3 tests/test_router.py          # 路由逻辑 13/13 ✅
python3 tests/test_integration.py     # 集成测试 12/12 ✅
python3 tests/test_role_interaction.py # 角色交互 10/10 ✅
```

详见 `docs/TEST_WORKFLOW.md`（含手动测试点）