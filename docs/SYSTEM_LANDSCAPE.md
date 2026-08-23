# Session 三项目架构全景图

> Updated: 2026-07-26 | Audience: Full-stack engineers | Related: [ARCHITECTURE.md](ARCHITECTURE.md), [DOCS.md](DOCS.md)

## 三个项目一句话

| 项目 | 类比 | 职责 | 代码量 |
|------|------|------|--------|
| **hermes-session-roles** | `/etc/passwd` | 角色定义：31 个角色身份 + 57 个 BH 人格 + 提示词模板 | ~2000 行 Python + 31 JSON + 57 JSON + 26 prompts |
| **session-launcher** | `systemd` | CCS 进程管理：tmux 创建/哨兵/看门狗/CLI 入口 | ~3500 行 Python |
| **session-pipeline** | 消息队列 + 状态机 | 消息路由 + 工作流引擎 + 生命周期状态机 + 可靠性 | ~4000 行 Python |

**实际关系不是三层流水线，而是两消费者并行：**

```
session-roles（定义层）
  ├──→ launcher（执行层）        ← 读 persona JSON 启动 CCS + 注入 prompt
  └──→ pipeline（路由+执行层）    ← 读 roles_export.json 构建路由表 + pipeflow 推动
         └──→ launcher/ccs.py send  ← 唯一跨项目调用（subprocess）
```

## 代码量细节

| 项目 | Python 源文件 | 行数 | 其他 |
|------|-------------|------|------|
| hermes-session-roles | 6 个 `.py` | ~1040 | 31 角色 JSON, 57 人格 JSON, 26 prompt 文件 |
| session-launcher | ~15 个 `.py` | ~3500 | 文档、测试、配置 |
| session-pipeline | ~13 个 `.py` | ~4000 | 测试 136 个 |
| **总计** | **~34 个 `.py`** | **~8500** | **100+ 数据/配置/文档文件** |

## 系统入口

| 项目 | 入口 | 调用方式 |
|------|------|----------|
| session-roles | `python3 src/shared_loader.py --export` | CLI（生成 roles_export.json） |
| session-roles | `python3 src/validate_roles.py` | CLI（全量校验） |
| session-roles | `python3 src/role_assembler.py <role>` | CLI（组装 prompt） |
| session-launcher | `python3 src/ccs.py start/stop/send/output` | CLI（交互）或 subprocess（被 pipeline 调） |
| session-pipeline | `python3 src/pipeflow/daemon.py --interval 10` | systemd / 后台进程 |
| session-pipeline | `python3 src/routing/auto.py --daemon` | CLI |

## 端口与 socket

| 地址 | 所属 | 用途 |
|------|------|------|
| `:20128` | nine-router | 模型路由 |
| `:8767` | hermes-gateway | 外部网关 |
| `:8890` | hermes-control-panel | 控制面板 |
| `:9901` | memory-server-dkk | 大姐记忆服务 |
| `:9902` | memory-server-ssk | 小妹记忆服务 |
| `/tmp/sister_bus_ccs.sock` | Sister Bus | CCS 间直连通信 |
| `/tmp/sister_bus_feed.sock` | Sister Bus | 实时推送 |

## 持久化数据库

| DB | 位置 | 用途 |
|----|------|------|
| Blackboard | `~/.hermes/sister_bus/blackboard.db` | 消息总线 |
| Workflows | `~/.hermes/state/workflows.db` | 工作流实例 + 模板 + 任务 |
| ACK 回执 | `~/.hermes/state/ack_tracker.db` | 消费确认 |
| 路由表 | `~/.hermes/state/routing.db` | 路由表 + 变更审计 |
| Pipeline Cursor | `~/.hermes/state/pipeline_cursor.db` | 去重游标 |

## 哨兵文件

| 目录 | 内容 |
|------|------|
| `/tmp/ccs-sentinels/` | CCS 运行状态（role.json，含 PID/生命周期/伙伴/健康） |
| `/tmp/ccs-health/` | 进程健康数据（watchdog 状态） |
| `/tmp/ccs-lifecycle-sentinels/` | ondemand 角色生命周期哨兵 |
| `/tmp/cdx-sentinels/` | Codex session 哨兵 |

## 工作空间

`~/ccs-workspaces/` 下每个 CCS 角色有独立目录，各含独立 `CLAUDE.md`。
通过 `<!-- WORKSPACE_SYS:START/END -->` + `<!-- KNOWLEDGE:START/END -->` 标记管理专业知识注入。

## 执行循环

```
pipeflow/daemon.py:51-76    while True (sleep 10s) — 系统唯一的执行循环
  └── engine.py:258         run_once()
        ├── _ensure_role_alive()            检查 CCS 存活，死了启动。注入 /loop（无效但无害）
        ├── SQLite 扫描 workflow_instances  按 status IN (pending, running)
        │     └── 对每个运行中工作流：
        │           ├── 步骤未开始 → 发初始 prompt 给目标角色
        │           ├── exit_condition 匹配 → 推进到下一步
        │           ├── 超时未完成 → 提醒→重复提醒→升级给 coordinator
        │           └── 子工作流完成 → 推进父工作流
        ├── _check_anomalies()             检测多步骤卡死→尝试自愈
        └── _scan_tasks()                  同步 task 状态 + 子工作流级联推进
```

## 角色协作矩阵（31 角色）

> 所有 produce/consume 路由来自 `shared_loader.py` → `roles_export.json` → pipeline Router。

| 角色 | 产出 | 消费 |
|------|------|------|
| ccs-monitor | architecture, notice | architecture, notice, blocker, code_fix |
| closer | architecture | _all_ |
| codex-dev | code_review, code_fix | code_fix, architecture, performance |
| coordinator | scheduler, architecture, ccs_health | task_spec, workflow, user_story, test_plan, ... |
| curator | skill_audit, cleanup, architecture | skill_audit, architecture, code_fix, performance |
| debate_verifier | debate, verification | debate, task_spec |
| devops | deployment_plan, deployment_report | architecture, code_fix, security, ... |
| engineer | code_fix, code_review, architecture | architecture, task_spec, user_story, ... |
| investigator_general | root_cause_analysis | _all_ |
| investigator_python | root_cause_analysis | code_fix, architecture |
| investigator_senior | root_cause_analysis | _all_ |
| knowledge_curator | architecture, code_fix, cleanup | _all_ |
| maintainer | code_fix, architecture | security |
| optimizer | architecture, optimization | architecture, performance, bug_report, code_review |
| pg | code_fix, code_review | task_spec, tech_decision, ... |
| pm | user_story, feedback | prd, architecture, test_report, ... |
| product_architect | prd, system_design, task_spec | architecture, code_fix, blocker, ... |
| qa | test_plan, test_report, bug_report | task_spec, code_fix, prd, ... |
| reviewer | code_review, architecture | code_review, architecture, security |
| scout | architecture, evolution_report | architecture |
| security_auditor | security_audit, code_fix | security, blocker, code_fix, ... |
| writer | documentation, changelog | code_fix, architecture, deployment_plan, ... |

## 关键发现（基于代码通读）

### 1. 路由数据源链路
```
角色 JSON → shared_loader --export → roles_export.json → pipeline Router
                                                      ↕ SQLite (routing.db)
```
修改角色定义后必须执行 `shared_loader.py --export` 才能更新路由。Router 是 singleton，需重启进程才能反映变更。

### 2. 双 ensure_paths() 不同行为
三项目各有自己的 `paths.py` 和 `ensure_paths()`:
- **session-roles**: 只加自己的根目录到 sys.path（最纯净）
- **session-launcher**: 加 `SESSION_PIPELINE_SRC` + `HERMES_SCRIPTS` + 自身
- **session-pipeline**: 加 `SESSION_LAUNCHER_SRC` + `HERMES_SCRIPTS` + 自身，并特殊处理 `lifecycle` 包遮蔽

两个 `paths.py`（launcher + pipeline）都从 `hermes_bus.config` 导入核心路径常量。

### 3. Engine 绕过 LifecycleManager
`engine.py` 有 3 处绕过 `lifecycle/manager.py` 直接写 SQL：`_advance_production_wf()`, `_tick()`, `_scan_tasks()`。这些绕过丢失了 RLock 保护和审批流程。

### 4. `/loop` 在 tmux 环境不可用
`_ensure_role_alive()` 注入 `/loop` 是无效操作——不报错，但 CCS 不会进入工作循环。修复方案：`engine.py:582` 将 `"--drive", "loop"` 改为 `"--drive", "ondemand"`。

### 5. 角色契约字段消费缺口
`workgroup`, `cron_schedule`, `sla_seconds` 等字段定义但未消费。`input_signals`, `eval_criteria` 仅注入 prompt 无运行时执行。详见 session-roles ARCHITECTURE.md 的 Consumed fields audit。

## 健康检查入口

```bash
# 全量自检
python3 ~/session-pipeline/tests/health_check_all.py

# 角色定义验证
python3 ~/hermes-session-roles/src/validate_roles.py

# 路由表快照
cd ~/session-pipeline && PYTHONPATH=src python3 -c "
from routing.router import format_pipeline; print(format_pipeline())
"

# CCS 存活检查
python3 ~/session-launcher/src/ccs.py health
```
