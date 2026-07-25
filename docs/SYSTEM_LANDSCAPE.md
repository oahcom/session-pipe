# Session 三项目架构全景图

> 更新: 2026-07-24 | 受众: 全栈工程师 | 相关: [DOCS.md](DOCS.md)

## 三个项目一句话

| 项目 | 类比 | 职责 | 代码量 |
|------|------|------|--------|
| **hermes-session-roles** | `/etc/passwd` | 角色定义：32 个角色身份 + 提示词模板 | 1,750 行 |
| **session-launcher** | `systemd` | CCS 进程管理：tmux 创建/哨兵/看门狗/协作 | 7,737 行 |
| **session-pipeline** | 消息队列 | 消息路由 + 工作流引擎 + 可靠性基础设施 | 6,088 行 |

---

## 端口与 socket

| 地址 | 所属 | 用途 |
|------|------|------|
| `:20128` | nine-router | 模型路由 |
| `:8767` | hermes-gateway | 外部网关 |
| `:8890` | hermes-control-panel | 控制面板 |
| `:9901` | memory-server-dkk | 大姐记忆服务 |
| `:9902` | memory-server-ssk | 小妹记忆服务 |
| `/tmp/sister_bus_ccs.sock` | Sister Bus | CCS 间直连通信 |
| `/tmp/sister_bus_cron.sock` | Sister Bus | cron 通道 |
| `/tmp/sister_bus_dkk.sock` | Sister Bus | 大姐通道 |
| `/tmp/sister_bus_feed.sock` | Sister Bus | 实时推送 |
| `/tmp/sister_bus_ssk.sock` | Sister Bus | 小妹通道 |

## 持久化数据库

| DB | 位置 | 大小 |
|----|------|------|
| Blackboard | `~/.hermes/sister_bus/blackboard.db` | 1.3MB |
| Workflows | `~/.hermes/state/workflows.db` | 176KB |
| ACK 回执 | `~/.hermes/state/ack_tracker.db` | 4.3MB |
| 路由表 | `~/.hermes/state/routing.db` | 40KB |
| Pipeline Cursor | `~/.hermes/state/pipeline_cursor.db` | 28KB |

## 哨兵文件

| 目录 | 内容 |
|------|------|
| `/tmp/ccs-sentinels/` | CCS 运行状态 (role.json) |
| `/tmp/cdx-sentinels/` | Codex session 哨兵 |

## 工作空间

**`~/ccs-workspaces/`** 下 32 个角色 workspace，各含独立 `CLAUDE.md`，
通过 `<!-- WORKSPACE_SYS:START/END -->` + `<!-- KNOWLEDGE:START/END -->` 标记管理。

---

## 数据流

```
角色 JSON (hermes-session-roles)
  ↓ router._build_routing()
路由表 (session-pipeline + routing.db)
  ↓ auto_route.poll_unconsumed()
CCS 消费者 (session-launcher 启动的 tmux session)
  ↓ bus_client.py write/read
Blackboard SQLite 中心总线
```

---

## 角色协作矩阵（32 角色）

> 全部 32 个 session 角色的 produce/consume 路由，来自 `router._build_routing()`。

| 角色 | 产出 → | 消费 ← |
|------|--------|--------|
| ccs-monitor | architecture, notice | architecture, notice, blocker, code_fix |
| closer | architecture | *全部* |
| codex-dev | code_review, code_fix | code_fix, architecture, performance |
| coordinator | scheduler, architecture, ccs_health | task_spec, workflow, user_story, test_plan, deployment_plan, deployment_report, test_report, bug_report, security_audit |
| curator | audit_report, cleanup_report, architecture | audit_report, architecture, code_fix, performance |
| debate_verifier | debate, verification_report | debate, task_spec |
| devops | deployment_plan, deployment_report, dashboard | architecture, code_fix, test_report, security, deployment_plan, threat_model, documentation |
| double_test | test_report, bug_report | task_spec, bug_report |
| engineer | code_fix, code_review, architecture | architecture, task_spec, user_story, code_review, bug_report, test_report, test_plan, documentation, security_audit |
| integration_test | test_report | task_spec |
| investigator_general | root_cause_analysis | *全部* |
| investigator_python | root_cause_analysis | code_fix, architecture |
| investigator_senior | root_cause_analysis | *全部* |
| knowledge_curator | architecture, code_fix, cleanup_report, distilling_report, memory_report | *全部* |
| lr | tech_decision, architecture | task_spec, architecture, root_cause_analysis |
| maintainer | code_fix, architecture | security |
| optimizer | architecture, performance | architecture, performance, bug_report, code_review |
| pg | code_fix, code_review | task_spec, tech_decision, root_cause_analysis, bug_report |
| pm | user_story, feedback | prd, architecture, test_report, deployment_report, feedback, bug_report, documentation |
| product_architect | prd, system_design, task_spec | architecture, code_fix, product_design, blocker, design_issue, threat_model, security_audit |
| qa | test_plan, test_report, bug_report | task_spec, code_fix, prd, test_plan, bug_report |
| reviewer | code_review, architecture | code_review, architecture, security |
| scout | architecture, evolution_report | architecture |
| security_auditor | security_audit, code_fix, architecture | security, blocker, code_fix, security_audit, threat_model, deployment_plan |
| writer | documentation, changelog | code_fix, architecture, deployment_plan, deployment_report, documentation |

---

## 健康检查入口

```bash
# 全量自检
python3 /home/administrator/session-pipeline/tests/health_check_all.py
# 结果: Blackboard (architecture cat) + /tmp/session-health-report.json
```
