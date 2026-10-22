# 文档治理

> 更新: 2026-10-22

## 文档职责分工

每篇文档有**唯一职责**，不重叠。读者按需查阅。

| 文档 | 受众 | 职责 |
|------|------|------|
| **README.md** | 新开发者 (5min) | 项目是什么、怎么跑起来、CLI |
| **AGENTS.md** | AI Agent + 维护者 | 协作红线、自维护指令、模板元数据治理（**规则单源**） |
| **CLAUDE.md** | 本机维护者 | 代码布局与行数、P0 豁免、上下文溢出、SQLite 运维、测试隔离 |
| **PUBLIC_API.md** | API 消费者 | 函数签名、参数、返回值 |
| **ISSUES.md** | 排障者 | 历史问题单（已复核状态表） |
| **CHANGES.md** | 维护者 | 按日期的变更记录（含验证命令与实测输出） |
| **docs/ARCHITECTURE.md** | 架构师 | 模块清单、数据流、已知缺陷（**不写行数**；行数在 CLAUDE.md，由 `scripts/doccheck.py` 校验） |
| **docs/TECHNICAL_DEEP_DIVE.md** | 高级开发者 | 实现原理、配置加载、可靠性机制、对话监控、状态同步 |
| **docs/SYSTEM_LANDSCAPE.md** | 全栈工程师 | 三项目全景、端口、DB、角色矩阵 |
| **docs/TEST_WORKFLOW.md** | 测试工程师 | 测试命令、实测通过数、已知缺口 |
| **docs/WORKFLOW_TEMPLATE_SPEC.md** | 模板开发者 | 模板 Schema、prompt 三段式、subflow 规则、校验命令 |
| **docs/collab-modes-implementation.md** | 模板开发者 | 步骤级协作声明 `guard` / `wait_roles` 与 persona 注入 |
| **docs/ADR-APPROVAL-AUTHORITY.md** | 排障者 | 审批权威性高于文本判驳的决策记录 |
| **docs/DOCS.md** | 维护者 | 本文档（索引 + 规则单源声明） |

## 核心规则单源

以下规则**只在 AGENTS.md 维护**，其他文档引用：

- 协作红线（§协作红线）
- 自维护指令（§自维护指令）
- Git 工作流（§Git 工作流）
- 7 维度验证（§路由修改 7 维度验证）
- 模板必填元数据（§6 工作流模板治理）

## 变更流程

1. 修改代码后，检查是否涉及文档变更
2. 如涉及，更新对应文档 + 时间戳；CLAUDE.md 的行数声明用 `python3 scripts/doccheck.py --fix`（别的文档不写行数）
3. 核心规则变更只改 AGENTS.md
4. 一次性产物（单次审查报告、交接单）不留在仓库：结论并入 ISSUES.md / CHANGES.md
