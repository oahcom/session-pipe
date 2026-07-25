# 文档治理

> 版本: 1.0 | 更新: 2026-07-24

## 文档职责分工

每篇文档有**唯一职责**，不重叠。读者按需查阅。

| 文档 | 受众 | 职责 | 不包含 |
|------|------|------|--------|
| **README.md** | 新开发者 (5min) | 项目是什么、怎么跑起来 | 实现细节、API 签名 |
| **AGENTS.md** | AI Agent + 维护者 | 协作红线、自维护指令、验证命令 | 项目介绍、架构图 |
| **PUBLIC_API.md** | API 消费者 | 函数签名、参数、返回值 | 实现原理、使用示例 |
| **docs/ARCHITECTURE.md** | 架构师 | 模块关系、数据流、依赖 | 具体代码、配置细节 |
| **docs/TECHNICAL_DEEP_DIVE.md** | 高级开发者 | 实现原理、配置热重载、可靠性机制 | 入门指南 |
| **docs/SYSTEM_LANDSCAPE.md** | 全栈工程师 | 三项目全景、端口、DB、角色矩阵 | 单项目细节 |
| **docs/TEST_WORKFLOW.md** | 测试工程师 | 测试运行方式、覆盖率、已知缺口 | 测试设计原理 |
| **docs/WORKFLOW_TEMPLATE_SPEC.md** | 模板开发者 | Schema 规范、审查机制、测试场景 | 引擎实现 |

## 核心规则单源

以下规则**只在 AGENTS.md 维护**，其他文档引用：

- 协作红线（§协作红线）
- 自维护指令（§自维护指令）
- Git 工作流（§Git 工作流）
- 7 维度验证（§路由修改 7 维度验证）

## 变更流程

1. 修改代码后，检查是否涉及文档变更
2. 如涉及，更新对应文档 + 时间戳
3. 核心规则变更只改 AGENTS.md
4. 运行 `python3 docs/doc_check.py` 验证一致性

## 文档清单

```
session-pipeline/
├── README.md                        # 快速入门
├── AGENTS.md                        # 协作规则 (单源)
├── PUBLIC_API.md                    # API 签名
└── docs/
    ├── DOCS.md                      # 本文档 (治理索引)
    ├── ARCHITECTURE.md              # 架构
    ├── TECHNICAL_DEEP_DIVE.md       # 技术深度
    ├── SYSTEM_LANDSCAPE.md          # 全景图
    ├── TEST_WORKFLOW.md             # 测试
    ├── WORKFLOW_TEMPLATE_SPEC.md    # 模板规范
    └── doc_check.py                 # 一致性检查脚本
```
