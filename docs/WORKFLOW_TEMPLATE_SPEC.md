# 工作流模板规范

> 版本: 2.0 | 更新: 2026-07-24 | 受众: 模板开发者
> 本文件定义 `~/.hermes/workflows/*.json` 的编写规范。

---

## 1. 模板文件结构

每个 `.json` 文件必须包含以下字段：

```json
{
  "name": "engineer_feature",              // 模板唯一标识（文件名也应匹配）
  "title": "功能开发",                       // 人类可读名（禁止以"工作流/流程"结尾）
  "description": "engineer 接收任务...",     // 简短描述

  "workflow_id": "WF-engineer_feature",
  "trigger_scene": ["功能开发", "编码实现"],   // ≥1 条，用于 recommend 匹配
  "allowed_initiators": ["coordinator", "pm"], // 可发起此工作流的角色
  "allowed_executors": ["engineer"],          // 可被指派为此工作流执行者的角色
  "max_duration_hours": 24,                  // ≥1 的整数
  "quality_standards": "单元测试覆盖率 ≥80%...", // 领域特定的量化标准（≥8 字符）

  "steps": [ /* 见第 2 节 */ ]
}
```

### 1.1 必填字段速查

| 字段 | 校验规则 | 常见错误 |
|------|----------|----------|
| `trigger_scene` | 非空列表，每条 ≥4 字符 | 写"通用XXX相关任务"这种泛化描述 |
| `allowed_initiators` | 非空列表，每项为有效角色 | 抄别的模板不核对 |
| `allowed_executors` | 非空列表 | 遗漏步骤中的 target_role |
| `max_duration_hours` | ≥1 的数字 | 对多步模板写 4h 明显不够 |
| `quality_standards` | ≥8 字符，不写"通过 Codex 审查" | 使用泛化套话 |

---

## 2. 步骤（steps）

### 2.1 步骤基本结构

```json
{
  "id": "s1",
  "title": "编码实现",
  "type": "handoff",                   // handoff | review | single | gate | notify
  "target_role": "engineer",           // 该步骤的执行角色
  "prompt_template": "/goal\n## 任务\n请完成「编码实现」...", // 见 2.3
  "exit_condition": {
    "bus_category": "code_fix",        // 等待的 bus 消息分类
    "source_contains": "engineer",     // 可选，消息来源过滤
    "timeout_minutes": 60              // 超时时间
  },
  "max_retries": 1,
  "failure_patterns": [                // 见 2.4
    "实现代码未通过单元测试或编译失败",
    "接口契约与设计文档不一致"
  ]
}
```

### 2.2 步骤类型

| 类型 | 完成策略 | 适用场景 |
|------|----------|----------|
| `single` | 自动推进（不阻塞） | 一次性操作：提交、通知、发现 |
| `handoff` | 需审批确认（`confirm_step`） | 交接给下游角色 |
| `review` | 需审批确认 + 审批密钥 | 代码审查、设计评审 |
| `gate` | 条件检查（如文件是否存在） | 门禁、质量关卡 |
| `notify` | 自动推进 | 通知下游、触发事件 |

### 2.3 Prompt 模板规则

```markdown
/goal
## 任务
请完成「{步骤标题}」。这是 {模板名} 的步骤之一，你的角色是 {角色}。

## 原始提示
[这里直接写步骤特有的指令，不加通用操作要求]
```

**禁止**出现以下段落：
```
## 操作要求          → 已剥离（之前的版本中已全局清除）
## 产出物要求         → 已剥离
## 质量门禁           → 已剥离（替代为 quality_standards）
## 引用要求           → 已剥离
```

如果步骤需要搜索、框架或引用，用内联方式融入指令中，不单独成节。

### 2.4 Failure Patterns 规则

- 每步 **3~4 条**
- 必须归属该步骤的业务场景（不可跨步复制）
- 描述一个**可观测的失败信号**（"X 没有 Y"）
- **禁止**以下泛化描述：

```
❌ 产出物为空或仅模板占位符
❌ 产出物非预期格式（缺少必要章节）
❌ 步骤执行后未写对应 bus 消息通知下游
❌ 产出物质量审查未通过（P0/P1 问题）
```

### 2.5 Exit Condition 规则

- `timeout_minutes` 至少为正整数
- `bus_category` 应使用规范分类（如 `code_fix`、`architecture`、`code_review`）
- 如果步骤不需要等待 bus 消息（如 gate 类型），使用 `completion_check` 替代

---

## 3. 三层架构与 subflow_template

### 3.1 架构模型

```
编排层 (orchestrator)         领域层 (domain flow)
  task_dispatch_execute       coordinator_dispatch
  architect_adr               architect_full_design
  code_review_chain           engineer_feature / reviewer_pr_review
  deploy_canary               devops_deploy_execute
  qa_test_plan                qa_test_execute
  security_incident           security_audit_scan

原子层 (atomic) — 独立工作流，不依赖编排
  closer_close_loop, investigator_analyze, lr_tech_decision,
  maintainer_health_monitor, pm_requirements, scout_research_cycle, writer_document
```

### 3.2 subflow_template 使用规则

```
编排层: 每步引用一个领域层模板作为子工作流    → ✅ 允许
领域层: 不引用任何 subflow_template         → ❌ 禁止
原子层: 不引用任何 subflow_template         → ❌ 禁止
自引用: subflow_template 指向自身          → ❌ 禁止（已被全局清理）
悬空引用: 指向不存在的模板名               → ❌ 禁止（已被全局清理）
```

校验：
```bash
PYTHONPATH=src python3 -c "
from template_registry import TemplateRegistry
# 所有 subflow_template 引用的模板名必须能在 SQLite 或 JSON 文件中找到
"
```

---

## 4. 角色配置规则

- `allowed_initiators`：包含该模板合理发起者的角色（通常为 `coordinator`、`pm` 等）
- `allowed_executors`：**必须**包含所有步骤中 `target_role` 的去重集合
- 如果有步骤角色不在 `allowed_executors` 中，则该角色无法被指派为此工作流的执行者，会导致运行时异常

---

## 5. 校验与治理

### 5.1 日常校验

```bash
# 校验所有模板的元数据完整性 + 内容质量
PYTHONPATH=src python3 -m template_registry validate-all

# 校验单个模板文件
PYTHONPATH=src python3 -m template_registry validate path/to/template.json
```

### 5.2 validate-all 检查清单

| 检查项 | 规则 |
|--------|------|
| trigger_scene | 非空列表 |
| allowed_initiators | 非空列表 |
| allowed_executors | 非空列表 |
| max_duration_hours | ≥1 |
| quality_standards | ≥8 字符，不写纯"通过 Codex" |
| prompt 模板段落残留 | 不含"## 操作要求\n1. 理解任务背景" |
| prompt 长度 | ≥150B |
| failure_patterns 泛化 | 不含"产出物为空或仅模板占位符"等 |
| failure_patterns 回退模式 | 不含"执行步骤未达预期质量标准" |
| quality_standards 领域化 | 不单独写"Codex" |

### 5.3 创建新模板流程

```
1. 确定模板归属层（编排/领域/原子）
2. 编写 steps，按规范填写所有字段
3. 运行 validate-all 确保通过
4. 用 recommend 验证场景匹配
5. 如果是领域层模板，确认已有编排层引用它
```

---

## 6. 常用命令速查

```bash
# 列出所有模板
PYTHONPATH=src python3 -m template_registry list

# 按场景推荐
PYTHONPATH=src python3 -m template_registry recommend "编码实现" coordinator engineer

# 验证所有模板
PYTHONPATH=src python3 -m template_registry validate-all

# 启动引擎（自动同步模板元数据到 SQLite）
PYTHONPATH=src python3 -c "from pipeflow.engine import WorkflowEngine; WorkflowEngine()"
```


## 7. Bus 消息规范

### 7.1 步骤完成消息

每步的 `## 完成` 指令中的 `写 bus cat=XXX` 分类必须与步骤的 `exit_condition.bus_category` 一致。不一致时引擎无法检测到步骤完成，步骤将一直等到超时。

```json
{
  "exit_condition": { "bus_category": "code_fix" },
  "prompt_template": "/goal\n...\n## 完成\n写 bus cat=code_fix 标记完成。"
}
// ✅ 一致 — exit_condition 等待 code_fix，CCS 也写 code_fix
```

```json
{
  "exit_condition": { "bus_category": "architecture" },
  "prompt_template": "/goal\n...\n## 完成\n写 bus cat=notice 标记完成。"
}
// ❌ 不一致 — exit_condition 等 architecture，但 CCS 写 notice，永远等不到
```

`validate-all` 会检测这种不一致。

### 7.2 分类选用原则

| 情况 | 推荐分类 | 不要用 |
|------|----------|--------|
| 通知下游角色 | 业务相关分类 | `notice`（太泛） |
| 状态更新 | 领域分类 | `architecture`（兜底分类，22/105 步在用） |
| 技术决策 | `tech_decision` | `architecture` |
| 架构设计 | `system_design` 或 `architecture` | — |
| 代码修复 | `code_fix` | `architecture` |
| 安全事件 | `security_audit` | `architecture` |

`architecture` 应只在确实涉及架构输出时使用（ADR、系统设计），避免作为所有产出的兜底分类。
