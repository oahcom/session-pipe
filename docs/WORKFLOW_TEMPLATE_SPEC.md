# 工作流模板规范

> 版本: 2.1 | 更新: 2026-10-22 | 受众: 模板开发者
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
| `handoff` | 自动推进（交接，非审批闸门） | 交接给下游角色 |
| `review` | 等 target_role 用 bus 文本给结论（`CHANGES_REQUESTED`/`rejected`，判据 `src/review_gate.py`） | 代码审查、设计评审 |
| `gate` | 校验 `completion_check` / `exit_schema` | 门禁、质量关卡 |
| `notify` | 自动推进 | 通知下游、触发事件 |

审批密钥已于 2026-10-22 退场（CHANGES.md）；驳回语义由 bus 文本承担，无需 token。

### 2.3 Prompt 模板规则

校验器（`template_registry.py:339-363`）要求 `prompt_template` 含**三段式小标题**，且总长 ≥150B：

```markdown
/goal
## 你负责……（做什么：本轮要产出的东西）

## 你要做的
1. ……（怎么做：可执行的步骤）

## 完成
写 bus cat=XXX，title 含 s<N>-complete（验收标准：可二值判定）
```

实测（2026-10-22，复算 `python3 src/template_registry.py validate-all`，模板总数用 `select count(*) from workflow_templates`）：少数模板通过 `validate-all`，失败项主要是缺"做什么/怎么做/验收标准"三段与 prompt 过短（如 `test_exit_schema.json` 138B < 150B）。

**禁止**出现 `## 操作要求` / `## 引用要求` 这两个段落（需要搜索/引用时内联写进指令）。
校验器只拦两个**字面串**（`template_registry.py:_BOILER_PROMPT_SECTIONS`）：
`## 操作要求\n1. 理解任务背景` 与 `## 引用要求\n所有事实性声明标注来源`——续行不同就漏；
`## 产出物要求` / `## 质量门禁` **没有任何判据**。所以「已全局清除」不成立：
2026-10-22 实测 `lr_tech_decision.json` 仍有 2 处 `## 产出物要求`、`knowledge_sync.json` 2 处
`## 引用要求`，validate-all 都不报。

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

### 3.1 架构模型（2026-10-22 从模板 JSON 实测）

```
编排层（引用子链）                     子链（被引用）
  architect_review        → qa_test_execute
  coordinator_schedule    → coordinator_dispatch
  dev_implement           → engineer_feature, reviewer_pr_review
  food_chain_loop         → pm_requirements, architect_full_design, engineer_feature,
                            qa_test_execute, reviewer_pr_review
  full_dev_pipeline       → architect_full_design, engineer_feature, qa_test_execute,
                            reviewer_pr_review, closer_close_loop
  产品交付全流程          → pm_requirements, architect_full_design, reviewer_pr_review,
                            engineer_feature, qa_test_execute

其余模板（清单见 `template_registry list`），含上面那些带子链的
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
PYTHONPATH=src python3 -m template_registry validate-all
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
| allowed_executors | 非空列表，覆盖全步骤 target_role |
| max_duration_hours | ≥1 |
| prompt 三段式 | 含子串「做什么 / 怎么做 / 验收标准」三者且 ≥150B（`_validate_step_content`） |
| prompt 完成标头 | 含 `## 完成` 标头（单独硬检，不在三段式里） |
| prompt 模板段落残留 | 不含上节两个泡沫串（无「`## 原始提示`」判据） |
| failure_patterns 泛化 | 不含"产出物为空或仅模板占位符"等 |
| failure_patterns 回退模式 | 不含"执行步骤未达预期质量标准" |

### 5.3 创建新模板流程

```
1. 确定模板归属层（编排/领域/原子）
2. 编写 steps，按规范填写所有字段
3. 运行 validate-all 看输出——**它不返回非零**：2026-10-22 实测 24/28 FAIL 而 rc=0，不能当 CI 门禁
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

#### `require_wf: true` 的死锁陷阱（强制）

若步骤 `exit_condition` 含 `"require_wf": true`，引擎（`step_handler._check_exit` 的 `strict_wf`）**硬性要求**总线消息标题正文含当前实例 id（如 `wf_728c6e4307c1`）才匹配退出。此时模板的 `## 完成` 指令**必须让角色把实例 id 写入消息标题**，否则即使产出也永久匹配不到 → 死锁。

```json
{
  "exit_condition": {
    "bus_category": "user_story", "source_contains": "pm",
    "text_contains": "s2-complete", "require_wf": true
  },
  "prompt_template": "/goal\n...\n## 完成\n写 bus cat=user_story，标题必须同时含 s2-complete 与本实例 id（wf_xxxx 格式，引擎在 task_spec 标题中给出）。缺少实例 id 会导致 require_wf 硬校验永久匹配不到，工作流卡死。"
}
// ✅ 模板要求写实例 id → 角色产出标题含 wf_xxx → 匹配成功
```

```json
{
  "exit_condition": { "...", "require_wf": true },
  "prompt_template": "/goal\n...\n## 完成\n写 bus cat=user_story，标题含 s2-complete。"
}
// ❌ 模板未要求写实例 id → 角色产出标题只有 s2-complete → strict_wf 永不匹配 → 死锁
```

> **根因**：`_check_exit` 比对的是 `Fact.t`（消息标题），非正文。扫描发现 `food_chain_loop/s2` 与 `WL-09_doc_driven_5stage/s1` 曾缺此约束，已修。新增 `require_wf: true` 步骤时必须同步在 `## 完成` 段加实例 id 约束。

### 7.2 分类选用原则

| 情况 | 推荐分类 | 不要用 |
|------|----------|--------|
| 通知下游角色 | 业务相关分类 | `notice`（太泛） |
| 状态更新 | 领域分类 | `architecture`（兜底分类；2026-10-22 实测各模板写入步骤的分类分布：`code_fix` 最多、`architecture` 次之——复算要真跑一次再统计，别抄这里的排名） |
| 技术决策 | `tech_decision` | `architecture` |
| 架构设计 | `system_design` 或 `architecture` | — |
| 代码修复 | `code_fix` | `architecture` |
| 安全事件 | `security_audit` | `architecture` |

`architecture` 应只在确实涉及架构输出时使用（ADR、系统设计），避免作为所有产出的兜底分类。
