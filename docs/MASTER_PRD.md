# Session Pipeline — 全量需求文档

> 版本 1.0 | 2026-07-24

---

## 一、模板元数据标准化

### 问题
32 个 JSON 工作流模板缺少 `trigger_scene`、`allowed_initiators`、`allowed_executors` 等字段，导致推荐系统无法准确匹配模板。

### 方案
每个模板必须包含 6 个必填元数据字段：

| 字段 | 要求 | 校验 |
|------|------|------|
| `workflow_id` | `"WF-" + name` | 必填 |
| `trigger_scene` | ≥1 条，每条 ≥4 字符，可被关键词匹配 | 非空列表 |
| `allowed_initiators` | ≥1 个角色 | 非空列表 |
| `allowed_executors` | 覆盖所有步骤的 target_role | 非空列表 |
| `max_duration_hours` | ≥1 | 数值 ≥1 |
| `quality_standards` | ≥8 字符，领域特化，不含"通过 Codex"等泛化 | 字符串 ≥8 |

### 验收标准
- [ ] `python3 -m template_registry validate-all` 全部通过
- [ ] 每个模板至少有 2 条 trigger_scene 不包含"相关任务"等泛化后缀
- [ ] allowed_executors 包含所有步骤的 target_role 角色

---

## 二、模板内容质量提升

### 问题
- failure_patterns 全部共用 5 条通用套话（"产出物为空或仅模板占位符"），无法区分各步骤的实际失败场景
- quality_standards 全部沿用"通过 Codex 审查"等套话，与具体模板业务场景无关
- title 20/32 以"工作流/流程"结尾，不传达业务目的
- prompt 每步重复 ~500B 通用模板段落（"## 操作要求 1. 理解任务背景…"），稀释定制指令

### 方案
- title：去掉"工作流/流程"后缀，格式为 `<动词短语>`
- quality_standards：每模板写入领域特化的 2-3 条量化标准（如"单元测试覆盖率 ≥80%"）
- failure_patterns：每步写入归属该步骤业务场景的 3-4 条可观测失败信号
- prompt：剥离"## 操作要求～## 质量门禁"段落和"## 引用要求"段落

### 验收标准
- [ ] 所有 title 不含"工作流"/"流程"后缀
- [ ] 所有 quality_standards 不含"Codex"关键词
- [ ] 所有 failure_patterns 不含"产出物为空或仅模板占位符"等 5 条通用套话
- [ ] 所有 prompt 不含"## 操作要求\n1. 理解任务背景"段落
- [ ] 所有 prompt 不含"所有事实性声明标注来源，无法确定时标注 [来源待确认]"段落

---

## 三、推荐系统

### 问题
`create_task_v2` 必须显式传 `template_id`，CCS 角色需要自己知道用哪个模板。不同任务适合的工作流不同，角色也没有动力主动用模板。

### 方案
- `create_task_v2(template_id)` → `create_task_v2(template_id="")`：未传 template_id 时自动推荐
- 推荐算法：角色过滤（排除发起者/执行者不匹配）→ bus_category 匹配（+5 分）→ trigger_scene 2-gram 匹配
- 中文文本：2-gram 字符交叉匹配代替空格分词

### 验收标准
- [ ] PM→engineer 场景推荐结果包含 engineer_feature
- [ ] coordinator→security_auditor 场景推荐结果包含 security_incident 或 security_audit_scan
- [ ] scout→engineer 场景推荐结果为空（scout 不能发起开发任务）
- [ ] qa→devops 场景推荐结果为空（qa 不能发起部署任务）
- [ ] "修复登录白屏"→engineer 推荐结果包含 hotfix_pipeline（中文 2-gram 匹配验证）

---

## 四、路由 Daemon 自动创建 Workflow

### 问题
CCS 角色没有动力主动使用 `wf create`——不创建也能把事情干了。路由 daemon 目前仅推送 raw bus 消息。

### 方案
`route_to_ccs()` 在成功推送消息后，追加调用 `create_task_v2(title, assignee=role, initiator_role="pipeline", bus_category=category)`。推荐系统匹配到模板则创建，匹配不到则跳过（退化为原始推送行为）。

### 触发条件
每次 route_to_ccs 推送消息时都尝试。不进白名单/黑名单，推荐系统自身作为过滤器。

### 幂等性
由 route_all 的 cursor 机制保证——同一条消息不会被重复路由，因此不会重复创建工作流。

### Gate 处理
`initiator_role="pipeline"` 跳过发起人角色校验（否则"pipeline"不在任何 allowed_initiators 中）。

### 验收标准
- [ ] bus 消息 `{category: "code_fix", text: "修复登录白屏"}` 路由给 engineer 时产生 workflow_instance 记录
- [ ] 创建的工作流 template_id 为 engineer_feature 或 hotfix_pipeline
- [ ] bus 消息 `{category: "architecture", text: "状态更新"}` 路由给 product_architect 时不产生 workflow_instance（无匹配模板）

---

## 五、Prompt 角色定义剥离

### 问题
工作流模板的 prompt_template 中包含了"请完成「…」。这是…工作流的步骤之一，你的角色是…"等角色介绍性语言，与 hermes-session-roles 的 persona 文件职责重叠。实际运行中 persona 文件多为空，角色定义由 claude.md 处理，不应重复。

### 方案
- 移除 prompt_template 中的 `/goal\n## 任务\n请完成「…」。这是…工作流的步骤之一，你的角色是…。` 头部
- 移除 `## 原始提示` 标头
- 在 `_send_to_role()` 中统一添加 `## 工作习惯` 前缀，提醒 CCS 使用 `wf create`

### 验收标准
- [ ] 所有 prompt 不含"请完成「"字符串
- [ ] 所有 prompt 不含"## 原始提示"字符串
- [ ] `_send_to_role()` 发出的所有 message 以 `/goal\n\n## 工作习惯\n每次开始工作前…` 开头

---

## 六、校验与治理

### 问题
模板规范仅记录在文档中，无人主动阅读。"写的人不读，读的人不写"。

### 方案
- 引擎加载时自动校验：`_load_workflows()` 对每个 JSON 模板调 `_validate_meta()`
- 推荐时自动过滤：`recommend()` 跳过 `_validate_meta()` 不合格的模板
- 新增内容校验：failure_patterns 泛化检测、prompt 模板段落残留检测、## 完成标头检查

### 验收标准
- [ ] 故意写一个缺少 trigger_scene 的模板，引擎启动时打印警告
- [ ] 故意写一个 quality_standards 仅"Codex"的模板，recommend 不会返回它
- [ ] validate-all 能检测出缺少"## 完成"标头的步骤

---

## 七、CLI 工具

### 方案
- `wf create --template` 改为可选（不指定则自动推荐）
- 新增 `wf suggest <title> --initiator <角色> --assignee <角色>` 命令

### 验收标准
- [ ] `wf create "修复登录白屏" --assignee engineer` 成功创建 workflow 并返回 wf_id
- [ ] `wf suggest "修复登录白屏" --initiator coordinator --assignee engineer` 返回推荐列表
