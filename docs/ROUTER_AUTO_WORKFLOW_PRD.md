# 路由 Daemon 自动创建工作流 PRD

## 问题陈述

当前 Bus 消息到达 CCS 角色的路径是：路由 daemon 轮询到消息 → `route_to_ccs()` 通过 `ccs send` 推送给 CCS → CCS 直接执行。没有工作流模板参与，任务不可追踪、不可审批、不可回滚。CCS 角色也没有动力主动使用 `wf create`——因为"先创建工作流再执行"是额外步骤，不创建也能把事干了。

## 解决方案

路由 daemon 在推送消息给 CCS 时，根据消息的 `category`、`assignee`、`text` 三个维度自动选择一个工作流模板并创建实例。CCS 收到的不再是 raw bus 消息，而是工作流步骤提示词——任务天然有模板、有步骤、可追踪。

## 用户故事

1. 作为 **engineer CCS**，当我收到 `code_fix` 类型的修复任务时，我不需要手动 `wf create`——路由 daemon 已经创建好了 `engineer_feature` 工作流实例，我直接推进步骤即可，以便我把精力放在编码上而不是流程管理上。

2. 作为 **coordinator CCS**，当我收到 `scheduler` 类型的分发任务时，路由 daemon 自动创建了 `coordinator_dispatch` 工作流，包含了从任务发现→分配→跟踪→验证的完整步骤，以便我不遗漏任何分发环节。

3. 作为 **security_auditor CCS**，当我收到 `security_audit` 类型的告警时，路由 daemon 自动匹配 `security_incident` 模板，包含事件确认→根因分析→修复→复盘的标准流程，以便安全事件处理不跳步。

4. 作为 **pm CCS**，当我收到 `user_story` 类型的需求时，路由 daemon 创建 `pm_requirements` 工作流，自动引导我完成需求收集→故事拆分→优先级排序的完整链路，以便需求不会卡在"接收到但还没拆"的状态。

5. 作为 **系统维护者**，当 bus 上出现 `architecture` 这种被 14 个模板共用的分类时，路由 daemon 通过角色过滤（只保留 engineer 可执行的模板）+ 标题 2-gram 匹配兜底，不会选错模板，以便自动创建不会比手动选择更差。

## 实现决策

### 需要构建/修改的模块

- **路由分发层**（`routing/routes.py`）：`route_to_ccs()` 在成功推送消息后追加 `create_task_v2()` 调用
- **模板推荐层**（`template_registry.py`）：`recommend()` 新增 `bus_category` 参数，精准匹配模板步骤的 exit_condition
- **工作流客户端**（`workflow/client.py`）：`create_task_v2()` 透传 `bus_category` 给 recommend

### 模块接口变更

- `TemplateRegistry.recommend(scenario, initiator_role, assignee, bus_category="")`——`bus_category` 新增，匹配时给含该分类的模板 +5 分
- `WorkflowClient.create_task_v2(title, assignee, template_id, initiator_role, description, parent_task_id, bus_category="")`——`bus_category` 新增透传
- `route_to_ccs()`——消息推送成功后追加 `create_task_v2(title, assignee=role, initiator_role="pipeline", bus_category=category)`

### 技术澄清

- 自动创建是 **best-effort**：如果 recommend 找不到匹配模板（score=0），直接跳过，CCS 仍通过原始路径收到消息。不阻塞、不退化为错误。
- 幂等性由 `route_all` 的 cursor 机制保证：同一条消息不会被重复路由，因此不会重复创建工作流。
- `initiator_role="pipeline"` 表示创建者是路由 daemon 自身，非人工发起——Gate 需要允许 `pipeline` 作为合法发起者，或跳过 Gate 校验。

### 架构决策

- 不改动路由 daemon 的主循环结构——仅在 `route_to_ccs()` 的成功分支追加一行。
- 不改动模板的 schema——bus_category 匹配基于现有的 `exit_condition.bus_category` 字段，无需新增字段。
- route_all（同时分发到多个角色）路径不做自动创建，只在 route_to_ccs（单角色推送）路径做——因为 route_all 是批量消费标记，不涉及 CCS 执行。

### 数据库变更

无。workflow_instances 和 tasks 表已有完整 schema。

## 测试决策

- 好测试：给定一个 mock bus 消息 `{category: "code_fix", text: "修复登录白屏"}`，断言 `route_to_ccs()` 调用后产生了 `workflow_instances` 记录，且 `template_id` 为 `engineer_feature`
- 不测：不测 recommend 的内部打分细节（那是 template_registry 的测试职责）、不测 route_all 路径（不做自动创建）
- 测试接缝：`route_to_ccs()` 的 try/except 块内调用可以被 mock 的 `WorkflowClient` 截获
- 测试先例：`tests/test_router.py` 中的 `test_route_to_ccs` 系列

## 超出范围

- 不改动 `route_all()`——批量消费路径不做自动创建
- 不改动 CCS 循环或 persona 文件——自动创建完全在路由层完成
- 不处理 `architecture` 等宽分类的歧义——角色过滤 + 文本匹配已经够用，未来 category→template 映射数量级增长后再考虑人工黑名单
- 不改动 SQLite 的 `workflow_templates` 表 schema

## 备注

### 匹配优先级示例

| bus category | assignee | 匹配模板 | 依据 |
|------|----------|---------|------|
| `code_fix` | engineer | engineer_feature | category 匹配 + 角色匹配 |
| `security_audit` | security_auditor | security_incident | category 匹配 |
| `task_spec` | coordinator | task_dispatch_execute | category 精确匹配（唯一） |
| `architecture` | product_architect | architect_full_design | category + 角色过滤后 3 选 1，标题 2-gram 活字 |
| `bug_report` | engineer | hotfix_pipeline | category 精准匹配 |
| `code_review` | reviewer | reviewer_pr_review | category 匹配 |
| `deployment_report` | devops | deploy_canary / devops_deploy_execute | category 命中 2 个，角色过滤后标题打分 |
| `standup` | coordinator | coordinator_standup | category 唯一匹配 |
| `retrospective` | coordinator | sprint_retro | category 唯一匹配 |
