# PRD：对话增量监控 — Conversation Monitor

## Problem Statement

pipeline daemon 通过 bus（`exit_condition.bus_category`）判断工作流步骤是否完成，但完全看不到 CCS 角色的实际对话内容。导致三个具体问题：

1. **完成信号漏检**：角色在对话中已确认完成（如 "s1 已由 complete+confirm 正确关闭"），但 bus 消息尚未匹配 `exit_condition`，daemon 仍按超时逻辑等待 30 分钟后才升级。
2. **空派发**：角色已主动表示空闲（如 "Same generic template. No change. Idle."），daemon 无法感知，继续给该角色派发无效 prompt，浪费 token。
3. **无法验证实际工作内容**：角色可能写了 bus 消息（匹配 exit_condition），但对话记录显示它实际做了无关的事（如工程师写设计文档而非代码），daemon 看不到这个差异。

## Users

| 用户 | 身份 | 诉求 |
|------|------|------|
| pipeline daemon | 消费方 | 用对话信号辅助判断步骤完成，减少空转 |
| coordinator | 审核方 | 能看到角色是否真正在做被分配的工作 |
| developer | 排障方 | 能通过对话记录追溯"为什么这个步骤没推进" |

## Scope

**在范围内：**
- daemon 进程中新增对话增量读取模块（`conversation_monitor.py`）
- `engine.py` 的 `_tick()` 中加入对话信号检测，辅助 exit_condition 判断
- 三个信号：步骤完成 / 角色主动说 idle / 操作超出职责（仅模式匹配）

**不在范围内：**
- 对话历史持久化（不存文件、不进 SQLite）
- 全量对话解析 / 语义理解
- 修改 bus 协议
- 修改 lifecycle manager 的审批流程
- 替换现有 `exit_condition` 机制（对话信号是补充，不是替代）

## User Stories

1. 作为 pipeline daemon，当角色在对话中明确表示"步骤完成"时，能提前触发 `complete_step`，不等待 bus 消息匹配。
2. 作为 pipeline daemon，当角色说"没有真实任务"或"idle"时，能跳过对该角色的下一轮 prompt 推送。
3. 作为 coordinator，能从日志中看到角色对话中是否出现了职责越界信号（如工程师写设计文档、PG 自己跑测试）。

## Out of Scope

- 对话内容全文存储（隐私 + 性能开销）
- LLM 驱动的对话语义分析（不引入额外 LLM 调用）
- 替代或修改现有 bus 退出条件机制

## Goal

减少 pipeline daemon 的空转（等待已完成步骤的超时 + 对空闲角色的无效派发），同时提供角色工作内容的可观测性补充信号。

## Criteria

- [ ] 新增 `conversation_monitor.py`，含 `capture_role_pane(role)` 和 `parse_signals(pane_text, role)` 两个核心函数
- [ ] `capture_role_pane()` 在角色不存在时返回空字符串，不抛异常
- [ ] `parse_signals()` 对三个信号（完成 / idle / 越界）做正则匹配，返回结构化 `ConversationSignal`
- [ ] `engine.py` 的 `_tick()` 中读取对话信号：`step_completed=True` 时触发 `complete_step`，`idle_complaint=True` 时跳过派发
- [ ] 不读全量历史，只读最后 80 行增量（`tmux capture-pane -S -80`）
- [ ] 不引入新的 Python 依赖（stdlib + 已有 subprocess 模式）
- [ ] 现有 bus-based exit_condition 完全保留，对话信号是并行补充，不替代任何现有逻辑
- [ ] 单元测试覆盖三个信号的正则匹配（`test_conversation_monitor.py`）

## Test Strategy

- **单元测试**：给 `parse_signals()` 各种输入（含完成信号、idle 信号、越界信号、无信号、混淆信号），验证返回值
- **集成测试**：用 `tmux` 创建临时 session → 写入已知文本 → 调用 `capture_role_pane()` → 验证读取内容
- **不破坏性**：现有测试全部通过，不修改任何已有行为

## 技术约束

| 约束 | 说明 |
|------|------|
| tmux capture-pane 不影响输出 | `-p` 参数打印到 stdout，不修改 pane 内容 |
| 不读全量历史 | 只读 `-S -80`（最后 80 行），防止全量解析开销 |
| 信号是补充不是替代 | 对话说"完成了"只触发早期推进尝试，bus 仍需最终确认 |
| 无副作用 | 读 pane 是只读操作，不写 tmux、不发消息 |
| 不引入依赖 | subprocess + re + dataclass，纯 stdlib |

## 验证命令

```bash
cd ~/session-pipeline
# 单元测试
python3 -m pytest tests/test_conversation_monitor.py -x -q

# 语法验证
python3 -m py_compile src/conversation_monitor.py

# 集成验证（需要有活跃 CCS）
python3 -c "
from conversation_monitor import capture_role_pane, parse_signals
p = capture_role_pane('lr')
sig = parse_signals(p, 'lr')
print(f'completed={sig.step_completed} idle={sig.idle_complaint} violation={sig.scope_violation}')
"
```
