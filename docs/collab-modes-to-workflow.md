# 协作模式收编到 workflow 引擎 — 设计文档

> 状态: 方案定稿 | 日期: 2026-08-08
> 结论: ccs.py 的 --partner/--bus-track 四种模式能力已全部被 pipeflow 引擎覆盖，
> 需收编的是"审计信号"，并修复 tracker 的假死锁噪音。

## 一、现状诊断

### 1.1 ccs.py 四种模式的真实状态

| 模式 | CLI 参数 | 守护线程 | 实际状态 |
|------|---------|---------|---------|
| 主从 | --partner --auto-restart | daemon 线程 | ❌ 随 CLI 退出死亡 |
| 对等 | --bus-track | daemon 线程 | ❌ 随 CLI 退出死亡 |
| 组合 | 两者叠加 | daemon 线程 | ❌ 随 CLI 退出死亡 |
| 仲裁 | 多 partner | daemon 线程 | ❌ 随 CLI 退出死亡 |

根因: `core.py:490` 启动的 `threading.Thread(daemon=True)` 随 CLI 进程退出被杀。
已实测: `last_watchdog_check: 0.0` 持续 60s+ 未更新。

### 1.2 引擎已覆盖的能力（无需新增）

| 四种模式能力 | 引擎实现 | 位置 | 引擎更强处 |
|-------------|---------|------|-----------|
| 主从（守护伙伴） | `_ensure_role_alive` 每 tick 拉起 | role_lifecycle.py:197 | 每 10s vs watchdog 30s |
| 对等（轮次追踪） | `exit_condition` + `_check_exit` | step_handler.py:468 | 精确 bus 消息匹配 |
| 超时防死锁 | timeout_count + 重推 + 升级 | step_handler.py:59-145 | 3 级升级到 coordinator |
| 角色忙检测 | `_is_role_busy` 双信号 | role_lifecycle.py:234 | survival health + DB |
| 组合（返工门禁） | review 拒绝 → rework subflow | wf_lifecycle.py:331 | 自动创建返工链 |
| 仲裁（多角色） | loop + producer_roles + value_gate | wf_lifecycle.py:587 | 价值审计闭环 |

### 1.3 真实缺口（需收编）

1. **伙伴死亡显式告警**: 引擎角色死了会拉起，但无"伙伴死亡"审计事件
2. **轮次超时审计**: 引擎超时升级到 coordinator，但不写 monitor_audit
3. **假死锁噪音**: tracker 拿"分类最后消息年龄"当死锁，无主分类产生永久假死锁

## 二、映射方案

### 2.1 主从 → workflow: review 步骤 + _ensure_role_alive

```json
{
  "id": "s2",
  "type": "review",
  "target_role": "reviewer",
  "exit_condition": {"bus_category": "code_review", "timeout_minutes": 30},
  "rollback_to": "s1"
}
```
引擎自动: 角色死 → 拉起；超时 → 重推 + 升级 coordinator；被拒 → 返工子链。

### 2.2 对等 → exit_condition 轮次追踪

```json
{
  "id": "s1",
  "exit_condition": {"bus_category": "task_spec", "source_contains": "pm", "timeout_minutes": 60}
}
```
引擎自动: 匹配 pm 写的 task_spec → 推进；超时 → 催办。

### 2.3 组合 → 子工作流 + 返工门禁

```json
{
  "id": "s4",
  "type": "handoff",
  "target_role": "engineer",
  "subflow_template": "engineer_feature",
  "exit_condition": {"bus_category": "code_fix"}
}
```
引擎自动: 自动创建子工作流；角色忙 → 排队不抢占。

### 2.4 仲裁 → loop + producer_roles + value_gate

```json
{
  "loop": {
    "enabled": true,
    "max_iterations": 3,
    "producer_roles": ["scout"],
    "value_gate": {"file": "VALUE_AUDIT.md", "workspace": "lr"}
  }
}
```
引擎自动: 价值审计 FAIL → 停止循环；达到迭代 → 重启生产者。

## 三、收编落地清单（最小改动）

### A. 引擎补审计信号（核心）

在 `role_lifecycle.py` 的 `_ensure_role_alive` 补"角色被拉起"事件:

```python
# role_lifecycle.py 拉起角色成功处，追加:
self._bb.write("monitor_audit",
    f"决策: 角色拉起 → {role} CCS 已重启 (workflow 驱动)",
    src="workflow_engine")
```

在 `step_handler.py` 的 timeout 升级处补"轮次超时"审计:

```python
# step_handler.py 升级 coordinator 前，追加:
self._bb.write("monitor_audit",
    f"决策: 轮次超时 → {run.workflow_name}/{step.id} 超时 {timeout}s，已升级 coordinator",
    src="workflow_engine")
```

### B. 修复 tracker 假死锁（去噪音）

`tracker.py:77-79` 增加"分类是否有人负责"校验:

```python
if not latest:
    continue
# 无主分类检测: 该分类最近 N 小时无新消息且无角色消费 → 不是死锁，跳过
```

### C. ccs.py --partner/--bus-track 标记 deprecated

help 文本 + epilog 标注"已由 workflow 引擎取代"。

## 四、验证方案

1. 单测: 引擎拉起角色 → 断言 monitor_audit 有"角色拉起"事件
2. 单测: 步骤超时 → 断言 monitor_audit 有"轮次超时"事件
3. 集成: 跑一个 3 步 workflow（pm→engineer→reviewer），验证完整推进
4. tracker: 无主分类 → 不再报死锁

## 五、影响评估

- 引擎新增 2 处 bus 写（审计信号），不影响推进逻辑
- tracker 加无主检测，去掉假死锁噪音
- ccs.py 仅改文档/help，不影响现有 13 个角色启动
- monitor_audit 从"噪音"变"真实 SLA 看板"
