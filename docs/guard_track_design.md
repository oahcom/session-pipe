# 协作模式收编到 workflow 步骤 — 架构设计草案

> 状态: 定案（4 轮 codex 审查连续 CLEAN，608 测试通过）| 日期: 2026-08-10
> 目标: 让四种模式成为 workflow 步骤可声明的能力，替代 ccs.py --partner/--bus-track

## 1. 问题背景

### 旧模式（已废弃）
- `ccs.py start verifier --partner rebutter --auto-restart`：verifier CCS 守护 rebutter CCS 存活
- `ccs.py start pro --bus-track debate`：pro CCS 追踪 debate 分类轮次
- **本质**：CCS 进程间的守护/追踪拓扑，需要长驻线程（watchdog/tracker）

### 现状
- Step dataclass 无 `guard`/`track` 字段
- 模板无法声明协作需求
- 引擎无消费逻辑
- ccs.py 参数已标注 deprecated，但**无替代实现**

## 2. 设计方案

### 2.1 Step 新增字段

```python
@dataclass
class Step:
    ...
    guard: dict = None  # CCS 守护声明
    track: dict = None  # 轮次追踪声明
```

**guard 字段结构**：
```json
{
  "guard": {
    "watch": ["engineer", "qa"],      // 被守护的角色列表（CCS 实体）
    "auto_restart": true              // 挂了自动重启
  }
}
```

**track 字段结构**：
```json
{
  "track": {
    "bus_category": "code_review",    // 追踪的 bus 分类
    "timeout_sec": 300                // 超时秒数，默认 300
  }
}
```

### 2.2 模板使用示例

**主从模式**（替代 `--partner --auto-restart`）：
```json
{
  "id": "s2",
  "target_role": "reviewer",
  "guard": {"watch": ["engineer", "qa"], "auto_restart": true}
}
```

**对等模式**（替代 `--bus-track`）：
```json
{
  "id": "s1",
  "target_role": "pm",
  "track": {"bus_category": "task_spec", "timeout_sec": 300}
}
```

**组合模式**：
```json
{
  "id": "s2",
  "target_role": "reviewer",
  "guard": {"watch": ["engineer"], "auto_restart": true},
  "track": {"bus_category": "code_review", "timeout_sec": 300}
}
```

**仲裁模式**：workflow 顶层 `loop` 已支持，配合 producer_roles + value_gate。

### 2.3 引擎消费逻辑

**位置**：`WorkflowEngine.run_once()` tick 循环里，步骤执行期间持续生效。

#### guard 消费（主从模式）
```python
# run_once 每 tick（约 5s）执行
if step.guard:
    for role in step.guard.get("watch", []):
        self._ensure_role_alive(role)  # 角色死 → 自动拉起
```

- 每 5s tick 检查一次被守护角色 CCS 存活
- 角色死 → `_ensure_role_alive(role)` 自动拉起
- 这**替代了 watchdog 线程**，且间隔 5s（优于旧 30s）

#### track 消费（对等模式）
```python
# _tick 内，已经有 exit_condition 检查
if step.track:
    cat = step.track.get("bus_category")
    timeout = step.track.get("timeout_sec", 300)
    # 检查 bus 消息年龄
    age = time.time() - last_msg_ts
    if age > timeout:
        # 写 monitor_audit 审计
        self._bb.write("monitor_audit", f"轮次超时: {cat} 超时 {age}s")
        # 提醒伙伴（旧 tracker 的“伙伴活但 bus 停 → 提醒伙伴”）
        self._send_to_role(partner, f"[deadlock] {cat} 超时 {age}s，请继续")
```

- 复用已有 `_check_exit` / `exit_condition` 基础设施
- 超时写 `monitor_audit` + 直接提醒伙伴（保留旧 tracker 的“直接提醒伙伴”语义）
- 不需要单独 tracker 线程

### 2.4 生命周期

- **启用时机**：步骤进入 `notified` 状态（已派发给 target_role）
- **停用时机**：步骤完成（`completed`/`step_done_ready`）或 workflow 结束
- **作用域**：仅当前步骤有效，步骤结束自动关闭（不会跨步骤残留）

## 3. 实现改动点

### 文件：`src/pipeflow/engine.py`

| 位置 | 改动 |
|------|------|
| `Step` dataclass | + `guard: dict = None` / `track: dict = None` |
| `run_once` tick 循环 | 每 tick 检查当前步骤的 `guard`，调用 `_ensure_role_alive` |
| `_tick` | 步骤 `track` 时，检查 bus 消息年龄，超时写 audit + 提醒 |

### 文件：`src/pipeflow/template_registry.py`

| 位置 | 改动 |
|------|------|
| `_validate_meta` | 校验 `guard`/`track` 字段结构 |

### 文件：模板（`~/.hermes/workflows/*.json`）

| 示例 | 场景 |
|------|------|
| `food_chain_loop` s2 | `guard: {watch: [engineer], auto_restart: true}` |
| `dev_implement` s2 | `guard: {watch: [engineer], auto_restart: true}` + `track: {bus_category: code_review}` |

## 4. 验收标准

| 场景 | 预期行为 |
|------|---------|
| 模板声明 `guard: {watch: ["engineer"]}` | 引擎每 5s 调用 `_ensure_role_alive("engineer")`，engineer CCS 死即拉起 |
| 模板声明 `track: {bus_category: "code_review", timeout_sec: 300}` | code_review 消息 300s 无更新 → 写 monitor_audit + 发提醒 |
| 步骤完成 | guard/track 自动停用，不再消耗 tick 资源 |
| food_chain_loop s2 加 `guard` | 实际跑通，engineer 死即拉起，reviewer 能推进 |

## 5. 兼容性

- 字段可选（默认 None），现有模板无需修改
- `ccs.py --partner/--bus-track` 保留兼容（文档标注 deprecated）
- 不影响现有 `exit_condition`/`type`/`subflow_template` 逻辑

## 6. 风险点（需架构师确认）

1. **守护作用域**：step.guard 仅当前步骤生效，步骤结束即停。这是否符合"长期守护"语义？（旧 watchdog 是长驻、跨工作流）
2. **track 的提醒伙伴**：`_send_to_role(partner, ...)` 需要明确是谁（从 guard.watch 推导？还是 track 单独声明？）
3. **性能**：每 tick 5s 检查所有活跃步骤的 guard，在有大量并发工作流时是否会开销过大？

---

**请架构师审查后回复：批准 / 需修改 / 需补充细节**