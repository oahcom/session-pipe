# 工作流存储合并 —— 架构设计（深度版）

> 版本 2.0 | 2026-07-25 | 受众: 架构师

---

## 0. 核心问题定位

### 0.1 双存储不是根本问题——数据丢失才是

JSON/SQLite 双写本身不是问题（SQLite 是权威存储，JSON 是只读缓存）。真正的问题：

**`_tick()` 的中间状态只写 JSON，不写 SQLite。**

`_tick()` 在超时检测中更新三个跟踪字段：

```
timeout_count → 判断是否超过 max_retries
poll_since    → exit_condition 消息扫描起点
last_reminder → 提醒间隔控制
```

这三个字段通过 `_save_run(run)` 写入 JSON 文件，**从不写入 SQLite**。对于通过 `start()` 创建的工作流，JSON 循环每次读取 JSON 文件，能拿到最新的 timeout_count。对于通过 `create_task_v2()` 创建的纯 SQLite 工作流，每次 `_tick()` 从 SQLite 读取 step_results（不包含 timeout_count），temp 对象的修改被丢弃，timeout_count 永远是 0。

### 0.2 波及范围

| 功能 | JSON 工作流 | SQLite 工作流 |
|------|------------|---------------|
| exit_condition 检测 | ✅ 正常 | ✅ 正常（不依赖 timeout_count） |
| 超时提醒 | ✅ 正常 | ❌ 每次重置，从不发送 |
| 超时升级 (coordinator) | ✅ 正常 | ❌ timeout_count 永远 0，不升级 |
| max_retries 终止 | ✅ 正常 | ❌ timeout_count 永远 0，不终止 |
| poll_since 优化 | ✅ 正常 | ❌ 每次从 created_at 开始扫 |
| 自修复触发 (≥3步超时) | ✅ 正常 | ❌ timeout_count 不累积，永不触发 |

所以真正需要合并的**不是存储路径，是状态同步机制**。

---

## 1. 合并方案

### 1.1 核心变更

**`_tick()` 每次更新跟踪字段时，同时写入 SQLite 的 step_results，而不是只写 JSON。**

```python
# 改前：只写 JSON
run.step_results[step.id]["timeout_count"] = timeout_count
self._save_run(run)

# 改后：同时写 SQLite
run.step_results[step.id]["timeout_count"] = timeout_count
self._save_run(run)  # 保留，兼容遗留 JSON
self._sync_step_results(run.id, run.step_results)  # 新增
```

这个 `_sync_step_results` 方法直接 UPDATE `workflow_instances.step_results`。

### 1.2 为什么增加而不是替换

JSON 工作流仍然通过 `_save_run(run)` 维护状态。在**所有**遗留 JSON 工作流完成前，不能删除 JSON 写入。增量方案：在每个写入点追加 SQLite 同步，两个路径都更新。

---

## 2. 状态同步设计

### 2.1 新增方法

```python
def _sync_step_results(self, wf_id: str, step_results: dict):
    """将 _tick 的中间状态同步到 SQLite。仅更新 step_results 列，不碰状态机。"""
    try:
        conn = self._lifecycle._conn
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
            (json.dumps(step_results, ensure_ascii=False), wf_id)
        )
        conn.commit()
    except Exception:
        conn.rollback()
```

### 2.2 插入点

`_tick()` 中所有 `_save_run(run)` 调用后追加 `_sync_step_results(run.id, run.step_results)`：

| 位置 | 修改的字段 | 用途 |
|------|-----------|------|
| complete_step 后 | status=done, ts | 步骤完成标记 |
| complete_failed 后 | status=complete_failed, error | LM 调用失败 |
| poll_since 更新 | poll_since | exit_condition 扫描起点 |
| last_reminder 更新 | last_reminder | 提醒间隔 |
| timeout_count 更新 | timeout_count, status, failed_at | 超时追踪 |
| 自修复时间 | last_heal | 异常检测 |

### 2.3 效果

改后 SQLite 的 step_results 将包含：

```json
{
  "s1": {
    "status": "notified",
    "notified_at": 1712345678,
    "poll_since": 1712345700,
    "timeout_count": 2,
    "last_reminder": 1712345750
  }
}
```

下次 `run_once()` 的 SQLite 循环加载这个 step_results 时，`timeout_count=2` 被保留。连续超时检测正常工作。

---

## 3. 未来清理

### 3.1 JSON 文件处境

JSON 文件不再是"状态存储"，变成"仅用于判断工作流来源的标记"。功能上完全可废弃。

### 3.2 清理时机

条件：`runs/` 目录下没有 running 状态的 JSON 工作流。达到后：

1. 删除 JSON 循环（`run_once()` 的 `for run_file in self.runs_dir.glob("*.json"):`）
2. 删除 `_save_run()`、`_load_run()`、`_load_run_data()`
3. 删除 `_advance()`（其职责已由 `_advance_production_wf` + `LM._advance_unsafe` 覆盖）
4. 删除 `WorkflowRun` 类
5. 删除 `runs_dir` 初始化

### 3.3 兼容检测

```python
# 在引擎启动时检测是否有遗留 JSON 工作流
_legacy = list(self.runs_dir.glob("*.json"))
if _legacy:
    print(f"  [wf] 发现 {len(_legacy)} 个 JSON 工作流，兼容模式运行中")
    # 激活 JSON 循环
```

---

## 4. 风险矩阵

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| `_sync_step_results` 与 `LM.complete_step` 并发写 | 低 | step_results 覆盖 | BEGIN IMMEDIATE + 单线程 run_once |
| JSON 循环与 SQLite 循环处理同一工作流 | 低 | 两次推进 | JSON 循环仅在 LM.complete_step 幂等保护下运行 |
| timeout_count 从 0 跃升 | 低 | 误触发 max_retries | 每次 +1 递增，不重置 |
| 旧 JSON 文件在 `_sync_step_results` 后不被写入 | 中 | JSON 文件过时 | JSON 循环仍运行，_save_run 仍在 |

---

## 5. 涉及代码

| 文件 | 改动 |
|------|------|
| `engine.py` | + `_sync_step_results()` 方法 |
| `engine.py` | `_tick()` 中 6 个 `_save_run` 后追加 `_sync_step_results` |
| `engine.py` | `start()` 简化（可选，不阻塞 timeout 修复） |
