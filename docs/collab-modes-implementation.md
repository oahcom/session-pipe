# 步骤级协作声明（guard / wait_roles）

> 更新: 2026-10-22 | 代码: `src/pipeflow/engine.py`（`Step` 字段、加载校验、守护消费）+ `src/pipeflow/step_handler.py`（协作门）
> 原设计稿 `guard_track_design.md` 与收编方案 `collab-modes-to-workflow.md` 已删除（结论已并入本文）。

## 1. 现状（2026-10-22 实测）

`ccs.py --partner/--bus-track` 的四种模式已废弃；引擎侧只剩两种声明字段：

| 能力 | 声明字段 | 引擎行为 | 实现位置 |
|------|---------|---------|---------|
| 守护（主从） | `guard` | 收集被守护角色并保活 | `engine.py:1638 _collect_guard_roles` / `:1678 _ensure_guard_roles` |
| 协作门（仲裁） | `wait_roles` | 等这些角色都产出匹配消息才推进 | `step_handler.py` `_check_exit` |

**`track`（轮次追踪）已不在 `Step` 里**：模板 JSON 中残留的 `track` 键会被加载器按 `Step.__dataclass_fields__` 过滤丢弃（`engine.py:416`），不产生任何行为。

## 2. 声明格式

```json
{
  "id": "s1",
  "type": "handoff",
  "target_role": "engineer",
  "guard": { "watch": ["reviewer", "qa"], "auto_restart": true },
  "wait_roles": ["qa", "reviewer"]
}
```

- `guard.watch`：字符串数组；`auto_restart` 默认 true。
- `wait_roles`：字符串数组；`[]` 语义 = 无门控。
- 模板未显式声明 `guard` 时，引擎从 persona 的 `collab` 字段注入（`engine.py:419-430`，按 persona 目录 mtime 缓存）。`wait_roles` **不**从 persona 注入——协作拓扑必须在模板里显式写。

## 3. 结构校验（加载期）

| 字段 | 非法形态 | 处理 |
|------|---------|------|
| `guard.watch` | 非 list 或含非 str | 丢弃整个 `guard`（`engine.py:432-437`） |
| `wait_roles` | 非 list 或含非 str | 置 None = 无门控（`engine.py:438-441`） |

非法值若放过，消费点会逐字符遍历去拉起不存在的角色。

## 4. 消费逻辑

### 4.1 守护角色集合

`_collect_guard_roles()` 汇总：`assignee`（workflow_instances）+ 当前步 `target_role` + `guard.watch` + `wait_roles`。
`wait_roles` 也在集合内：门控角色不存活 → 门永不过 → 工作流死锁。

- 只守护 `running`/`pending` 的实例；`step_done_ready`（等审批）不拉起——审批期角色状态不归引擎管。
- 过滤内部标识 `workflow_engine` 与非法角色名（`^[a-zA-Z0-9_-]+$`）。
- 单角色拉起异常不中断整轮（`_ensure_guard_roles` 内 try/except）。

### 4.2 协作门

`_check_exit()` 收集匹配消息后取 `have = {m["src"] for m in matched}`；`set(wait_roles).issubset(have)` 不满足则不推进。门控覆盖主 exit 匹配、忙检测扩展窗口、dual_node 超时抢救、track 出口、tmux 信号等路径。

## 5. 生产使用情况

2026-10-22 扫描 28 个模板：**仅 2 个用协作声明，且都只用了 `guard`，没有模板用 `wait_roles`**——`dev_with_collab.json`、`food_chain_loop.json`。

## 6. 测试

- `tests/test_collab_modes.py` + `tests/test_collab_regression.py`：合计 26 个测试（`pytest --collect-only` 实测）。
- 覆盖：guard 收集、wait_roles 门控（全过/缺角色/空 bus/无门）、非法类型、dual_node+wait_roles、角色收集、窗口溢出补查。

```bash
python3 -m pytest tests/test_collab_modes.py tests/test_collab_regression.py -q
PYTHONPATH=src python3 -m pipeflow.engine list   # 确认引擎加载到的模板数
```

## 7. 已知限制

- 守护只覆盖 `running`/`pending` 实例的当前步；等审批的实例角色掉线无人拉起。
- persona 注入只认 `guard`，不认 `wait_roles`。
- 门控依赖角色用**角色名**写 bus（而非 bus_client 默认的 `claude`）作为 `src`；否则门会被单条 claude 消息放行。
