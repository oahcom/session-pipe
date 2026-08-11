# 四种协作模式在 Workflow 引擎中的实现 — 完整指南

> 状态: 实现完成 | 日期: 2026-08-11
> 覆盖: Step 声明字段 / 引擎消费逻辑 / 测试 / 模板示例 / 故障模式与修复
> 代码: `src/pipeflow/engine.py` + `src/pipeflow/step_handler.py`

## 一、概述

四种协作模式（主从/对等/组合/仲裁）已从 ccs.py 的 CLI 参数（`--partner`/`--bus-track`，
已废弃）收编为 **workflow 模板步骤的可声明字段**。引擎在每 tick 消费这些声明，
实现多角色协作语义。

| 模式 | 声明字段 | 引擎行为 | 对应实现 |
|------|---------|---------|---------|
| 主从 | `guard` | 守护角色存活，死了拉起 | `_collect_guard_roles` + `_ensure_guard_roles` |
| 对等 | `track` | 轮次追踪，超时告警，节流 | `_tick` track 段 |
| 组合 | `guard` + `track` | 两者叠加 | 同上 |
| 仲裁 | `wait_roles` | 等所有声明角色产出匹配消息才推进 | `_check_exit` 协作门 |

## 二、Step 声明字段

`engine.py` L64-73：

```python
@dataclass
class Step:
    ...
    guard: dict = None      # 主从/组合: {"watch": ["role_a"], "auto_restart": true}
    track: dict = None      # 对等/组合: {"bus_category": "cat", "timeout_sec": 300,
                            #             "notify_roles": ["role_c"]}
    wait_roles: list = None # 仲裁: ["qa", "reviewer"] — 等这些角色都产出匹配消息才推进
```

### 2.1 persona collab 注入

模板未显式声明 guard/track 时，引擎从
`hermes-session-roles/personas/session-roles/persona_<role>.json` 的
`collab` 字段注入（按目录 mtime 缓存）。声明一次全模板生效。

```json
// persona_qa.json
{
  "name": "qa",
  "collab": {
    "guard": {"watch": ["reviewer"], "auto_restart": true},
    "track": {"bus_category": "code_fix", "timeout_sec": 300, "notify_roles": ["pm"]}
  }
}
```

### 2.2 结构校验（`engine.py` L218-222）

非法类型在加载时丢弃并告警，防止消费时逐字符遍历拉起不存在角色：

| 字段 | 非法类型 | 处理 |
|------|---------|------|
| `guard.watch` | 非 list 或含非 str | 丢弃整个 guard |
| `track.bus_category` | 非 str / 空 | 丢弃整个 track |
| `track.notify_roles` | 非 list 或含非 str | 丢弃整个 track |
| `wait_roles` | 非 list 或含非 str | 置 None（无门控） |

## 三、引擎消费逻辑

### 3.1 主从 — guard

`_collect_guard_roles`（`engine.py` L636）收集活跃工作流的守护角色：

```
assignee（workflow_instances）+ target_role + guard.watch + wait_roles
```

`_ensure_guard_roles` 对每个角色调 `_ensure_role_alive` 拉起。
wait_roles 角色也收集（防止协作门角色死亡 → 门永远不通过）。

### 3.2 对等 — track

`_tick` track 段（`step_handler.py` L560+）：
- 每次 tick 查询 `track.bus_category` 分类最新消息
- 最新消息作者是 `target_role` → 跳过告警（角色刚写过，不卡死）
- 消息年龄 > `timeout_sec` → 通知 `notify_roles`（节流：`track_notified_at` 防重复）
- 伙伴恢复（新消息到达）→ 停止告警

### 3.3 仲裁 — wait_roles 协作门

`_check_exit`（`step_handler.py` L867+）：
- 匹配消息收集后，`have = {m["src"] for m in matched}`
- `set(wait_roles).issubset(have)` 不满足 → 返回 `(0.0, [])` 不推进
- 满足 → 返回匹配消息，调用方 complete_step

**门控覆盖所有完成路径**：

| 路径 | wait_roles 传递 | 说明 |
|------|----------------|------|
| 主 exit 匹配（dual_node + 非 dual_node） | ✅ L439/442 | 正常路径 |
| 忙检测扩展窗口 | ✅ L170 | 角色忙重试 |
| dual_node 超时抢救 | ✅ L264 | 抢救不绕过门 |
| track 出口 | ✅ L611 | 轮次追踪后 exit |
| tmux 信号 | ✅ L100-104 | 信号不绕过门 |
| integrity 回复 | ❌ L388 | 1:1 协议，非协作门场景 |
| relaxed-advance | ✅ L294 排除 | wait_roles 步骤不走宽松推进 |

## 四、模板示例

`~/.hermes/workflows/dev_with_collab.json`：

```json
{
  "name": "dev_with_collab",
  "steps": [
    {
      "id": "s1", "type": "handoff", "target_role": "engineer",
      "guard": {"watch": ["reviewer", "qa"], "auto_restart": true}
    },
    {
      "id": "s2", "type": "review", "target_role": "reviewer"
    },
    {
      "id": "s3", "type": "handoff", "target_role": "qa"
    },
    {
      "id": "s4", "type": "review", "target_role": "closer",
      "wait_roles": ["qa", "reviewer"]
    }
  ]
}
```

s4（仲裁）：closer 审计，但必须 qa 和 reviewer 都产出 test_report 才推进。

## 五、故障模式与修复（codex 审查记录）

7 轮 codex CLI 审查发现并修复 10 个问题。关键 4 个：

| # | 问题 | 修复 | commit |
|---|------|------|--------|
| 1 | dual_node 超时抢救绕过 wait_roles 门 | L264 补传 wait_roles | eeec7db |
| 2 | `_collect_guard_roles` 未收集 wait_roles 角色（门永不过） | L654-656 补充收集 | eeec7db |
| 3 | source_contains + wait_roles 死锁（src 过滤拦死协作消息） | L886-888 协作角色放行 | eeec7db |
| 4 | 高流量分类 50 条窗口滑出协作消息（门永不过） | L910-934 按角色滚动补查 limit=200 | eeec7db |
| 5 | claude-src 消息一票满足所有缺失角色（门被绕过） | L917 精确角色匹配（不放宽 claude） | 8ffb9bc |

## 六、测试

`tests/test_collab_modes.py` — 14 个测试：主从 guard 收集、对等 track 超时、
组合 guard+track、仲裁 multi-guard + wait_roles 校验（string/int/dict/list/empty）。

`tests/test_collab_regression.py` — 21 个测试：wait_roles 门控（全过/缺角色/空 bus/无门）、
track 作者过滤/节流/伙伴恢复、guard 非法类型、dual_node+wait_roles 门（受阻/放行）、
角色收集、窗口溢出补查（命中/缺失）、source_contains 共存。

全量: **645 passed**（无 skip/fail）。

## 七、验证与部署

```bash
cd ~/session-pipeline
# 全量测试
timeout 180 python3 -m pytest tests/ -q

# 引擎加载生产模板（确认无非法 wait_roles）
PYTHONPATH=src python3 -c "
from pipeflow.engine import WorkflowEngine
eng = WorkflowEngine()
print(len(eng._workflows), 'templates loaded')
"

# 模板质量检查
PYTHONPATH=src python3 -c "
from template_registry import _validate_meta
import json
d = json.load(open('~/.hermes/workflows/dev_with_collab.json'))
print(_validate_meta(d).passed)
"
```

## 八、已知限制（ponytail:）

- 补查 limit=200：极端流量（>200 条/分类/查）仍可能滑出；升级路径：按角色+时间戳游标
- `wait_roles: []`：空列表语义 = 无门控（等价未声明）
- src 精确匹配依赖协作角色写 bus 时用角色名（而非 bus_client 默认 claude）；否则门被 claude-src 一票绕过（8ffb9bc 已修，但角色侧需规范）

## 九、参考

- 旧设计草案: `docs/guard_track_design.md`（8-10 定案, 4 轮 CLEAN, 608 测试）
- 旧收编方案: `docs/collab-modes-to-workflow.md`（8-08 方案定稿）
- 本实现基于上述两稿新增 wait_roles 协作门、窗口补查、死锁修复。
## 十、协作模式入门指南（补充）

生产 25 模板中仅 2 个使用协作模式（food_chain_loop: guard+track, dev_with_collab: guard+wait_roles）。
采用率低的根因是**作者不知道怎么写**，不是能力缺失。以下是从简单到复杂的模板写法：

### 10.1 最小示例：单步骤守护（主从）

```json
{
  "name": "hello_collab",
  "steps": [{
    "id": "s1", "type": "handoff", "target_role": "engineer",
    "guard": {"watch": ["reviewer"], "auto_restart": true},
    "prompt_template": "## 目标\n...\n\n## 完成\n写 bus cat=code_fix，标题含 s1-complete。",
    "exit_condition": {"bus_category": "code_fix", "timeout_minutes": 60}
  }]
}
```

效果：engineer 工作时 reviewer 保持存活，死了自动拉起。

### 10.2 对等追踪：步骤超时提醒（对等）

```json
{
  "id": "s2", "type": "handoff", "target_role": "engineer",
  "track": {"bus_category": "code_fix", "timeout_sec": 300, "notify_roles": ["pm"]},
  "prompt_template": "## 目标\n...\n\n## 完成\n写 bus cat=code_fix，标题含 s1-complete。",
  "exit_condition": {"bus_category": "code_fix", "timeout_minutes": 60}
}
```

效果：5 分钟无产出 → 通知 pm 催办；角色刚写过 → 不重复打扰。

### 10.3 仲裁门：等多人产出才推进（仲裁）

```json
{
  "id": "s4", "type": "review", "target_role": "closer",
  "wait_roles": ["qa", "reviewer"],
  "prompt_template": "## 目标\n审计 qa 和 reviewer 的产出...\n\n## 完成\n写 bus cat=workflow，标题含 s4-complete。",
  "exit_condition": {"bus_category": "workflow", "timeout_minutes": 60}
}
```

效果：qa 和 reviewer 都产出 test_report 分类消息后，closer 的步骤才推进。

### 10.4 组合：守护 + 追踪 + 门控

```json
{
  "id": "s3", "type": "handoff", "target_role": "qa",
  "guard": {"watch": ["reviewer"], "auto_restart": true},
  "track": {"bus_category": "test_report", "timeout_sec": 300, "notify_roles": ["pm"]},
  "wait_roles": ["reviewer"],  // reviewer 产出 review 后才推进
  "prompt_template": "## 目标\n...\n\n## 完成\n写 bus cat=test_report，标题含 s3-complete。",
  "exit_condition": {"bus_category": "test_report", "timeout_minutes": 60}
}
```

### 10.5 persona 注入（声明一次全模板生效）

```json
// persona_qaf.json — 在角色 persona 里声明
{
  "name": "qa",
  "collab": {
    "guard": {"watch": ["reviewer"], "auto_restart": true},
    "track": {"bus_category": "code_fix", "timeout_sec": 300, "notify_roles": ["pm"]}
  }
}
```

模板里就不用重复写 guard/track（引擎自动注入）。wait_roles 是步骤级声明，
**不会**从 persona 注入（协作拓扑必须在模板里显式声明）。

