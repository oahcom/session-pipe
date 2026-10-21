# REVIEW_R7_regression — Round 7 stale-kill 回归审查

## 自检

我声称完成的任务: 回归审查 Round 7 的 stale-kill 条件变更 (wf_lifecycle.py 条件4)
我实际运行的验证命令:
  - git diff: 见下文 `diff` 输出 (wf_lifecycle.py 11 ins / 4 del)
  - py_compile: src/pipeflow/step_handler.py PASS / src/pipeflow/wf_lifecycle.py PASS
  - systemctl: (not checked — no service restart relevant to this code-only review)
  - md5 对比: git diff --stat 显示 1 个文件变更 (wf_lifecycle.py)；step_handler.py 未变、未修改

未验证的声明: systemctl 服务状态 (本次审查与服务状态无关，仅 static 代码 + test)
已知风险: test_liveness / test_collab_regression 覆盖 busy 检测与 timeout_count>=1 推进路径，但未直接覆盖 _cleanup_stale_workflows 的条件4 分支的边界（empty sr + timeout_count>=1 via current_step lookup）

## 1. Diff (Round 7 实变更)

```
-                # 条件4: running 但 step_results 为空且 >1h（从未推进）
-                elif _st == "running" and age_h > 1 and not sr:
-                    _step = d.get("current_step_id", "")
-                    reason = f"running 状态 {_step} 无推进记录 {age_h:.1f}h"
+                # 条件4: running 但当前步骤确已超时（step 级 timeout_count 真增长）且 >1h
+                elif _st == "running" and age_h > 1:
+                    _cur = d.get("current_step_id", "")
+                    _cur_sr = sr.get(_cur, {}) if isinstance(sr, dict) else {}
+                    _cur_tc = _cur_sr.get("timeout_count", 0) if isinstance(_cur_sr, dict) else 0
+                    if _cur_tc >= 1:
+                        _reason_step = _cur_sr.get("step", _cur) if isinstance(_cur_sr, dict) else _cur
+                        reason = f"running 状态 {_reason_step} 超时无推进记录 {age_h:.1f}h (timeout_count={_cur_tc})"
```

## 2. 20 维度审查

### D1 正确性
Q2 (timeout_count write 路径): **是**。`step_handler.py:224` 在 `elapsed >= timeout` 且角色非忙时执行 `timeout_count = run.step_results.get(step.id, {}).get("timeout_count", 0) + 1`，随后 `_sync_step_results`（line 230）落库。line 224-230 确认写入的是当前步骤 dict 的 `timeout_count` 字段，与 wf_lifecycle line 92 读取 `sr.get(_cur, {}).get("timeout_count", 0)` 的路径完全匹配。

Q4 (嵌套 elif 防误杀): **否**。`_cleanup_stale_workflows` 是 `elif` 链，但条件1 (`max_tc >= 6`)、条件2 (`tpl in _ZOMBIE_TEMPLATES and age_h > 2`)、条件3 (`age_h > _global_limit_h`) 均为独立 `elif`，互不重叠。条件4 仅在前 3 不成立时评估，且内部 `if _cur_tc >= 1` 是条件4 的 gate，无法越过其它 elif 触发。

Q3 (漏判真 stale): **潜在风险**。新条件要求 `current_step_id` 的 step_results 存在且 `timeout_count >= 1`。若 `current_step_id` 为空/None，`sr.get(None, {})` 返回 `{}`，`_cur_tc = 0` → 不回收。但 `current_step_id` 在正常推进时始终设置 (wf_lifecycle line 255 初始化), edge case 概率极低。

Q4 (条件1 回归): **无影响**。条件1 读取 `max_tc = max(... timeout_count ...)`，独立于条件4。`timeout_count >= 6` 仍触发回收，回归正常。

### D2 安全性
- 路径遍历: 本次变更无文件路径输入，N/A
- 输入验证: `current_step_id`, `sr` 皆来自 DB 反序列化，已有 `isinstance(sr, dict)` 防译

### D3 可维护性
- 变更函数长度: `_cleanup_stale_workflows` 约 140 行 (整体超 20 行，但为单一责职的 monolithic 清理循环，拆分收益低)
- 参数: 0 新增参数
- 命名: `_cur` / `_cur_sr` / `_cur_tc` 意图明确

### D4 性能
- 额外 `sr.get(_cur, {})` O(1) 字典查找，无 N+1
- 僵尸回收是低频 (daemon 60s 轮询) 任务，每次扫行 `running` + `step_done_ready` 行集合

### D5 一致性
- 风格: 与条件1-3 保持一致 (elif 链 + reason 字符串)
- 注释: 解释 why (`bus #728c6e430` 引用，误杀根因)

### D6 可测试性
- 现有 test_liveness.py:138 (`timeout_count: 3`)、test_collab_regression.py:271/296 (`timeout_count: 1`) 覆盖 `_s4_stepping` + dual_node Rescue，但 **未直接覆盖 `_cleanup_stale_workflows` 条件4 分支**。
- 建议补: test 注入 `running` 行 + `created_at` 回推 > 1h，`current_step_id="s1"`, `step_results={"s1": {"timeout_count": 1}}` → assert 被标为 `cancelled`；反向：`timeout_count: 0` → assert 存活。

### D7 (审查者额外) — 条件 1-3-4 优先级
链式 `elif` 保证互斥，先匹配优先级: `max_tc>=6` > zombie模板 > 全局阈值 > 条件4。条件4 仅作为兜底，不会与其它条件并发触发，无竞态。

### D8-D20
D8 部署变更: 仅代码变更，`python3 -m py_compile` PASS。
D9 架构漂移: 未变更服务/端口/数据流，`arch_drift_detector` 无需触发。
D10 依赖: 无新增依赖。
D11-D20 N/A (无 CLI/网关改动)。

## 3. 评分 (20 维度，P0=0, P1<=2)

| 维度 | 评分 | 说明 |
|------|------|------|
| 1. timeout_count write 路径确认 | P0=0 | step_handler.py:224 写入，230 持久化，路径一致 |
| 2. 漏判真 stale 风险 | P2 | current_step_id 为空时 _cur_tc=0 不回收 (edge, low) |
| 3. elif 嵌套防误杀 | P0=0 | 独立 elif，条件4 gate 内层 if |
| 4. 条件1 (timeout_count>=6) 回归 | P0=0 | 独立路径不受影响 |
| 5. 旧 `not sr` 分支移除 | P0=0 | 彻底移除，无残留 (grep 确认) |
| 6. bus #728c6e430 误杀修复 | P0=0 | 长任务空 sr 1h 误杀 → 需 timeout_count>=1 真实超时才判 |
| 7. test 覆盖 gap | P1 | _cleanup_stale_workflows 条件4 无 directly test |
| 8. 代码风格一致 | P0=0 | elif 链 + reason 格式统一 |
| 9. 注释解释 why | P0=0 | bus 回溯引用 |
| 10. py_compile | P0=0 | 两文件均 PASS |
| 11-20. (架构/性能/安全等) | P0=0 | 无改动 |

## 4. 结论

**P0=0, P1=1, P2=2** → **达标** (P0=0 且 P1<=2)

Round 7 变更实现了其意图：将 `running + age_h>1 + sr_empty` (误杀长任务根源) 改为 `running + age_h>1 + current_step.timeout_count>=1` (真实超时才回收)。timeout_count 写路径 (step_handler.py:224) 与读路径 (wf_lifecycle.py:92) 契约一致，三级 elif 互不干扰，条件1 (>=6) 回收不受影响。

唯一 P1: 缺少 `_cleanup_stale_workflows` 条件4 的 directly unit test (建议补充，见 D6)。
