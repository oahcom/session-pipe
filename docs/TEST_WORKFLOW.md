# 测试工作流文档

> 更新: 2026-07-24 | 受众: 测试工程师 | 相关: [DOCS.md](DOCS.md)

## 如何运行全部测试

```bash
# session-pipeline（1241 例，2026-10-22 collect 实测）
cd /home/administrator/session-pipeline
python3 -m pytest tests/ -v

# hermes-session-roles（43 pass，2026-10-22 实测）
cd /home/administrator/hermes-session-roles
python3 -m pytest tests/ -v

# session-launcher（459 pass，2026-10-22 实测）
cd /home/administrator/session-launcher
python3 -m pytest tests/ -v
```

## 已知缺口

- tmux 空闲取消是**两道门**，各有回归：`_has_live_subflow`（`test_tick_paths.py` 的 4 个 idle 用例，覆盖活子链/子链已死/无子链/parent 链接回退）+ `_role_producing_recently`（`test_idle_cancel_blocked_when_role_still_producing`、`test_idle_cancel_noop_logs_debug_not_info`）。
- 已知缺口：`_has_live_subflow` 只看 `subflow_id` / parent 链接，不看 `rework_ids`。**按守卫引入时间切窗实测**（守卫 `421f657` 是 10-19 17:45 加的）：引入**前** 10 例 reason 含「不响应」的取消里有 1 例子链活过父取消（`wf_f269a67e6e6/s2`，10-17，子链 `wf_sub_4af5167d8b89` 直到 22:26 才结束 = 父取消后 55 分钟，10%）；引入**后** 9 例里 0 例。故该缺口目前**无守卫引入后的证据**，n=9 也偏小，本轮不加判定。要修就复用 `engine._derive_rework_ids` / `_rework_alive`。
  ⚠️ 测这类缺口必须**按守卫引入时间切窗**：不切窗会把守卫修好之前的案例算成守卫的缺口（我前两版就是这么错的）。
