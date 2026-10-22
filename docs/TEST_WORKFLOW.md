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
- 已知缺口：`_has_live_subflow` 只看 `subflow_id` / parent 链接，不看 `rework_ids`。实测 6 天内 reason=角色不响应的取消 **19** 例，其中 1 例的子链活过父取消（`wf_f269a67e6e6/s2`，5.3%，且该子链自身已 cancelled）。**n=19 不足以下结论**（1 SE≈23pp），故不新增判定；要修就复用 `engine._derive_rework_ids` / `_rework_alive`。（分母别用“全部取消”的 249 —— 那是 0.4%，会低估 10 倍。）
