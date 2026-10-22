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
<<<<<<< HEAD
=======

`tests/conftest.py` 已把 `src/` 加入 `sys.path`，无需 `PYTHONPATH`；直接跑 `python3 src/...` 才需要。

## 实测基线（2026-10-22 05:40）

| 命令 | 结果 |
|------|------|
| `--collect-only` | `collected` 数 + `0 error`（条数不写死：并发改动每天在变） |
| `pytest tests/ -q` | **全 passed**（条数不写死，判据见下） |

> 表里的结果只是某个时刻的快照，别当门禁。判定"我的改动有没有引入回归"先跑
> `--collect-only`（秒级，能抓到导入断裂），再跑全量并只对齐失败集合。
> 收集错误通常来自**未提交的 src 改动与测试脱节**——先看 import 名对不对，别急着改测试。

## 测试文件规模

`find tests -name 'test_*.py' | wc -l` → 125 个文件。按主题分布（命名即职责，不再逐文件列表）：

| 主题 | 代表文件 |
|------|----------|
| 引擎推进/退出 | `test_engine_e2e.py` `test_exit_condition.py` `test_tick_paths.py` |
| 审批/驳回/返工 | `test_approval_channel_retired.py` `test_confirm_not_re_rejected.py` `test_r118_completion_integrity.py` |
| 协作模式 | `test_collab_modes.py` `test_collab_regression.py`（合计 26 tests） |
| 路由 | `test_router.py` `test_routes_unit.py` `test_consistency.py` |
| 投递/存活 | `test_send_to_role_timeout.py` `test_liveness.py` `test_survival_monitor_unit.py` |
| 模板/契约 | `test_contract_updater_*.py` `test_container_contract_satisfiable.py` |

## 已知缺口

- `_cleanup_stale_workflows` 条件 3/4 已有专门回归：`test_stale_reclaim_pane_gate.py`（pane 活体、全局阈值双预算、子链与返工链活体、主查询必带 assignee）。
- tmux 空闲取消是**两道门**，各有回归：`_has_live_subflow`（`test_tick_paths.py` 的 4 个 idle 用例，覆盖活子链/子链已死/无子链/parent 链接回退）+ `_role_producing_recently`（`test_idle_cancel_blocked_when_role_still_producing`、`test_idle_cancel_noop_logs_debug_not_info`）。
- 已知缺口：`_has_live_subflow` 只看 `subflow_id` / parent 链接，不看 `rework_ids`。**实测未触发**：6 天内 249 个"角色不响应"取消实例里，只有 1 个的子链活过父取消（`wf_f269a67e6e6/s2`，且该子链自身已 cancelled）= 0.4%。要修就复用 `engine._derive_rework_ids` / `_rework_alive`，别新写判定。
- 手工验证 `SurvivalMonitor().tick()` 会写生产 bus/哨兵，必须按 [CLAUDE.md](../CLAUDE.md) 三重 env 隔离后再跑。
>>>>>>> 3242a64 (docs+test: 补 cancel_step 契约 / 空闲取消两道门口径 / CHANGES 条目 / 修掉 4 个 idle 用例的隐式依赖)
