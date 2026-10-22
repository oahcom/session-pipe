# 测试工作流文档

> 更新: 2026-10-22 | 受众: 测试工程师 | 相关: [DOCS.md](DOCS.md)

## 运行

```bash
# 判据是 0 error + 全 passed；条数不写死（并发改动每天在变），别把某个数字当门禁。
cd /home/administrator/session-pipeline      && python3 -m pytest tests/ -q
cd /home/administrator/hermes-session-roles  && python3 -m pytest tests/ -q
cd /home/administrator/session-launcher      && python3 -m pytest tests/ -q

cd /home/administrator/session-pipeline && python3 -m pytest tests/ -q --collect-only  # 秒级，验证导入是否坏
cd /home/administrator/session-pipeline && python3 -m pytest tests/test_engine_e2e.py -q
```

`tests/conftest.py` 已把 `src/` 加入 `sys.path`，无需 `PYTHONPATH`；直接跑 `python3 src/...` 才需要。

## 隔离契约（conftest 自动桩）

默认桩掉 4 个会**对外界产生副作用**的入口，否则套件跑不完也跑不干净：

| 夹具 | 桩掉 | 不桩会怎样 |
|------|------|------------|
| `_no_guard_role_start` | `WorkflowEngine._ensure_guard_roles` → `set()` | tick 里对不存在的 `test_role` 真跑 `ccs start`（60s+），全量卡在第 29 例 |
| `_fast_role_ready` | `_wait_for_role_ready` → `True` | 每次派单等 90s（`ccs start` + 10×1s） |
| `_no_live_role_restart` | `_ensure_role_alive` → `True` | kill-session + 30×1s 重启循环，撞穿用例自带 `signal.alarm` |
| `_no_live_approval_notify` | `LifecycleManager._notify_result` → `None` | 测试夹具 id 被真 `ccs send` 投进活体角色上下文 |

要测的**就是**这一步时，把模块名加进对应 allowlist（`_REAL_GUARD_MODULES` / `_REAL_READY_MODULES` /
`_REAL_ENSURE_ALIVE_MODULES`）—— 类级桩撤不掉，只能整模块豁免；instance 级 `del` 对 class 桩无效。
自定义 patch 必须 `try/finally` 恢复：泄漏的全局 `subprocess.run` 桩会让 `eval_checker` 的
`stat -c %W` 拿到空串而 fail-open，症状是**其它文件**的用例转红。

带 `signal.alarm(N)` 的用例由 conftest 的 handler 转成一条红用例；没有 handler 时默认动作是杀进程
（`EXIT=142`、无 summary）。套件疑似卡死时用
`pytest tests/ -q --timeout=25 --timeout-method=thread` 抓栈，别靠猜。

## 实测基线（2026-10-22 05:40）

| 命令 | 结果 |
|------|------|
| `--collect-only` | `collected` 数 + `0 error`（条数不写死：并发改动每天在变） |
| `pytest tests/ -q` | **全 passed**（条数不写死，判据见下） |

> 表里的结果只是某个时刻的快照，别当门禁。判定"我的改动有没有引入回归"先跑
> `--collect-only`（秒级，能抓到导入断裂），再跑全量并只对齐失败集合。
> 收集错误通常来自**未提交的 src 改动与测试脱节**——先看 import 名对不对，别急着改测试。

## 测试文件规模

`find tests -name 'test_*.py' | wc -l` → 用这条命令现取文件数（别抄数字）。按主题分布（命名即职责，不再逐文件列表）：

| 主题 | 代表文件 |
|------|----------|
| 引擎推进/退出 | `test_engine_e2e.py` `test_exit_condition.py` `test_tick_paths.py` |
| 审批/驳回/返工 | `test_approval_channel_retired.py` `test_confirm_not_re_rejected.py` `test_r118_completion_integrity.py` |
| 协作模式 | `test_collab_modes.py` `test_collab_regression.py`（条数不写死，跑起来看） |
| 路由 | `test_router.py` `test_routes_unit.py` `test_consistency.py` |
| 投递/存活 | `test_send_to_role_timeout.py` `test_liveness.py` `test_survival_monitor_unit.py` |
| 模板/契约 | `test_contract_updater_*.py` `test_container_contract_satisfiable.py` |

## 已知缺口

- `_cleanup_stale_workflows` 条件 3/4 已有专门回归：`test_stale_reclaim_pane_gate.py`（pane 活体、全局阈值双预算、子链与返工链活体、主查询必带 assignee）。
- tmux 空闲取消是**两道门**，各有回归：`_has_live_subflow`（`test_tick_paths.py` 的 4 个 idle 用例，覆盖活子链/子链已死/无子链/parent 链接回退）+ `_role_producing_recently`（`test_idle_cancel_blocked_when_role_still_producing`、`test_idle_cancel_noop_logs_debug_not_info`）。
- 已知缺口：`_has_live_subflow` 只看 `subflow_id` / parent 链接，不看 `rework_ids`。**按守卫引入时间切窗实测**（守卫 `421f657` 是 10-19 17:45 加的）：引入**前** 10 例 reason 含「不响应」的取消里有 1 例子链活过父取消（`wf_f269a67e6e6/s2`，10-17，子链 `wf_sub_4af5167d8b89` 直到 22:26 才结束 = 父取消后 55 分钟，10%）；引入**后** 9 例里 0 例。故该缺口目前**无守卫引入后的证据**，n=9 也偏小，本轮不加判定。要修就复用 `engine._derive_rework_ids` / `_rework_alive`。
  ⚠️ 测这类缺口必须**按守卫引入时间切窗**：不切窗会把守卫修好之前的案例算成守卫的缺口（我前两版就是这么错的）。
- 手工验证 `SurvivalMonitor().tick()` 会写生产 bus/哨兵，必须按 [CLAUDE.md](../CLAUDE.md) 三重 env 隔离后再跑。
