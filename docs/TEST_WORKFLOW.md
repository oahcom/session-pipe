# 测试工作流文档

> 更新: 2026-10-22 | 受众: 测试工程师 | 相关: [DOCS.md](DOCS.md)

## 运行

```bash
cd /home/administrator/session-pipeline
python3 -m pytest tests/ -q                 # 全量
python3 -m pytest tests/ -q --collect-only  # 只收集（秒级，验证导入是否坏）
python3 -m pytest tests/test_engine_e2e.py -q
```

`tests/conftest.py` 已把 `src/` 加入 `sys.path`，无需 `PYTHONPATH`；直接跑 `python3 src/...` 才需要。

## 实测基线（2026-10-22 05:40）

| 命令 | 结果 |
|------|------|
| `--collect-only` | 1218 tests collected, 0 error |
| `pytest tests/ -q` | **1215 passed, 3 skipped** in 117s |

> 这两个数字只是某个时刻的快照，别当门禁。判定"我的改动有没有引入回归"先跑
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

- `_cleanup_stale_workflows` 条件 4（超时/排队/子链/证据门组合）无专门单测，行为靠 e2e 间接覆盖。
- 手工验证 `SurvivalMonitor().tick()` 会写生产 bus/哨兵，必须按 [CLAUDE.md](../CLAUDE.md) 三重 env 隔离后再跑。
