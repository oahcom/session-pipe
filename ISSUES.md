# session-pipeline 问题单

> 建单：2026-10-16（HEAD `e2fd23c`）| 复核：2026-10-22（分支 `temp-pr-branch`）
> 建单时的逐条取证正文已删——G1~G10 均已在代码中修掉，下表给出可复跑的复核依据。
> 生产 DB 现状（2026-10-22 实测 `select status,count(*) from workflow_instances group by status`）：
> cancelled 411 / completed 436 / failed 230 / running 20，共 1097 条。

| # | 原缺陷 | 状态 | 复核依据（2026-10-22 实测） |
|---|--------|------|------------------------------|
| G1 | `run.workspace` 属性不存在 → codex review 恒抛异常 | 已修 | `grep -rn "run\.workspace" src/` 无命中 |
| G2 | `_l3_check` 传 bus_client 不存在的 `--src` → 产出信号恒 False | 已修 | `survival_monitor.py:99-105` 改走进程内 `Blackboard()`；全仓 `--src` 仅剩写 fact 的调用（`:314`） |
| G3 | `_cleanup_stale_workflows` SELECT 漏 `current_step_id` → 条件 4 恒不触发 | 已修 | `wf_lifecycle.py:228-231` SELECT 已含该列（并在注释写明漏选后果） |
| G4 | `output_exists` 用 `Path(p).exists()` 直判 → gate 永久 blocked | 已修 | `manager.py:912-921` 只校验"像路径"的条目，描述性条目放行并告警 |
| G5 | `test_harness` 正则误伤 `qa_test_*` → 取消真实 QA 工作流 | 已修 | `task_generator.py:39` 改前缀白名单 `("test_", "exit_schema", "role_interact", "drill")` + `startswith` |
| G6 | `mustContainAnyOf`/`mustNotContain` 未列入缺产出属性 → 契约门可绕过 | 已修 | `step_handler.py:1479` 已加入元组，`:1514-1519` 有校验实现 |
| G7 | `_check_anomalies` 自愈重投无冷却 → 每 10s 重推 | 已修 | `task_generator.py:460` `_HEAL_COOLDOWN_SEC = 600`，`:478-479` 读取 `last_heal` |
| G8 | `delivery_blocked` 达恢复上限成永久孤儿 | 已修 | `engine.py:1783-1800` 达 3 次即 `UPDATE ... status='failed'` 并写 blocker |
| G9 | `wf_starve.json` 的 `last_active_ts` 是幽灵字段 | 已修 | `daemon.py:211` 每轮写 `state["last_active_ts"] = now` |
| G10 | `get_workflow_progress` 等用 `step["step_id"]` 直取 DB 模板 | 已修 | `grep -n 'step\["step_id"\"]' src/lifecycle/manager.py` 无命中；PUBLIC_API 中对应方法已不存在 |
| G11 | 每日 13 次服务重启、无自动拉起（部署健壮性） | **未验证** | `systemctl is-active session-pipeline-daemon.service` → active（2026-10-22 05:49 启动，NRestarts=0）；`journalctl -u session-pipeline-daemon.service --since today` 有"启动"条目。`/etc/systemd/system/session-pipeline.service` **存在但 inactive（disabled）**，不是“不存在”——旧结论来自 `Unit not found` 的误读，排障该用 `systemctl status session-pipeline-daemon.service` |
| G12 | 19 个角色里 13 个无可执行判据，抽样按判据数加权且有放回 | 已修（2026-10-22） | `python3 src/eval_consistency.py` 现输出 `blind_roles` 名单（实测 13 个，含 engineer/reviewer/qa/pm）；`run_eval_check` 改均匀无放回。盲区不设硬失败（每 600s 一条 WARNING 会淹没真不兼容项） |

## 未验证 / 不在本单

- G11 的重启计数只在建单当日成立；服务当前 inactive，历史计数无法从当日 journal 复核。
- 归因类结论（如"某实例由 G5 路径取消"）当年即标注未证实，本单不再保留。
