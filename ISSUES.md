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
| G12 | 19 个角色里 14 个零可执行判据（运行期窗口口径），抽样加权且有放回 | 已修（2026-10-22） | `eval_consistency` 现输出 `blind_roles`（实测 14 个）+ `exec_total`（窗口内可执行 9 条、`no_cmd_total` 27 条；`PYTHONPATH=src python3 src/eval_consistency.py --json` 可复跑）；`run_eval_check` 只跑 `eval_criteria[:3]`（`CRITERIA_WINDOW`）并改均匀无放回；daemon 盲区日志原在 `rc==0` 分支而生产 rc 恒为 1 → 提到 rc 分支之前（`4f04a13`）。盲区不设硬失败（每 600s 一条 WARNING 会淹没真不兼容项） |
| G13 | 主循环按饥饿排序时 `assignee=''` 的实例被 `assignee < ?` 比较丢到队尾，默认角色实例饿了也不被推进 | 待修（本轮只取证，未改代码） | 判据：`wig` 排序键里空串参与比较；实测这条结论尚未用「同一 tick 内该类实例是否被扫到」的日志证实，**归因待验证** |
| G14 | 额度退避的 attempt 计数行写盘后必被丢弃（`__rate_n__` 的值是计数不是时间戳，`_load_alert_ts` 的 TTL 过滤把它当 1970 年） | 未修（天花板已知） | `role_lifecycle.py:298` 读 `rows.get((role, "__rate_n__"))`，而 `:247` 的 `now - float(ts) < ttl` 对计数行恒假 → 重启后 attempt 恒 0、退避从 120s 重来。代价可控（每次重启多 1~2 次「继续」），升级路径：计数编码进 tag（`__rate_n__3`）或改存 `retry_at` 时间戳 |
| G15 | 白名单二次校验把 `systemctl --user is-active X` 判成写操作（只看 `parts[1]`）→ maintainer 的判据永久「不兼容」，daemon 每 600s 一条 WARNING | 已修（2026-10-22 `f16bb34`） | `_cmd_is_readonly` 跳过选项再取子命令（取值型选项 fail-closed 仍拒绝）；`eval_consistency` 由 `incompatible_roles=1` 变 0、`exec_total` 8→9；`tests/test_eval_checker_ext.py::test_systemctl_flags_before_subcommand_are_readonly` 钉住四个方向 |
| G16 | 工作树索引里残留 `f16bb34` 的回退 blob（`6beb7e7`）——`git commit -- src/eval_checker.py` 会把 `9ca49e2` 的 129 行修复整段写回旧版，24h 轨迹窗口 / 自有 workspace 盲改豁免 / 30s bus 超时再次消失 | 已修（2026-10-22，`git checkout HEAD -- src/eval_checker.py`） | `git status --porcelain=v2 -- src/eval_checker.py` 输出为空；`grep -n 'timeout=30' src/eval_checker.py` 命中 555 行；提交前先跑 `git diff --cached --numstat -- <file>`，非空就问一句“这是谁的 blob” |

## 未验证 / 不在本单

- G11 的重启计数只在建单当日成立；服务当前 inactive，历史计数无法从当日 journal 复核。
- 归因类结论（如"某实例由 G5 路径取消"）当年即标注未证实，本单不再保留。
