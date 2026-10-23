# session-pipeline 问题单

> 建单：2026-10-16（HEAD `e2fd23c`）| 复核：2026-10-22（分支 `temp-pr-branch`）
> 建单时的逐条取证正文已删——G1~G10 均已在代码中修掉，下表给出可复跑的复核依据。
> 生产 DB 现状现取（数字每轮在变，不抄绝对值）：`sqlite3 ~/.hermes/state/workflows.db "select status,count(*) from workflow_instances group by status"`。

| # | 原缺陷 | 状态 | 复核依据（2026-10-22 实测） |
|---|--------|------|------------------------------|
| G1 | `run.workspace` 属性不存在 → codex review 恒抛异常 | 已修 | `grep -rn "run\.workspace" src/` 无命中 |
| G2 | `_l3_check` 传 bus_client 不存在的 `--src` → 产出信号恒 False | 已修 | `survival_monitor.py:99-105` 改走进程内 `Blackboard()`；全仓 `--src` 仅剩写 fact 的调用（`:314`） |
| G3 | `_cleanup_stale_workflows` SELECT 漏 `current_step_id` → 条件 4 恒不触发 | 已修 | `wf_lifecycle.py:261-265` SELECT 已含该列（并在注释写明漏选后果） |
| G4 | `output_exists` 用 `Path(p).exists()` 直判 → gate 永久 blocked | 已修 | `manager.py:921-933` 只校验"像路径"的条目，描述性条目放行并告警 |
| G5 | `test_harness` 正则误伤 `qa_test_*` → 取消真实 QA 工作流 | 已修 | `task_generator.py:39` 改前缀白名单 `("test_", "exit_schema", "role_interact", "drill")` + `startswith` |
| G6 | `mustContainAnyOf`/`mustNotContain` 未列入缺产出属性 → 契约门可绕过 | 已修 | `step_handler.py:1495-1496` 已加入元组，`:1524-1535` 有校验实现 |
| G7 | `_check_anomalies` 自愈重投无冷却 → 每 10s 重推 | 已修 | `task_generator.py:608` `_HEAL_COOLDOWN_SEC = 600`，`:626-627` 读取 `last_heal` |
| G8 | `delivery_blocked` 达恢复上限成永久孤儿 | 已修 | `engine.py:1421-1452` 达 3 次即 `close_wf(status='delivery_blocked')` 并写 blocker |
| G9 | `wf_starve.json` 的 `last_active_ts` 是幽灵字段 | 已修 | `daemon.py:208` 每轮写 `state["last_active_ts"] = now` |
| G10 | `get_workflow_progress` 等用 `step["step_id"]` 直取 DB 模板 | 已修 | `grep -n 'step\["step_id"\"]' src/lifecycle/manager.py` 无命中；PUBLIC_API 中对应方法已不存在 |
| G11 | 每日 13 次服务重启、无自动拉起（部署健壮性） | **未验证** | `systemctl is-active session-pipeline-daemon.service` → active（2026-10-22 05:49 启动，NRestarts=0）；`journalctl -u session-pipeline-daemon.service --since today` 有"启动"条目。`/etc/systemd/system/session-pipeline.service` **存在但 inactive（disabled）**，不是“不存在”——旧结论来自 `Unit not found` 的误读，排障该用 `systemctl status session-pipeline-daemon.service` |
| G12 | 19 个角色里 14 个零可执行判据（运行期窗口口径），抽样加权且有放回 | 已修（2026-10-22） | `eval_consistency` 现输出 `blind_roles`（实测 14 个）+ `exec_total`（窗口内可执行 9 条、`no_cmd_total` 27 条；`PYTHONPATH=src python3 src/eval_consistency.py --json` 可复跑）；`run_eval_check` 只跑 `eval_criteria[:3]`（`CRITERIA_WINDOW`）并改均匀无放回；daemon 盲区日志原在 `rc==0` 分支而生产 rc 恒为 1 → 提到 rc 分支之前（`4f04a13`）。盲区不设硬失败（每 600s 一条 WARNING 会淹没真不兼容项） |
| G13 | 主循环按饥饿排序时 `assignee=''` 的实例被 `assignee < ?` 比较丢到队尾，默认角色实例饿了也不被推进 | 已失效（2026-10-23 复核） | `grep -rn 'wig\|assignee < ?' src/` 无命中：实例查询改成 `ORDER BY created_at` + 断点轮转（`engine.py:429-432` 写明「查询无 ORDER BY → 用断点轮转而不是 ORDER BY」）→ 老实例反而排在前面，该饥饿路径已不存在。原结论本来就标了「归因待验证」，不另立新说 |
| G14 | 额度退避的 attempt 计数行落盘后必被丢弃（`__rate_n__` 存的是计数不是时间戳，`_load_alert_ts` 的 TTL 过滤把它当 1970 年条目整行裁掉） | 已修（2026-10-23） | 计数行改为不受 TTL 裁剪（`role_lifecycle.py` 里 `title == _RATE_N_TAG or now - ts < ttl` 两处），并在 200 条上限排序里优先保留。**原测试是假绿**：`test_rate_state_survives_restart` 用 `now=1001.0` 使 `now-3 < ttl` 恒真；已改成真实量级时间戳，去掉修复后该用例必红（实测 1 failed / 10 passed） |
| G15 | 白名单二次校验把 `systemctl --user is-active X` 判成写操作（只看 `parts[1]`）→ maintainer 的判据永久「不兼容」，daemon 每 600s 一条 WARNING | 已修（2026-10-22 `f16bb34`） | `_cmd_is_readonly` 跳过选项再取子命令（取值型选项 fail-closed 仍拒绝）；`eval_consistency` 由 `incompatible_roles=1` 变 0、`exec_total` 8→9；`tests/test_eval_checker_ext.py::test_systemctl_flags_before_subcommand_are_readonly` 钉住四个方向 |
| G16 | 工作树索引里残留 `f16bb34` 的回退 blob（`6beb7e7`）——`git commit -- src/eval_checker.py` 会把 `9ca49e2` 的 129 行修复整段写回旧版，24h 轨迹窗口 / 自有 workspace 盲改豁免 / 30s bus 超时再次消失 | 已修（2026-10-22，`git checkout HEAD -- src/eval_checker.py`） | `git status --porcelain=v2 -- src/eval_checker.py` 输出为空；`grep -c 'timeout=30' src/eval_checker.py` = 1（行号随改动漂移，别抄）；提交前先跑 `git diff --cached --numstat -- <file>`，非空就问一句“这是谁的 blob” |

## 未验证 / 不在本单

- G11 的重启计数只在建单当日成立；系统单元 `session-pipeline.service` 当前 inactive，历史计数无法从当日 journal 复核。
- 归因类结论（如"某实例由 G5 路径取消"）当年即标注未证实，本单不再保留。
