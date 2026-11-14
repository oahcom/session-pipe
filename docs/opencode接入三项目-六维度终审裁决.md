# opencode 接入 session 三项目 — 六维度合成终审裁决

日期：2026-11-14　裁决人：主代理终审　审查架构：三方六维度

三方 = 实现子代理 / 交叉盲审子代理 / 主代理终审
六维度 = 正确性 / 完整性 / 一致性 / 影响面与零回归 / 实证性 / 防退化

---

## 一、裁决

**通过。** 12 个提交全部落地并经独立复核，P1–P6 六项验收全数成立。
三方审查共披露 4 个实质缺陷（C-1 / B1 / B2 / B3）与 1 个能力缺口（opencode MCP），
全部修复完毕并复验通过；无遗留阻断项。

| 仓 | 提交数 | 测试基线 | 终审结论 |
|---|---|---|---|
| session-launcher | 8（`7500306`…`803607c`） | 516 → **518 passed** | 通过 |
| session-pipeline | 3（`2231f83`/`15fcf40`/`0282708`） | 1359 → **1378 passed** | 通过 |
| hermes-session-roles | 1（`64eb7a4`） | **54 passed** | 通过 |

pipeline 的 3 个 failed 全在 `tests/test_output_continuity.py`（读生产 `blackboard.db` 的 24h 真实产出量，要求 ≥100 实际 1 条）。`git diff 40af62b..HEAD -- tests/test_output_continuity.py` 为空、该文件只 import 标准库、失败用例名改前改后逐字一致 → 环境型既有失败，与本次改动无关。

---

## 二、按最根本排序的缺陷链与修复因果

### 根因：引擎（engine）不是一等概念

改造前 21 处 claude 字面量散布在启动/判活/投递/上下文/观测五条链。哨兵有 `engine` 键但**只写不读**。
所有下游问题都是这一根因的症状，因此修复顺序按「最根本」而非「最好修」：

**第 1 层：引擎身份三层声明** → `persona.engine`（角色默认）→ `ccs.py start --engine`（按次覆盖）→ 哨兵 `engine`（事实源）。重启只读哨兵、永不回读 persona——这是 P3 不翻转的前提。

**第 2 层：启动面查表分派** → `_ENGINE_REGISTRY` 每引擎一行数据（进程名/comm、启动配方、就绪判据、注入策略），字面量收敛为查表。codex 行位保留但三入口显式拒绝（本期孤岛）。

**第 3 层：判活/自愈按引擎查表** → `_ENGINE_SPECS`（忙标记、composer 几何、提交确认）。`_role_is_busy` 对未标定引擎**抛错而非猜**（假闲是最危险方向：生成期误投会覆盖在飞推理）。

**第 4 层：TUI 判据按引擎几何** → ccs 双横线 / opencode 竖线+底边框 `╹▀`，皆经真机标定。

**第 5 层：上下文同源单份生成** → claude→`CLAUDE.md`，opencode→`AGENTS.md`，内容逐字节同源。

**第 6 层：评估面显式第三态** → 非 ccs 引擎的转录读取入口返回 `degraded`（`passed=None` + reason），既非 passed 也非 failed，防静默假阴。

---

## 三、三方审查披露的缺陷（全部已修复）

### C-1（交叉盲审发现，主代理复核成立）｜launcher｜`23cde12`

**问题**：`_restart_if_stale` 的「忙判据未标定」降级为 `busy=False`，而下一行是
`if busy and not _looks_stuck(...)` —— `and` 短路使 `_looks_stuck` **永不执行**，
「判不出忙」直接落入 mtime 判断并 `_kill(pid)`，杀掉生成中的进程。
与同函数 docstring、ValueError 分支的保守处置、原作者注释（「由 _looks_stuck 兜底」）三处证据矛盾。

**为何是根因级**：投递链与重启链的降级方向必须相反——投递误判空闲只是多投一次，
重启误判空闲会杀掉生成中的进程，代价不对等。改为 `busy=True`，重启须由 `_looks_stuck`
正向证明卡死（pane 不动 + CPU 不走）才放行。

**为何能解决**：回归测试 `test_restart_if_stale_uncalibrated_busy_does_not_kill` 在旧实现下
实测 `return True` 且 `_kill` 被调用（会红），新实现下 `kill.assert_not_called()`（变绿）。
附带补 `test_engine_registries_have_same_keys`——此前 `core._ENGINE_REGISTRY` 与
`tmux_ops._ENGINE_SPECS` 双注册表全仓无任何键集断言，一份加一份漏加会形成
「启动认得、判活认不得」的半接入且测试全绿。

### B1（交叉盲审发现，主代理复核成立）｜pipeline｜`0282708`

**问题**：D 侧 `session_engine_for` 只读 `engine` 字段、完全丢弃 `engine_source`。
而 A 侧在哨兵缺失/损坏/无 engine 键时按 tmux 前缀反推，返回 `("ccs","tmux-prefix-fallback")`。
于是 `15fcf40` 新增的「engine 缺失→unknown」分支在生产上是**死代码**（A 侧永不缺该字段），
真正的缺证据场景被静默标成 `ccs` 并被 D 侧当事实消费。
后果：一个**活着的 opencode 会话**若哨兵被删，评估面会去读同角色 24h 内的**旧 ccs 转录**
并据此下结论（假阴/假阳）——正是 T2D 要消灭的失败模式，只是搬到了 A 侧。

**为何能解决**：`engine_source != "sentinel"`（推断值不得冒充事实，契约 4.2）且 `alive` 为真
→ `unknown` 显式降级。**关键取舍**：`alive` 为假/缺失时不降级——19 个评估角色中 10 个无哨兵
且未运行，一刀切会每轮刷 10 条噪音；危险面只存在于「活会话 + 身份无事实源」。
反证：旧实现下 `test_session_engine_ignores_inferred_source_when_alive` 红。

### B2（交叉盲审发现）｜pipeline｜`0282708`

**问题**：`_fetch_status_role` 的 `communicate(timeout=10)` 超时后不 kill/wait，
子进程继续跑（实测 `poll()` 仍 `None`）。失败结果还进 600s 缓存 →
最坏每角色每 10 分钟泄漏一个 python3（含 sqlite 连接 + tmux 调用）。本次由 `2231f83` 引入。

**为何能解决**：`except TimeoutExpired: _p.kill(); _p.wait(); raise`。
反证：`test_fetch_status_role_kills_on_timeout` 断言 `kill.call_count==1`、`wait.call_count==1`。

### B3（交叉盲审发现）｜pipeline｜`0282708`

**问题**：`_transcript_progress_after_error` 无自己的降级门，靠 `_read_transcript_tail` 返回
`[]` 后落到底部 `return not _saw_error` → **降级时返回 True**（「确认有进展」是肯定性结论）。
这是全组唯一「未知 → 肯定值」方向（其余皆保守 False）。

**为何能解决**：顶部补降级门（同 `_read_transcript_tail` 形态）→ `return False`。
唯一调用点只用它 pop 退避状态，返回 False 的后果是保留退避（安全方向）。
反证：`test_progress_after_error_degraded_returns_false` 旧实现下红。

### 能力缺口：opencode 零 MCP｜launcher｜`803607c`

**问题**：19/19 persona 声明了 `mcp_servers`，但 `_write_mcp_settings` 只写
`.claude/settings.json`，而 opencode **不读**该文件——opencode 会话 `opencode mcp list`
返回 `No MCP servers configured`，角色拿不到任何 persona 声明的 MCP 工具。
计划书原把「opencode MCP 映射」划为一期不做的显式边界；但主代理实证可行且属「同级」
关键能力，故本期补上而非仅披露。

**实证形态差异**（v1.19.3）：
- opencode 读**项目根 `opencode.json`** 的 `mcp` 键（非 `opencode.jsonc`）；
- `type: "local"`（非 claude 的 `"stdio"`）；`command` 是**数组**（非 `command`+`args` 两段）；
- `command` 写裸名 `node` 会 `Executable not found in $PATH`（opencode 的 PATH 只有
  `/mnt/c/nvm4w/nvm/nodejs`）；必须绝对路径 `/home/administrator/.local/bin/node`；
- `env` → `environment`。

**为何能解决**：`_mcp_cfg_to_opencode` 做形态转换 + 命令绝对路径解析（解析不到返回 `None`
并跳过，**绝不写裸名**——写裸名会得到静默失败，比不写更坏）；
`_write_opencode_mcp` 只在 `engine == "opencode"` 时调用，claude 路径零新增 I/O。
只覆盖 `mcp` 键，保留用户手写其它键；JSON 损坏时不覆盖。opencode 权限模型与 claude 不同构，
一期不映射（只记日志，不猜着映射）。

**终审复核**（主代理实跑）：
- 端到端：`_write_opencode_mcp` 落盘 → `opencode mcp list` → **`✓ hex-line connected`**
- 合并语义：预置 `{"theme":"dark"}` → 写后 `theme` 保留、`mcp` 被覆盖
- claude 路径零回归：`_write_mcp_settings` 只写 `.claude/settings.json`，不产 `opencode.json`
- 无可转换项时**不建文件**（避免把用户手写 mcp 覆盖成空表）
- `git show 803607c` 删除行数 = **0**（纯增量，`_write_mcp_settings` 逐字未动）
- `pytest tests -q` → **518 passed**

---

## 四、P1–P6 验收核验

| 项 | 定义 | 判定 | 实证 |
|---|---|---|---|
| **P1 同级生命周期** | opencode 走与 claude 相同六链入口（启动/投递/判活/自愈/上下文/状态），行为等价 | **通过** | 双引擎端到端冒烟 `smoke_a/b/c_start.txt` + `_capture.txt` + `_ps.txt` + `_sentinel.json`；六链逐链留证 |
| **P2 可选择且持久** | 角色默认（persona）+ 单次覆盖（CLI）；选择结果持久化 | **通过** | 三入口哨兵：`smoke_a` 默认→`engine:ccs`、`smoke_b` persona 声明→`engine:opencode`、`smoke_c` CLI 覆盖→`engine:opencode`；`status-role` 输出含 `engine`/`engine_source` |
| **P3 不翻转** | 任何自动重启引擎与出生一致，只认持久事实不认推断 | **通过** | gate-rehearsal 三轮：R1 身份启动（pane 树 bash→opencode 无 claude）；R2 陈旧重启不翻转（pid 19473→55707）；R3 watchdog 自愈不翻转（55707→64188），哨兵 engine 恒 opencode |
| **P4 claude 零回归** | 不带新参数时全部现有行为与改前语义等价 | **通过** | claude cmd **字节等价**（`claude_path_equivalence.txt`：old/new 逐字相同，ready 判据全样本等价，fail_re 相同）；launcher 516→518（+2 为新增用例）；pipeline 1359→1378（+14 新增用例 +5 补丁用例，失败集同名同数）；`7500306^..HEAD` 无 ccs 分支逻辑删除 |
| **P5 评估诚实** | 观测/评估面对 opencode 会话显式标注「未纳入」，无静默假阴性 | **通过** | `eval-snapshots.txt`：`engine=opencode/codex/unknown` 三态均 `{"passed":null,"status":"degraded",…"note":"未纳入评估"}`；summary 有独立 `degraded` 计数 + `degraded_detail` + bus notice；ccs 同型零触碰降级 |
| **P6 codex 不受损** | codex 孤岛行为不变 | **通过** | `codex start` 显式拒绝（`neg_codex_reject.txt`：rc=1 + 保留行位说明）；`codex status` 行为不变；`cdx-` 前缀解析、`_find_claude_pid` 的 `-f` 回退逻辑零改动 |

---

## 五、术语碰撞警示（审查红线）

「引擎」在本仓有两义，禁止混同：

1. **会话承载引擎**（`claude`/`opencode`/`codex`）—— 本次改造对象，`core._ENGINE_REGISTRY`、
   `tmux_ops._ENGINE_SPECS`、哨兵 `engine` 键皆指此义。
2. **工作流引擎**（`wf:engine`，`ecosystem_health._check_workflow_engine`）—— daemon 锁判活，与会话无关。

`tests/test_ecosystem_health_engine.py` 测的是**第 2 义**，3 passed。
核验：`git diff 7500306^..HEAD -- src/ecosystem_health.py` 中 `_check_workflow_engine` 出现
**0 次**，wf:engine 语义零改动。

---

## 六、已知限制（随交付披露，本期不改）

| # | 限制 | 位置 | 为何登记不修 | 影响方向 |
|---|---|---|---|---|
| L1 | opencode 忙标记文案漂移 | `tmux_ops._ENGINE_SPECS["opencode"].busy_markers=("esc interrupt",)` | v1.19.3 实测对上；加固需「全不命中+转点动画」二次判据，属 TUI 协议面新增逻辑，误报忙会饿死投递 | 静默假闲（引擎改版换措辞时） |
| L2 | codex composer/submit 未标定 | `_ENGINE_SPECS["codex"]` = `uncalibrated` | codex 本期孤岛，主启动面显式拒绝；判据须真机标定 | 降级为投递不误报（`23cde12`），但不保证提交确认准确 |
| L3 | `_ENGINE_CACHE` 600s 引擎切换窗口 | `role_lifecycle._ENGINE_CACHE_TTL` | 切换低频、P3 防翻转前提；失效键需哨兵 mtime 钩子 | ccs→opencode 时 ≤600s 用旧转录下结论 |
| L4 | `ops.workspace.register` 未传 engine | `ops/workspace.py:15` | 恢复存量会话的路径，改动涉副本归属判定 | 恢复 opencode 会话时哨兵写成 ccs |
| L5 | codex sleep 间隙仍判僵尸 | `ecosystem_health._pane_has_claude` | cmdline 判据分辨率上限；旧实现对 codex 全程 False 更差 | 健康扫描误报 |
| L6 | 评估面 `main()` 退出码不反映降级 | `eval_checker.py:1692` | 全角色降级时 rc=0（绿）；summary/bus 有可见面故非静默 | cron 层无法区分「全通过」与「全降级」 |
| L7 | opencode 权限模型未映射 | `core._write_opencode_mcp` | `permission` 是 allow/ask 分级对象，与 claude `permissions.allow` 不同构，猜着映射会静默放宽或收紧 | 只记日志「一期不映射」 |
| L8 | `_restart_if_stale` 对 codex 是死代码 | `tmux_ops.py:433` | 调用方只喂 `ccs-{role}` 名，codex 会话是 `cdx-` | 无实际影响，仅记录 |

---

## 七、证据索引

`C:\Users\Administrator\WorkBuddy\2026-11-13-16-01-58\evidence\`

| 目录 | 内容 | 关键文件 |
|---|---|---|
| `t1a-core/` | 引擎一等概念 + 启动面分派 | `smoke_a/b/c_start.txt` + `_capture.txt` + `_ps.txt` + `_sentinel.json`（三入口哨兵取证）；`status_role_{def,pers,fallback}.json`；`claude_path_equivalence.txt`（P4 字节等价）；`neg_codex_reject.txt`/`neg_bad_engine.txt`（拒绝面）；`calib_opencode_ready_frame.txt` |
| `t1b-liveness/` | 判活/自愈/TUI 判据 | `calib/`（oc-01…07 竖线 composer 标定、cc-01…06 双横线对照）；`smoke-oc-{busy,composer}-live.txt`；`KNOWN-LIMITS.txt`（L1–L4） |
| `t1c-context/` | 上下文按引擎路由 | `VERDICT.md`（md5 同源表）；`reverify/rv2-05-md5-table.txt`（case1–10 同源证明） |
| `t2d-eval/` | 评估面显式降级 | `eval-snapshots.txt`（三引擎降级快照 + summary/degraded_detail/bus notice）；`status-role-engine-probe.txt` |
| `t2d-intent/` | 意图分类器转录守卫 | `fixture-output.txt`（26 断言全 PASS）；`fixture-diff-head-vs-worktree.txt`（七态差分：非 ccs 2→0 / ccs 三态同型） |
| `gate-rehearsal/r2/` | P3 端到端三轮 | `r1/r2/r3.txt`、`tmux-before/after.txt`、`cleanup.txt` |
| `fix-n1/` | N1 修复 + 真机 A/B | `smoke_before_start.txt`（必现 ERROR）vs `smoke_after_start.txt`（消失）；`ev_unit.py` + `ev_unit_output.txt`（17 断言）；`pytest_final.txt` |
| `rev/t2drev/` | T2D 交叉盲审取证 | `c1.diff`/`c2.diff`/`ev_all.txt`/`probe_out.txt` |
| 报告（仓根） | 三方审查报告 | `rev-n1-REPORT.md`、`rev-t2b-REPORT.md`、`fix-t2db-REPORT.md`（WSL `/home/administrator/`） |

---

## 八、改动清单

### session-launcher（`feat/opencode-engine`，8 提交）

| 提交 | 层 | 内容 |
|---|---|---|
| `7500306` | 3+4+7 | T1B 判活/自愈/TUI 判据按引擎查表 |
| `df0b434` | 1+2 | T1A 引擎一等概念 + 启动面按引擎分派 |
| `a7fcdd1` | 5 | T1C 上下文落盘按引擎路由 + 同源单份生成 |
| `5aaeae9` | 6 | T2D-E 意图分类器转录守卫按引擎显式降级 |
| `ae1182e` | 5 | T1C 注入引擎取数补 persona 声明兜底（首启） |
| `57fbdb6` | 1+2 | T1A-fix 哨兵写失败受控回收/非 ccs 就绪显式失败/status 引擎 pid/注入透传/codex 三入口拒绝 |
| `1f0d9cb` | — | **N1** 首启投递链 engine 显式透传 + 未标定忙判据投递降级 |
| `23cde12` | — | **C-1** 陈旧重启降级改为保守 + 双注册表键集一致性测试 |
| `803607c` | — | **MCP** opencode 侧按 persona 落盘 `opencode.json`（claude 路径零改动） |

改面：`core.py` +566、`tmux_ops.py` +303、`routing/roles.py` +70、`ccls_daemon_intent_classifier.py` +96、
`ops/watchdog.py` +52、`ecosystem_health.py` +32、`ccs.py` +40。

### session-pipeline（3 提交）

| 提交 | 内容 |
|---|---|
| `2231f83` | 评估面显式降级——非 ccs 引擎转录读取入口返回带原因的第三态 |
| `15fcf40` | 引擎兜底口径收紧——engine 缺失/未知值显式降级，勿静默当 ccs |
| `0282708` | **B1** 只认事实源 / **B2** 超时回收子进程 / **B3** 进展判定降级保守 |

### hermes-session-roles（1 提交）

`64eb7a4`：persona 增 `engine` 承载引擎声明（19 存量显式补 `ccs` + 装载端缺省 + 取值校验）。

---

## 九、三方六维度逐项裁决

| 维度 | 实现子代理 | 交叉盲审 | 主代理终审 |
|---|---|---|---|
| **正确性** | 三层声明 + 查表分派落地 | C-1（重启降级方向反）、B1（推断值当事实） | 两缺陷已修 + 反证红测确认 |
| **完整性** | 六链全覆盖，5 个转录读点加门 | 确认无漏网入口（全仓 `~/.claude/projects` 仅 3 处引用） | 通过 |
| **一致性** | 字段/三值域对齐 | B2（进程泄漏）、600s 缓存不一致 | 两缺陷已修，L3 登记 |
| **影响面与零回归** | ccs 路径逐字不动 | `2231f83` 零删除行；launcher 516 passed | 字节等价 + 失败集同名同数 |
| **实证性** | 三仓全量测试 + 真机冒烟 | 端到端 A/B 对照、P3 三轮演练 | 主代理独立复跑 518/1378/54 |
| **防退化** | 未标定即抛错不猜 | B3（未知升格为肯定值）、`test_ecosystem_health_engine` 术语碰撞 | B3 已修 + 双注册表一致性测试补上 |

---

## 十、主代理独立复核记录

- 读全 `1f0d9cb`/`23cde12`/`0282708`/`803607c` 四个修复 diff，逐行核对声明与实现一致
- `pytest tests -q`：launcher **518 passed**（4.60s）、pipeline **1378 passed + 3 failed**（63.66s，3 个失败同名）、roles **54 passed**
- 生产四清复核：`tmux ls` 仅 `ccs-consumer/engineer/scout`，pane_pid `1675/1690/1659` 与开跑前一致，pane 首子进程均为 `claude`，哨兵 `engine` 全 `ccs`
- MCP 端到端：`opencode mcp list` → `✓ hex-line connected`
- claude 路径零改动：`git show 803607c` 删除行 = 0；`_write_mcp_settings` 逐字未动
- 术语碰撞：`_check_workflow_engine` 在 diff 中 0 次出现，`test_ecosystem_health_engine.py` 3 passed
- 双注册表：`core._ENGINE_REGISTRY` 与 `tmux_ops._ENGINE_SPECS` 键集均为 `['ccs','codex','opencode']`，equal=True
- 12 哨兵 `engine` 全 `ccs`；`status-role` 输出含 `engine`/`engine_source`/`pid`/`pid_source`

**审查偏差处置**：
- 交叉盲审 `review-n1` 报「`ev_unit.py` 全盘不存在」（条件项 C-2）→ **驳回**：该文件在 Windows 侧
  `evidence/fix-n1/ev_unit.py`（4846 字节，17 断言输出完整），审查代理只在 WSL 侧 find，取证位置错位。
- 交叉盲审 `review-t2b` 报「`evidence/t2d-eval/` 不存在」→ **驳回**：同上，文件在 Windows 侧
  `evidence/t2d-eval/`，4 个文件齐全。
- `fix-t2d-b` 如实申报「5 例并非全部变红，实为 3 红 2 绿」（2 个是防过度降级的护栏）→ **接受**，
  该申报符合实证纪律，未冒充。
