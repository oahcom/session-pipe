# Session 三项目架构全景图

> Updated: 2026-10-23 | Audience: Full-stack engineers | Related: [ARCHITECTURE.md](ARCHITECTURE.md), [DOCS.md](DOCS.md)
> 数字均为 2026-10-22 实测（`glob('**/*.py')` 计行，**含 tests**，排除 `__pycache__`），不是估算。

## 三个项目一句话

| 项目 | 类比 | 职责 | 实测规模 |
|------|------|------|----------|
| **hermes-session-roles** | `/etc/passwd` | 角色定义：19 个 persona JSON（`ls personas/session-roles/*.json`，源）；运行期读 `~/.hermes/data/roles_export.json`（导出物，会落后）+ prompt 模板 | 2499 行（14 个 `.py`，src 7） |
| **session-launcher** | `systemd` | CCS 进程管理：tmux 创建/哨兵/看门狗/CLI 入口 | 17121 行（73 个 `.py`，src 32） |
| **session-pipeline** | 消息队列 + 状态机 | 消息路由 + 工作流引擎 + 生命周期状态机 + 可靠性 | 现算：`git ls-files '*.py' | wc -l`；`git ls-files 'src/*.py' | xargs wc -l` 取 src 总行数；`ls tests/test_*.py | wc -l` 取测试文件数（不写死，随改动变化） |

**实际关系不是三层流水线，而是两消费者并行：**

```
session-roles（定义层）
  ├──→ launcher（执行层）        ← 读 persona JSON 启动 CCS + 注入 prompt
  └──→ pipeline（路由+执行层）    ← 读 roles_export.json 构建路由表 + pipeflow 推动
         └──→ launcher/ccs.py send  ← 唯一跨项目调用（subprocess）
```

## 运行中的代码来自哪里（2026-10-23 实测）

两个 systemd 单元的 `WorkingDirectory` 都是 `~/session-pipeline`（分支 `temp-pr-branch`），**不是** `~/wt-pipeline` 的 `main`。合到 main 的提交不会自己生效，要在该 checkout 合入并重启单元：

```bash
systemctl --user show workflow-engine.service session-pipeline-daemon.service -p WorkingDirectory
git -C ~/session-pipeline rev-parse --abbrev-ref HEAD
git -C ~/session-pipeline merge-base --is-ancestor <commit> HEAD && echo live || echo "未上线"
```

实测 2026-10-26：merge-base 已推进到 `241511b`，main 独有 **224** 个补丁未进运行分支，运行分支独有 **1** 个（v25 L2 并入快照）；「已合并」与「已生效」是两件事，判断上线只能看上面第三条命令。`git merge-base --is-ancestor <commit> HEAD` 为 `live` 才代表已上线。`doccheck` 只校验 `~/wt-pipeline` 的 `CLAUDE.md` 行数，不校验运行分支。`9901`（memory-server-dkk）与 `9902`（memory-server-ssk）在 `ss` 输出为 *:20127（nine-router 主实例）、*:8890（control-panel）、*:8767（gateway），说明 Sister Bus 4 个 session socket（cron/dkk/feed/ssk）与三项目端口在同台机器上跑。

## 系统入口

| 项目 | 入口 | 调用方式 |
|------|------|----------|
| session-roles | `python3 src/shared_loader.py --export` | CLI（生成 `~/.hermes/data/roles_export.json`，无 daemon 定时跑） |
| session-roles | `python3 src/validate_roles.py` | CLI（全量校验） |
| session-roles | `python3 src/role_assembler.py <role>` | CLI（组装 prompt） |
| session-launcher | `python3 src/ccs.py start/stop/send/output` | CLI 或 subprocess（被 pipeline 调） |
| session-launcher | `python3 src/wf.py <子命令>` | CLI（工作流客户端，含 stats） |
| session-pipeline | `PYTHONPATH=src python3 -m pipeflow.daemon --interval 10` | systemd / 后台进程 |
| session-pipeline | `PYTHONPATH=src python3 -m routing_daemon` | 路由守护（常驻） |
| session-pipeline | `PYTHONPATH=src python3 -m routing.auto --daemon` | CLI（一次性，非守护；`--status` 无派发分支） |

## 端口与 socket（`ss -ltnp` 实测 2026-10-22）

| 地址 | 所属 | 状态 |
|------|------|------|
| `:20127` | nine-router | LISTEN（主实例） |
| `:20128` | nine-router | 未监听（旧实例，CLAUDE.md 记的回落位） |
| `:8767` | hermes-gateway | LISTEN |
| `:8890` | hermes-control-panel | LISTEN |
| `:9901` | memory-server-dkk | LISTEN |
| `:9902` | memory-server-ssk | LISTEN |
| `/tmp/sister_bus_{dkk,ssk,cron}.sock` + `/tmp/sister_bus_feed.sock` | Sister Bus | 按 session 分 socket，无单一 `_ccs.sock` |

## 持久化数据库（`~/.hermes/state` 是 `~/.hermes/data/state` 的软链，两路径同指）

| DB | 位置 | 用途 |
|----|------|------|
| Blackboard | `~/.hermes/sister_bus/blackboard.db` | 消息总线 |
| Workflows | `~/.hermes/state/workflows.db` | 表：`workflow_instances`/`workflow_templates`/`tasks`/`workflow_logs` |
| ACK 回执 | `~/.hermes/state/ack_tracker.db` | 消费确认 |
| 路由表 | `~/.hermes/state/routing.db` | 路由表 + 变更审计 |
| ~~Pipeline Cursor~~ | `~/.hermes/state/pipeline_cursor.db` | **已退役**（2026-10-22，去重改由 bus consume 记录承担；表 0 行） |

## 哨兵文件（实测在 `~/.hermes/run/`；两个目录写的不是同一份数据）

| 目录 | 内容 |
|------|------|
| `~/.hermes/run/ccs-sentinels/` | CCS 运行状态（launcher `ops/sentinel.py` 写，含 `health.last_watchdog_check`） |
| `~/.hermes/run/ccs-health/` | 存活探针（pipeline `survival_monitor.py` 写，键只有 `survival_*`；watchdog 字段**不在这里**） |
| `~/.hermes/run/ccs-lifecycle-sentinels/` | ondemand 角色生命周期哨兵 |

`/tmp/ccs-sentinels`、`/tmp/ccs-health`、`/tmp/cdx-sentinels` 均已不存在——旧文档里的 `/tmp` 路径是过期记录。

## 工作空间

`~/ccs-workspaces/` 下每个 CCS 角色有独立目录，各含独立 `CLAUDE.md`，用
`<!-- WORKSPACE_SYS:START/END -->` + `<!-- KNOWLEDGE:START/END -->` 标记管理知识注入。

## 执行循环

```
pipeflow/daemon.py  while True (sleep 10s) — 工作流推进唯一循环
  └── engine.run_once()
        ├── _ensure_guard_roles()         守护当前步骤 target_role/guard/watch/wait_roles
        ├── SQLite 扫描 workflow_instances（pending/running/step_done_ready）
        │     ├── 步骤未开始 → 发初始 prompt
        │     ├── exit_condition/证据门匹配 → 推进
        │     ├── 超时 → 提醒 → 重推 → 升级 coordinator
        │     └── 子链完成 → 推进父链
        ├── _check_anomalies()            多步卡死 → 自愈/取消
        └── _scan_tasks()                 任务状态同步 + 子链级联
```

## 路由矩阵不在此维护

角色 produce/consume 随 `roles_export.json` 变化，文档抄一遍必然过期。看实时值：

```bash
cd ~/session-pipeline
PYTHONPATH=src python3 -m routing.router list      # 每角色 produce/consume
PYTHONPATH=src python3 -c "from routing.router import format_pipeline; print(format_pipeline())"
```

## 健康检查入口

```bash
python3 ~/session-pipeline/tests/health_check_all.py
python3 ~/hermes-session-roles/src/validate_roles.py
python3 ~/session-launcher/src/ccs.py health
```
