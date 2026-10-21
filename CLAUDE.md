# session-pipeline — 消息路由与工作流执行层

> 仅含本项目操作规则，不涉及角色定义或 launcher 配置。

## 项目职责

消息路由/分发/优先级/重试/熔断、工作流引擎、生命周期状态机、路由表持久化。

## 架构定位

```
session-roles（定义层） ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
         │                                          │
         ├──→ session-launcher（执行层）              ├──→ 各自独立消费角色定义
         └──→ session-pipeline（路由+执行） ─ ─ ─ ─ ┘
                  │
         pipeline → launcher 的唯一调用:
         subprocess ccs.py send
```

pipeline **不直接调用** launcher 的 API，通过 `subprocess ccs.py send` 通信。

> **唯一真实优化标的**：角色对话质量、思考质量、动作质量，不是管道吞吐量。路由/工作流/守护进程都在转 ≠ 系统在工作——近 3 小时零条真实角色产出就是"转而不活"。本项目的所有指标（路由数、running 工作流数、存活角色数）只证明"管道通"，**不可作为角色在高质量产出的证据**。

## 关键规则

### 路由策略
- 消息按 bus 分类自动路由到对应角色
- 优先级：`P0 > P1 > P2 > P3 > 常规`
- 超时熔断：连续 N 次路由失败暂停 30s

### 工作流引擎 (pipeflow)
- 模板由 `src/template_registry.py` 注册管理
- pipeline 自身运行工作流状态机（不依赖 launcher）
- 工作流完成/失败通过 `ccs.py send`（subprocess）通知角色

### 验证命令
```bash
# 测试路由规则
python3 -m pytest tests/ -q -k router

# 列出路由表（router CLI）
PYTHONPATH=src python3 -m routing.router list

# 健康检查（工作流统计；wf CLI 在 launcher 侧，pipeline 无 stats 子命令）
python3 ~/session-launcher/src/wf.py -r maintainer stats
```

### 禁止操作
- ❌ 直接调 launcher 的 API（只能用 subprocess ccs.py send）
- ❌ 修改角色定义（那是 session-roles 的事）
- ❌ 重启 gateway（SIGKILL 中断长连接）
- ❌ 用流程指标代理质量判断：路由数、workflow running 数、存活角色数、bus stats 条数只能证明"管道通"，**不可证明角色在高质量产出**。声称优化完成前，必须亲自打开一个角色的对话逐轮读、亲手判断思考与动作质量。禁止用质量分/看板/automated checker 替代"人读对话做判断"——那会让开发者去刷指标而非去想内容。
- ❌ 把"消息送达"等同于"任务完成"：角色收到 prompt 且回了话，不等于它想对了、做对了；step 盖戳 success 必须基于产出内容成立，不能只看投递成功。
- ❌ 在 `engine.run_once` 中把 `_send_to_role` 的返回值当 void 处理、无条件盖戳 `notified`：`_send_to_role` 投递被并发守卫/变量校验拦截时返回 `False`，调用方**必须**据此决定 step 状态机（投递失败 → 不盖戳 → 下轮重试）。否则 step 永久停在空 `step_results` 的 running 状态，触发 `_cleanup_stale_workflows` "running 且 sr 空>1h" 静默误杀（bus #728c6e4307c1 根因）。

## 代码布局（行数 2026-08-23 实测 `wc -l`）
```
src/
  routing/
    router.py         ── 路由表 + produce/consume 推导（361 行）
    routes.py         ── 路由表定义 + route_all + route_to_ccs（612 行）
    auto.py           ── 自动路由消费 + investigator 分派（260 行）
    polling.py        ── 消息轮询 + cursor（88 行）
    rdb.py            ── 路由表 SQLite 持久化 + 审计（293 行）
    survival_monitor.py ── 三层存活监测 L1/L2/L3（640 行）
    consistency.py    ── 路由一致性检查（124 行，生长接入 daemon）

  liveness.py         ── 角色存活/忙检测统一模块 L0-L4（消费 survival 写的 health 文件）

  pipeflow/  ── engine.py 已拆分为以下 mixin，不再有单体 engine
    engine.py         ── 编排 + CLI（1024 行，导入下方 mixin）
    step_handler.py   ── 步骤处理（1634 行）
    role_lifecycle.py ── 角色投递生命周期 + _send_to_role（1153 行）
    task_generator.py ── 任务生成（846 行）
    wf_lifecycle.py   ── 工作流生命周期（1969 行）
    daemon.py         ── 工作流守护 while 循环（314 行）
    pipeflow/db.py    ── 工作流三层 SQLite 持久化（327 行）
    integrity.py      ── 完整性提醒（旁路提醒）

  lifecycle/
    manager.py        ── 工作流状态机（1307 行）

  workflow/
    client.py         ── CCS 角色工作流客户端（515 行）
    gateway.py        ── 工作流网关门禁（188 行）
    workflow/db.py    ── 工作流数据访问（96 行）
    sync.py           ── 工作流同步（65 行）

  p0_exemption.py     ── P0 豁免通道（coordinator/lr/pm）
  config_loader.py    ── 配置加载（YAML + env override + 热重载）
  eval_checker.py     ── 安全白名单命令执行检查
  eval_consistency.py ── 评估一致性
  output_validator.py ── 输出格式验证
  contract_updater.py ── 合约同步（自改进闭环，见 grow-unused-logic 需求）
  conversation_monitor.py ── 对话监控
  cron_scheduler.py   ── cron 调度器
  drift_detector.py   ── 架构漂移检测
  routing_daemon.py   ── 路由守护进程
  reliability.py      ── 熔断/重试/心跳/TTL/Metrics
  reliability_core.py ── 可靠性核心实现
  template_registry.py── 模板注册中心
  task_evidence.py    ── 任务证据链
  workflow_metrics.py ── 工作流指标
  paths.py            ── 路径常量

注：step 终态含 `delivery_blocked`（投递达上限暂停，engine.py close_wf 收敛行级 status）。
```
```

### P0 豁免快速操作
```bash
# 创建 P0 任务（仅 coordinator/lr/pm；p0_reason ≥15 中文字符）
PYTHONPATH=src python3 -c "from p0_exemption import P0Exemption; e=P0Exemption(role='coordinator'); e.create_p0_task(title='紧急修复', description='修复说明', assignee='engineer', initiator_role='coordinator', p0_reason='紧急故障需要立即修复处理')"

# 审计扫描
PYTHONPATH=src python3 -c "from p0_exemption import P0Exemption; P0Exemption().p0_audit_scan()"
```


### P0_draft 阶梯机制

P0_draft 是工程师等角色可标记的预备 P0 任务，通过自动化阶梯机制防止遗漏：

**角色权限**：
- `ALLOWED_P0_ROLES = {"coordinator", "lr"}` — 可标记正式 P0（全天候）
- `ALLOWED_DRAFT_ROLES = {"engineer", "pg", "qa", "devops", "reviewer", "scout", "writer"}` — 可标记 P0_draft
- `pm` 仅在 08:00–22:00 工作时段可标记 P0_draft（`_check_pm_work_hours`）

**状态机** (`p0_state` 列：`NULL / draft / escalated / confirmed / downgraded`)：
- `mark_p0_draft()` — 仅允许角色可标记，`reason ≥15` 字符，task 非 completed
- `confirm_p0()` — coordinator/lr 将 `draft/escalated → confirmed`
- `downgrade_p0()` — coordinator/lr/system 将 P0_draft → P1（`priority=1`）
- `extend_deadline()` — 单次 ≤30min、累计 ≤2h，修改 `p0_marked_at` 后移窗口，审计结构化记录防预算绕过

**阶梯扫描 `p0_audit_scan()`** (每 30 分钟 daemon 调用)：
- 1h 未确认 → `p0_state='escalated'`（blocker 通知 supervisor 介入）
- 50min（窗口 -10min）发预警，5min 窗口 + 30min 幂等去重防重复
- 4h 超时（draft 或 escalated）→ `downgrade_p0()` 自动降级为 P1

**升级链 `_check_escalation_chain()`** (on-demand / 手动触发)：
- 6h+ → 通知 supervisor（blocker 分类）
- 12h+ → `@everyone`
- 跳过 `confirmed / downgraded / completed`

**运行时接线**：
- `routing_daemon.py` 每 30 分钟 (`P0_CHECK_INTERVAL=1800`) 调用 `p0_audit_scan()`，自动驱动阶梯机制
- 如需手动触发升级链：`PYTHONPATH=src python3 -c "from p0_exemption import P0Exemption; P0Exemption()._check_escalation_chain()`"

## 上下文溢出自动 /compact（role_lifecycle）

`_check_context_overflow()` 每 10s 轮询，两路信号**任一命中**即向该角色 pane 发 `/compact`：

1. **pane 措辞**（第一判据）：`capture-pane -S -500` 抓输出匹配
   `Context limit reached` / `Context low` / `Prompt is too long` / `Context N% remaining`。
2. **transcript 回退**（`_transcript_shows_overflow`）：读角色最新 transcript 尾部 64KB，
   取最新 `isApiErrorMessage` 事件的 ts，**600s 内**算刚发生。

第 2 路存在的原因：claude TUI 跑在**备用屏**（`alternate_on=1`），该屏**无 scrollback**
——`-S -500` 与 `-S -5000` 实测返回逐字节相同的视口内容，故第 1 路只能看到当前视口。
scout 实测在 `Prompt is too long` 终止后 1s 才被检测到，此时 agent 已死、`send-keys` 无处可发
（bus #361060）。transcript 是落盘事实，不受视口与进程存活影响。

**按时间而非距文件尾的字节数判断**：实测错误行距文件尾 7.8KB~10.8MB（35 会话样本）、
最远 4111 行，任何固定字节/行数窗口都会漏。按 ts 判"多久前"同时保证旧错误不无限期重发。

- 冷却 `_OVERFLOW_COOLDOWN=300s`；时效 `_OVERFLOW_ERR_MAX_AGE=600s`
- 回归守卫：`tests/test_context_overflow.py`（含 transcript 四例）
- **改了本逻辑需重启 `session-pipeline.service`** —— daemon 无热重载

## SQLite 运维
生产 DB 位于 `~/.hermes/data/state/workflows.db`。SQLite **不自动回收** DELETE/REPLACE 后的空闲页面——历史行删多了会积累 free 页，文件暴胀至 600MB+ 而表格仅数十行（实测 45 行 / 610MB，99.6% free 页）。

- **监测阈值**：`freelist_count / page_count > 0.5` 或单一 DB > 50MB 视为膨胀。
- **恢复**：`VACUUM;`（会新建事务写整个 DB，期间 daemon 应停。VACUUM 前 `cp` 备份）。
- **检查命令**：
```bash
python3 - <<'EOF'
import sqlite3, os
c=sqlite3.connect(os.path.expanduser("~/.hermes/data/state/workflows.db"))
pc=c.execute("PRAGMA page_count").fetchone()[0]; fl=c.execute("PRAGMA freelist_count").fetchone()[0]
print("freelist ratio=%.1f%%"%(fl/pc*100))
EOF
```

## 测试
```bash
cd ~/session-pipeline && python3 -m pytest tests/ -x -q
python3 -m py_compile src/*.py src/*/*.py
```

### 手工验证必须走隔离（防止污染生产）

**手工跑 `SurvivalMonitor().tick()` 等只读验证会直接写生产** —— `tick()` 无 dry-run，
会往生产 bus 写 fact、往 `~/.hermes/run/ccs-health/` 写状态文件。R71 曾因此写入 10 条
合成 fact（#373660–373669，事后手工删除）。

三个口子全部重定向后才安全（缺任一个都会碰生产）：

```bash
CCS_HEALTH_DIR=/tmp/iso/health \
HERMES_BLACKBOARD_DB=/tmp/iso/bb.db \
CCS_SENTINEL_DIR=/tmp/iso/sentinels \
python3 -c "import sys; sys.path.insert(0,'src'); from routing.survival_monitor import SurvivalMonitor; SurvivalMonitor().tick()"
```

实测（2026-10-16）：三者齐备时生产 bus 增长 **+0 字节**、生产哨兵无变化。
`SENTINEL_DIR` 原先硬编码在 `paths.py`，本轮补上 env 口子才凑齐三重隔离。
