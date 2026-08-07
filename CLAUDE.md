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
python3 tests/test_router.py -x -q

# 列出路由表（router CLI）
PYTHONPATH=src python3 -m routing.router list

# 健康检查
python3 src/pipeflow/health_check.py
```

### 禁止操作
- ❌ 直接调 launcher 的 API（只能用 subprocess ccs.py send）
- ❌ 修改角色定义（那是 session-roles 的事）
- ❌ 重启 gateway（SIGKILL 中断长连接）

## 代码布局
```
src/
  routing/
    router.py         ── 路由表 + produce/consume 推导（391 行）
    routes.py         ── 路由表定义 + route_all + route_to_ccs（409 行）
    auto.py           ── 自动路由消费 + investigator 分派（299 行）
    polling.py        ── 消息轮询 + cursor（87 行）
    rdb.py            ── 路由表 SQLite 持久化 + 审计（299 行）
    survival_monitor.py ── 存活监测（373 行）
    consistency.py    ── 路由一致性检查（120 行）

  pipeflow/
    engine.py         ── 工作流执行引擎（2171 行）
    daemon.py         ── 工作流守护 while 循环（108 行）
    db.py             ── 工作流三层 SQLite 持久化（328 行）
    health_check.py   ── 健康检查（67 行）

  lifecycle/
    manager.py        ── 工作流状态机（1090 行）

  workflow/
    client.py         ── CCS 角色工作流客户端（497 行）
    gateway.py        ── 工作流网关门禁（193 行）
    db.py             ── 工作流数据访问（92 行）
    sync.py           ── 工作流同步（55 行）

  p0_exemption.py     ── P0 豁免通道（coordinator/lr/pm）
  config_loader.py    ── 配置加载（YAML + env override + 热重载）
  eval_checker.py     ── 安全白名单命令执行检查
  eval_consistency.py ── 评估一致性
  output_validator.py ── 输出格式验证
  contract_updater.py ── 合约同步
  conversation_monitor.py ── 对话监控
  cron_scheduler.py   ── cron 调度器
  dispatch_tasks.py   ── 任务分发
  drift_detector.py   ── 架构漂移检测
  routing_daemon.py   ── 路由守护进程
  role_validator.py   ── 角色校验
  reliability.py      ── 熔断/重试/心跳/TTL/Metrics
  reliability_core.py ── 可靠性核心实现
  template_registry.py── 模板注册中心
  task_evidence.py    ── 任务证据链
  workflow_metrics.py ── 工作流指标
  paths.py            ── 路径常量
```

### P0 豁免快速操作
```bash
# 创建 P0 任务（仅 coordinator/lr/pm；p0_reason ≥15 中文字符）
PYTHONPATH=src python3 -c "from p0_exemption import P0Exemption; e=P0Exemption(role='coordinator'); e.create_p0_task(title='紧急修复', description='修复说明', assignee='engineer', initiator_role='coordinator', p0_reason='紧急故障需要立即修复处理')"

# 审计扫描
PYTHONPATH=src python3 -c "from p0_exemption import P0Exemption; P0Exemption().p0_audit_scan()"
```

## 测试
```bash
cd ~/session-pipeline && python3 -m pytest tests/ -x -q
python3 -m py_compile src/**/*.py
```
