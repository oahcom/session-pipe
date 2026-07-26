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
- 模板在 `templates/` 目录注册
- pipeline 自身运行工作流状态机（不依赖 launcher）
- 工作流完成/失败通过 `ccs.py send`（subprocess）通知角色

### 验证命令
```bash
# 测试路由规则
python3 tests/test_routing.py -x -q

# 检查路由表
python3 src/route_table.py --check

# 健康检查
python3 src/health.py --check
```

### 禁止操作
- ❌ 直接调 launcher 的 API（只能用 subprocess ccs.py send）
- ❌ 修改角色定义（那是 session-roles 的事）
- ❌ 重启 gateway（SIGKILL 中断长连接）

## 代码布局
```
src/
  router.py         ── 主路由引擎（消息→角色分发）
  routes.py         ── 路由表定义
  rdb.py            ── 路由表持久化
  auto.py           ── 自动路由推导
  polling.py        ── 轮询调度
  survival_monitor.py ── 存活监测

  pipeflow/
    engine.py       ── 工作流执行引擎
    daemon.py       ── 工作流守护
    db.py           ── 工作流持久化

  lifecycle/
    manager.py      ── 工作流状态机

  workflow/
    client.py       ── CCS 角色使用的工作流客户端
    gateway.py      ── 工作流网关门禁
    db.py           ── 工作流数据访问

  p0_exemption.py   ── P0 豁免通道（coordinator/lr/pm）
  config_loader.py  ── 配置加载
  eval_checker.py   ── 安全白名单命令执行检查
  output_validator.py ── 输出格式验证
  contract_updater.py ── 合约同步
  cron_scheduler.py ── cron 调度器
  drift_detector.py ── 架构漂移检测
  reliability.py    ── 熔断/重试
  paths.py          ── 路径常量
```

### P0 豁免快速操作
```bash
# 创建 P0 任务（仅 coordinator/lr/pm）
python3 -c "from p0_exemption import P0Exemption; P0Exemption().create_p0_task('指定角色', 3600, '紧急修复原因（≥15字符）')"

# 审计扫描
python3 -c "from p0_exemption import P0Exemption; P0Exemption().p0_audit_scan()"
```

## 测试
```bash
cd ~/session-pipeline && python3 -m pytest tests/ -x -q
python3 -m py_compile src/**/*.py
```
