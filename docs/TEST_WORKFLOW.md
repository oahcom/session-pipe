# 测试工作流文档

## 如何运行全部测试

```bash
cd /home/administrator/session-pipeline

# 全部测试（132 个）
python3 tests/test_router.py           # 路由逻辑（13 个）
python3 tests/test_integration.py      # 集成测试（12 个）
python3 tests/test_role_interaction.py  # 角色交互（10 个）
python3 tests/test_workflow_db.py       # WorkflowDB 数据库（36 个）
python3 tests/test_workflow_engine.py   # WorkflowEngine 执行引擎（30 个）
python3 tests/test_workflow_daemon.py   # WorkflowDaemon 守护进程（14 个）
python3 tests/test_cross_component.py   # 跨组件集成（17 个）
```

## 自动化测试覆盖情况

| 测试文件 | 数量 | 覆盖模块 |
|---------|------|----------|
| test_router.py | 13 | router.py, auto_route.py |
| test_integration.py | 12 | bus_protocol, auto_route, reliability, router |
| test_role_interaction.py | 10 | bus 通信, 角色交互, 路由优先级 |
| test_workflow_db.py | 36 | Template/Task/Workflow CRUD, 权限, 状态同步, 进度计算, 链式创建, 日志审计 |
| test_workflow_engine.py | 30 | 加载, start/status/cancel, tick/advance, 超时/重试, 条件步骤, workspace_summary, 持久化 |
| test_workflow_daemon.py | 14 | connect_feed, push_prompt, check_and_push, daemon_loop |
| test_cross_component.py | 17 | DB+Client 一致性, DB+Engine 联动, 并发操作, 权限规则, 链式生命周期, Bus联动 |

**总计：132 个测试**

## 手动测试点

| 场景 | 原因 |
|------|------|
| feed socket 断线重连 | 需真实 socket 服务 |
| SIGTERM 信号处理 | 需真实 daemon 进程 |
| 大数据量性能 (>10k 条) | 耗时较长 |
| DB 重启后状态恢复 | 需持久化 DB 测试 |
