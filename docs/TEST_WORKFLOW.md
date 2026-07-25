# 测试工作流文档

> 更新: 2026-07-24 | 受众: 测试工程师 | 相关: [DOCS.md](DOCS.md)

## 如何运行全部测试

```bash
# session-pipeline（136 pass / 0 fail）
cd /home/administrator/session-pipeline
python3 -m pytest tests/ -v

# hermes-session-roles（24 pass）
cd /home/administrator/hermes-session-roles
python3 -m pytest tests/ -v

# session-launcher（22 pass / 2 fail）
cd /home/administrator/session-launcher
python3 -m pytest tests/ -v
```

## 已实现测试

### session-pipeline（136 pass / 0 fail）

| 测试文件 | 数量 | 通过 | 失败 | 覆盖模块 |
|---------|------|------|------|----------|
| test_workflow_db.py | 36 | 36 | 0 | Template/Task/Workflow CRUD, 权限, 状态同步, 进度计算, 链式创建, 日志审计 |
| test_workflow_engine.py | 31 | 31 | 0 | 加载, start/status/cancel, tick/advance, 超时/重试, 条件步骤, workspace_summary, 持久化 |
| test_cross_component.py | 17 | 17 | 0 | DB+Client 一致性, DB+Engine 联动, 并发操作, 权限规则, 链式生命周期, Bus联动 |
| test_router.py | 13 | 13 | 0 | router.py, auto_route.py |
| test_integration.py | 12 | 12 | 0 | bus_protocol, auto_route, reliability, router |
| test_role_interaction.py | 10 | 10 | 0 | bus 通信, 角色交互, 路由优先级 |
| test_engine_e2e.py | 6 | 6 | 0 | CCS 模拟, 超时升级, 多步骤完成, 提醒心跳 |
| test_router_extend_bh.py | 7 | 7 | 0 | Browser Harness 路由扩展 |
| test_workflow_daemon.py | 4 | 4 | 0 | daemon_loop, PID 锁 |

### hermes-session-roles（24 pass）

| 测试文件 | 数量 | 覆盖模块 |
|---------|------|----------|
| test_search.py | 15 | 中文/英文/同义词搜索 |
| test_registry.py | 9 | Role/Persona 加载、注册、验证 |

### session-launcher（22 pass / 2 fail）

| 测试文件 | 数量 | 通过 | 失败 | 覆盖模块 |
|---------|------|------|------|----------|
| test_ccs_socket.py | 12 | 12 | 0 | Streamer capture, CLI send, socket 监听 |
| test_e2e_mock.py | 12 | 10 | 2 | Session 生命周期端到端 |

失败详情：
- `test_e2e_mock.py::test_main_no_work` — assert 2 == 0（exit code 不符预期）
- `test_e2e_mock.py::test_main_shell_output_format` — export 行输出为空

**总计：182 pass / 2 fail（跨三个项目）**

## 手动测试点

| 场景 | 原因 |
|------|------|
| feed socket 断线重连 | 需真实 socket 服务 |
| SIGTERM 信号处理 | 需真实 daemon 进程 |
| 大数据量性能 (>10k 条) | 耗时较长 |
| DB 重启后状态恢复 | 需持久化 DB 测试 |
