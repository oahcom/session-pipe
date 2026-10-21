# Session Pipeline

> 更新: 2026-10-22 | 受众: 新开发者 (5min)

Session 生态的**路由层**——自动感知 bus 新消息，按优先级分发给对应 CCS 消费。

```
hermes-session-roles → session-launcher → session-pipeline (本项目)
    (定义层)             (执行层)             (路由层)
                                                    ↓
                                               Sister Bus
```

## 快速开始

```bash
cd /home/administrator/session-pipeline

# 验证导入（必须带 PYTHONPATH=src，项目未安装为包）
PYTHONPATH=src python3 -c "
from routing.router import get_router
from reliability import health_check
from pipeflow.engine import WorkflowEngine
print('All imports OK')
"

# 跑测试
python3 -m pytest tests/ -q
```

## 核心能力

| 能力 | 模块 | 一句话 |
|------|------|--------|
| 路由映射 | `routing/router.py` | 从 roles_export.json 推导 produce/consume |
| 自动分发 | `routing/auto.py` | 轮询 bus → 按优先级分发给 CCS |
| 工作流引擎 | `pipeflow/engine.py` | 加载 `~/.hermes/workflows/*.json`（实测 28 个模板） |
| 步骤状态机 | `lifecycle/manager.py` | 审批/回滚/升级/重分配 |
| 可靠性 | `reliability.py` | 重试/熔断/心跳/TTL/Metrics |

## CLI

```bash
# 路由
PYTHONPATH=src python3 -m routing.auto --status        # 队列状态（缺省行为同）
PYTHONPATH=src python3 -m routing.auto --route-all     # 路由所有
PYTHONPATH=src python3 -m routing.auto --health        # 健康检查
PYTHONPATH=src python3 -m routing.router list          # 路由表

# 工作流
PYTHONPATH=src python3 -m pipeflow.engine list                # 列出模板
PYTHONPATH=src python3 -m pipeflow.engine start <name>        # 启动
PYTHONPATH=src python3 -m pipeflow.daemon --interval 10       # 工作流守护循环
PYTHONPATH=src python3 -m routing_daemon                      # 路由守护进程
```

注意：`routing.auto --daemon` 只打印一次 status 后退出（`auto.py:219-221`），不是守护模式；常驻进程是 `routing_daemon.py`。

## 项目结构

```
src/
├── routing/           # 路由系统（router/auto/routes/rdb/polling/survival_monitor/consistency）
├── pipeflow/          # 引擎（engine + 4 个 mixin + daemon；无 db.py，DB 在 workflow/db.py）
│   （另有 eval_*/drift_detector/routing_daemon/task_evidence/liveness/template_registry/p0_exemption/paths/reliability_core/review_gate/output_validator/contract_updater/conversation_monitor/cron_scheduler）
├── lifecycle/         # 步骤状态机
├── workflow/          # 工作流客户端/网关/同步
├── reliability.py     # 可靠性基础设施
└── config_loader.py   # 配置加载
```

## 配置

`config/config.yaml` — 见 `docs/TECHNICAL_DEEP_DIVE.md` §3.7 配置块表。

## 进阶文档

见 [docs/DOCS.md](docs/DOCS.md)（文档索引，唯一入口）。

## 协作红线

> 完整规则见 [AGENTS.md](AGENTS.md)

1. 不直接创建 CCS
2. 不修改 bus_protocol 核心逻辑
3. 路由映射从 roles_export.json 自动生成
4. 消息推送前检查 CCS 是否运行
5. 零外部依赖
6. 每个修改必须通过测试
