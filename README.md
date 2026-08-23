# Session Pipeline

> 更新: 2026-07-24 | 受众: 新开发者 (5min)

Session 生态的**路由层**——自动感知 bus 新消息，按优先级分发给对应 CCS 消费。

```
hermes-session-roles → session-launcher → session-pipeline (本项目)
    (定义层)             (执行层)             (路由层)
                                                    ↓
                                               Sister Bus
```

## 快速开始

```bash
# 验证导入
cd /home/administrator/session-pipeline
PYTHONPATH=src python3 -c "
from routing.router import get_router
from reliability import health_check
from pipeflow.engine import WorkflowEngine
print('All imports OK')
"

# 跑测试
python3 -m pytest tests/ -v

# 查看路由
PYTHONPATH=src python3 -c "from routing.router import get_router; print(get_router().routing_summary())"
```

## 核心能力

| 能力 | 模块 | 一句话 |
|------|------|--------|
| 路由映射 | `routing/router.py` | 从角色 JSON 自动推导 produce/consume |
| 自动分发 | `routing/auto.py` | 轮询 bus → 按优先级分发给 CCS |
| 工作流引擎 | `pipeflow/engine.py` | 32 个模板 + 3 个复合链 |
| 步骤状态机 | `lifecycle/manager.py` | 审批/回滚/升级/重分配 |
| 可靠性 | `reliability.py` | 重试/熔断/心跳/TTL/Metrics |

## CLI

```bash
# 路由
PYTHONPATH=src python3 -m routing.auto               # 查看队列
PYTHONPATH=src python3 -m routing.auto --route-all   # 路由所有
PYTHONPATH=src python3 -m routing.auto --daemon      # 守护模式

# 工作流
python3 -m pipeflow.engine list                # 列出模板
python3 -m pipeflow.engine start <name>        # 启动
python3 -m pipeflow.engine daemon              # 守护
```

## 项目结构

```
src/
├── routing/           # 路由系统
├── pipeflow/          # 工作流引擎
├── lifecycle/         # 步骤状态机
├── workflow/          # 工作流客户端
├── reliability.py     # 可靠性基础设施
└── config_loader.py   # 配置加载
```

## 配置

`config/config.yaml` — 见 `docs/TECHNICAL_DEEP_DIVE.md` §3.8 完整配置表。

## 进阶文档

| 文档 | 受众 | 内容 |
|------|------|------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | 架构师 | 模块关系、数据流 |
| [docs/TECHNICAL_DEEP_DIVE.md](docs/TECHNICAL_DEEP_DIVE.md) | 高级开发者 | 实现原理、配置热重载 |
| [docs/SYSTEM_LANDSCAPE.md](docs/SYSTEM_LANDSCAPE.md) | 全栈工程师 | 三项目全景、端口、DB |
| [docs/TEST_WORKFLOW.md](docs/TEST_WORKFLOW.md) | 测试工程师 | 测试覆盖率、已知缺口 |
| [docs/WORKFLOW_TEMPLATE_SPEC.md](docs/WORKFLOW_TEMPLATE_SPEC.md) | 模板开发者 | Schema、审查、测试场景 |
| [PUBLIC_API.md](PUBLIC_API.md) | API 消费者 | 函数签名 |
| [docs/DOCS.md](docs/DOCS.md) | 维护者 | 文档治理、职责分工 |

## 协作红线

> 完整规则见 [AGENTS.md](AGENTS.md)

1. 不直接创建 CCS
2. 不修改 bus_protocol 核心逻辑
3. 路由映射从角色 JSON 自动生成
4. 消息推送前检查 CCS 是否运行
5. 零外部依赖
6. 每个修改必须通过测试
