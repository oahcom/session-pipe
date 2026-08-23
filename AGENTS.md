# Session Pipeline AGENTS.md

> 更新: 2026-07-24 | 受众: AI Agent + 维护者 | 相关: [docs/DOCS.md](docs/DOCS.md)

## 项目概述
Session 生态的**路由层**——自动感知 bus 新消息，按优先级分发给对应 CCS 消费。

## 整体架构
```
hermes-session-roles (定义层)
  ├──→ session-launcher (执行层)
  └──→ session-pipeline (路由+执行层 ← 本项目)
         └──→ launcher/ccs.py send  ← 唯一跨项目调用（subprocess）
```

**注意：不是三层流水线。pipeline 通过 subprocess `ccs.py send` 与 launcher 通信。pipeline 的路由数据来自 roles_export.json，不直接读角色 JSON。**

**铁律：不直接创建 CCS，不改 bus_protocol 核心逻辑。**

## Git 工作流
1. 禁止切换分支，始终在 main 分支工作
2. 小步提交，每完成一个逻辑单元立即 commit
3. 出错用新提交修复，不要 revert
4. 本地即生产环境

## 协作红线
1. 不直接创建 CCS——CCS 生命周期由 session-launcher 管理
2. 不修改 bus_protocol 核心逻辑——只新增路由层
3. 路由映射从角色 JSON 自动生成——不硬编码
4. 消息推送前必须检查 CCS 是否在运行
5. 零外部依赖（stdlib + 已有 bus_protocol）
6. 不使用 eval()——使用安全的 regex 模式匹配

---

## 自维护指令（Agent 按此执行）

### 1. 每次修改前：记录路由基线

```bash
PYTHONPATH=src python3 -c "
from routing.router import format_pipeline
print(format_pipeline())
" > /tmp/routing_before.txt
```

### 2. 修改后：验证路由 + 健康

```bash
# 路由表加载正常
PYTHONPATH=src python3 -c "
from routing.router import get_router; r = get_router()
assert len(r._routing) >= 25, f'路由表不完整: {len(r._routing)}'
print(f'OK: {len(r._routing)} roles')
"

# 可靠性层正常
PYTHONPATH=src python3 -c "
from reliability import CIRCUIT_BREAKER, health_check
assert CIRCUIT_BREAKER._state == 'closed', '熔断器异常'
print(f'Health: {health_check()[\"status\"]}')
"
```

### 3. 路由修改 7 维度验证

| 维度 | 检查内容 | 命令 |
|------|----------|------|
| D1 正确性 | produce/consume 映射准确 | 对比角色 JSON output_targets |
| D2 安全性 | 消息不泄露、不注入 | 无 eval/exec |
| D3 可维护性 | 配置化、不硬编码 | 检查 config.yaml 覆盖 |
| D4 性能 | 轮询间隔合理、无 N+1 | poll_interval=60s |
| D5 一致性 | 优先级表完整 | CATEGORY_PRIORITY 覆盖全部分类 |
| D6 可测试性 | 路由逻辑可 mock | db_path 参数注入 |
| D7 可靠性 | 重试/熔断/心跳/TTL 配置 | config.yaml 全部可调 |

### 4. 跨项目验证

```bash
# 路由 → 角色一致性
cd /home/administrator/hermes-session-roles
python3 src/validate_roles.py

# 路由 → Launcher 通信
cd /home/administrator/session-launcher
python3 src/ccs.py health
```

### 5. 每周自进化

- 检查 unconsumed 消息积压（`python3 src/routing/auto.py --status`）
- 检查路由命中率：哪些角色从未被路由到？
- 检查 TTL 清理是否正常运行
- 检查熔断器触发次数
- 评估是否需要新增消息分类

## 关键配置
| 参数 | 默认值 | 说明 |
|------|--------|------|
| bus.poll_interval | 60s | 轮询间隔 |
| retry.max_retries | 3 | 最大重试次数 |
| circuit_breaker.failure_threshold | 5 | 熔断阈值 |
| heartbeat.stale_threshold | 300s | 消费者超时 |
| ttl_pruner.max_age_days | 90 | 消息保留天数 |

## 6. 工作流模板治理

### 模板必填元数据
```bash
# 每个 ~/.hermes/workflows/*.json 必须包含以下 6 个字段：
trigger_scene=["编码实现", "bug修复"]   # 至少 1 条触发场景
allowed_initiators=["coordinator","pm"] # 至少 1 个允许发起角色
allowed_executors=["engineer"]          # 至少 1 个允许执行角色
max_duration_hours=24                   # 正整数
quality_standards="通过 code review..." # ≥8 字符质量标准

# 验证所有模板
PYTHONPATH=src python3 -m template_registry validate-all

# 验证单个模板
PYTHONPATH=src python3 -m template_registry validate <template.json>
```

### 创建模板流程
1. 认真填写 6 个元数据字段及每步的 `failure_patterns`
2. 运行 `validate-all` 确保校验通过
3. 用 `recommend` 验证推荐的场景覆盖符合预期

### 模板元数据缺失的后果
- 模板仍然可通过 `engine.start("name")` 显式启动
- 但 **TemplateRegistry.recommend() 不会推荐它**（无 trigger_scene 或角色不匹配时不可见）
- `_load_workflows()` 加载时会打印警告
