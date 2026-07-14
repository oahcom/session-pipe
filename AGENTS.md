# Session Pipeline AGENTS.md

## 项目概述
Session 生态的**路由层**——自动感知 bus 新消息，按优先级分发给对应 CCS 消费。

## 整体架构
```
hermes-session-roles  →  session-launcher  →  session-pipeline
  (定义层)              (执行层)              (路由层) ← 本项目
```

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
from router import format_pipeline
print(format_pipeline())
" > /tmp/routing_before.txt
```

### 2. 修改后：验证路由 + 健康

```bash
# 路由表加载正常
PYTHONPATH=src python3 -c "
from router import get_router; r = get_router()
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

- 检查 unconsumed 消息积压（`python3 src/auto_route.py --status`）
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
