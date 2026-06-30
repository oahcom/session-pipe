# Session Pipeline 项目

## 项目概述
Sister Bus 角色间路由——A 角色写 bus → B 角色自动收到通知。
让 scout/developer/maintainer 的产出自动流向 consumer/closer/coordinator。

## 架构
```
src/
  router.py     → ROLE_ROUTING 映射 + unconsumed_by_role()
  auto_route.py → 自动路由：新消息 → 通知对应角色
```

## 关键决策
- 基于现有 bus_protocol.py Blackboard
- 不修改 bus_protocol 核心逻辑，只新增路由层
- 角色间路由映射存储为 JSON
