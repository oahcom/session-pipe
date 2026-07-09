# 测试工程师角色、任务与工作流

> 基于以下来源研究蒸馏：
> - **Mike Bland** — *Goto Fail, Heartbleed, and Unit Testing Culture* (martinfowler.com, 2014) — Google 测试文化实践
> - **Martin Fowler** — *Software Testing Guide* / *Self-Testing Code* / *Test Pyramid* (martinfowler.com)
> - **Ham Vocke / ThoughtWorks** — *The Practical Test Pyramid* (martinfowler.com, 2018)
> - **Rouan Wilsenach / ThoughtWorks** — *QA in Production* (martinfowler.com, 2017)
> - **Martin Fowler** — *Mocks Aren't Stubs* / *Test Double* / *Integration Test* / *Contract Test*
> - **Google Testing Blog** — Testing on the Toilet / Test Certified / SET vs TE role definitions
> - **Kent Beck** — *Test-Driven Development* / Extreme Programming 测试实践
> - **Goto Fail (CVE-2014-1266)** / **Heartbleed (CVE-2014-0160)** — 测试缺失导致的灾难案例

---

## 0. 本项目中 SET 与 TE 的职责划分

### 0.1 角色对照表

| 维度 | SET (Software Engineer in Test) | TE (Test Engineer) |
|------|--------------------------------|--------------------|
| **靶子** | **测代码** — 测试框架/基础设施/CI | **测产品** — 功能/场景/用户体验 |
| **产出** | `test_helpers.py` / `run.py` / CI 配置 / mock 工具 | 测试场景清单 / Bug 报告 / TEST_ENGINEER.md |
| **日常** | 加测试工具函数 / 修 flaky test / 加覆盖分析 | 跑场景 / 写报告 / 探索测试 / 回归验证 |
| **问什么** | "这个函数怎么测？" | "这个功能到底对不对？" |
| **交付物** | 测试基础设施代码 | 执行报告 + Bug 清单 + 覆盖盲区 |

### 0.2 项目文件归属

| 文件 | 责任人 | 说明 |
|------|--------|------|
| `tests/test_helpers.py` | **SET** | 测试工具库（临时 DB / Bus 隔离 / TestSession / 场景注册表） |
| `tests/run.py` | **SET** | 统一测试运行器（SET 维护，TE 使用） |
| `tests/test_router.py` | **TE** | 路由逻辑测试场景 |
| `tests/test_integration.py` | **TE** | 集成测试场景 |
| `tests/test_role_interaction.py` | **TE** | 角色交互测试场景 |
| `docs/TEST_ENGINEER.md` | **TE** | 角色定义 + 场景库（本文件） |
| `docs/TEST_WORKFLOW.md` | **TE** | 工作流测试文档 |
| CI 配置 | **SET** | 自动化执行 + 报告 |

---

## 0A. SET — 任务清单

SET (Software Engineer in Test) **测代码**。对测试基础设施的质量负责。

### SET 任务列表

| ID | 任务 | 频次 | 产出 |
|----|------|------|------|
| SET-01 | 维护 `test_helpers.py`：增删改测试工具函数 | 按需 | `test_helpers.py` |
| SET-02 | 维护 `tests/run.py`：统一运行器 | 按需 | `run.py` |
| SET-03 | 覆盖盲区分析：`python3 tests/run.py --coverage` | 每周 | 覆盖报告 |
| SET-04 | 修 flaky test：诊断不稳定测试并修复 | 发现即修 | 测试稳定 |
| SET-05 | 加 mock / stub / fixture：给 TE 提供新的测试工具 | 按需 | test_helpers 新增函数 |
| SET-06 | 配置 CI：自动化运行 + 结果通知 | 一次性+维护 | CI 配置 |
| SET-07 | 测试性能优化：减少测试执行时间 | 按需 | 运行时间降低 |
| SET-08 | 评估测试框架：是否需要引入 pytest/unittest | 按需 | 决策记录 |

---

## 0B. TE — 任务清单

TE (Test Engineer) **测产品**。对功能的正确性负责。

### TE 任务列表

| ID | 任务 | 频次 | 产出 |
|----|------|------|------|
| TE-01 | 跑全部自动化测试：`python3 tests/run.py` | 每次改动后 | 执行结果 |
| TE-02 | 按场景库执行手动探索测试 | 每次发布前 | 执行日志 |
| TE-03 | 写 Bug 报告（按 §3.3 格式） | 发现即写 | Bug 报告 |
| TE-04 | 回归验证：修复后重新执行相关场景 | 修复后 | 验证结果 |
| TE-05 | 更新场景清单：新增测到的盲区 | 每次测试轮次 | 场景库更新 |
| TE-06 | 更新 TEST_ENGINEER.md：沉淀测试知识 | 每轮 | 文档更新 |
| TE-07 | 更新 TEST_WORKFLOW.md：工作流文档 | 架构变更时 | 文档更新 |

---

## 0C. SET — 工作流

### 场景：TE 需要新的测试工具

```
TE: "这个场景需要一个mock数据库的工具"
   ↓
SET-01: 分析需求 → 在 test_helpers.py 加函数 → 
        写使用示例 → 通知 TE
   ↓
TE: 验收工具可用
```

### 场景：覆盖盲区分析

```
SET-03: python3 tests/run.py --coverage
   ↓
发现 workflow_db.py 有新增函数未被覆盖
   ↓
SET-01: 如果是因为缺少工具导致无法测，加工具函数
   ↓
通知 TE: "workflow_db.py 新增了 X 函数，需要补场景"
   ↓
TE-02: 写场景 → TE-01: 验证通过
```

### 场景：flaky test 修复

```
SET-04: 发现 test_X 间歇性失败
   ↓
诊断根因: 并发冲突 / 外部依赖不稳定 / 时序问题
   ↓
修复: 加 retry / 加隔离 / 修断言
   ↓
SET-04: 连续跑 10 次确认稳定
```

---

## 0D. TE — 工作流

### 场景：版本发布前测试

```
TE-01: python3 tests/run.py                    # 全部自动化测试
   ↓
全部通过
   ↓
TE-02: 按 §4 场景库手动执行探索测试
   │
   ├─ 正常路径  → 全部 PASS
   ├─ 异常路径  → 发现 P1 bug: 删除不存在 workflow 返回了 True
   └─ 边界条件  → 全部 PASS
   ↓
TE-03: 写 Bug 报告 (格式见 §3.3)
   ↓
开发修复
   ↓
TE-04: 回归验证
   ↓
TE-05: 更新场景清单 — 把本次 bug 对应的场景加入
   ↓
TE-06: 更新 TEST_ENGINEER.md — 沉淀发现
```

### 场景：日常回归

```
TE-01: python3 tests/run.py                    # 跑自动化
   ↓
有失败
   ↓
TE-03: 分析是真实 bug 还是测试本身的问题
   ├─ 真实 bug → 写报告 → 通知开发修复
   └─ 测试问题 → 通知 SET 修复测试
   ↓
TE-04: 修复后回归验证
```

### 场景：架构变更后的全量测试

```
架构变更: 新增 workflow_daemon.py
   ↓
TE-07: 更新 TEST_WORKFLOW.md → 添加 daemon 测试章节
   ↓
TE-01: 跑现有测试 → 确认回归覆盖
   ↓
TE-02: 探索测试 workflow_daemon
   ↓
发现覆盖盲区 → TE-05 更新场景清单
   ↓
通知 SET: "daemon 需要 socket mock 工具" → SET-05
```

---

## 1. 测试工程师的角色

### 1.1 角色定位

参考 Google 的测试角色体系（来源: Mike Bland, *Goto Fail, Heartbleed, and Unit Testing Culture*, martinfowler.com 2014），现代测试工程有三种角色：

| 角色 | 大厂对应 | 职责 |
|------|---------|------|
| **SET (Software Engineer in Test)** | Google SET | 写测试基础设施、测试框架、CI 系统。**测试代码而非测试产品** |
| **TE (Test Engineer)** | Google TE / 字节跳动 QA | 手动探索测试 + 自动化场景设计。**测试产品而非测试代码** |
| **SDET (Software Development Engineer in Test)** | Microsoft SDET | 兼具开发与测试能力，写自动化工具 + 做测试设计 |

**关键洞察**（来自 Mike Bland 对 Google 测试文化的描述）：

> "测试工程师不是写测试代码的人，而是质量守护者。Google 的经验表明，测试文化需要从底层培养：'Testing on the Toilet'（厕所测试知识卡片）、Test Certified 认证体系、Test Mercenaries（测试雇佣兵）模式。"

### 1.2 Google 测试文化的关键实践

| 实践 | 来源 | 说明 |
|------|------|------|
| **Testing on the Toilet** | Google (Mike Bland) | 在厕所张贴 1 页测试技巧，每期一个主题，持续 10+ 年 |
| **Test Certified** | Google | 团队自评测试成熟度（1-5 级），达到 4+ 级才有发布权限 |
| **Test Mercenaries** | Google | 高级测试工程师轮岗到其他团队，帮助建立测试 |
| **Small/Medium/Large 测试金字塔** | Google | Small = 单元测试，Medium = 集成测试，Large = 端到端 |

### 1.3 测试金字塔

来源: Mike Cohn → Martin Fowler → Ham Vocke / ThoughtWorks

```
        /\           E2E / Large (少量)
       /  \          Integration / Medium (一些)
      /    \
     /______\        Unit / Small (大量)
```

**规则**（Ham Vocke, *The Practical Test Pyramid*, 2018）：
1. 写不同粒度的测试
2. 越是上层测试越少
3. 避免"测试冰淇淋"（上层多下层少）

---

## 2. 测试工程师的任务

### 2.1 测试分类框架

来源: Martin Fowler, *Software Testing Guide* / *Test Pyramid*

| 类别 | 粒度 | 目的 |
|------|------|------|
| **单元测试 (Unit Test)** | 函数/类 | 验证独立逻辑单元的正确性 |
| **集成测试 (Integration Test)** | 模块间 | 验证不同模块/服务协同工作 |
| **契约测试 (Contract Test)** | 服务间 | 验证服务接口的消费者/提供者契约一致 |
| **端到端测试 (Broad Stack / E2E)** | 全系统 | 验证系统整体功能 |
| **探索测试 (Exploratory Testing)** | 无预设 | 快速学习+设计+执行的循环 |

**关键区别**（Fowler, 2018）：集成测试应该是"窄"的——一次只测一个集成点，而不是启动所有服务。

### 2.2 负面测试（业界共识）

来自 Heartbleed 漏洞的教训（Mike Bland, 2014）：

> "测试代码做了什么固然重要，但测试代码**不应该做什么**更重要。"

异常路径测试清单：
- 依赖服务崩溃 → 降级行为
- 参数传错 → 优雅拒绝
- 并发冲突 → 数据不丢失
- 重复执行 → 幂等
- 持久化文件被删 → 重建/报错
- 输入超长 → 截断/拒绝
- 空输入 → 默认值/错误

### 2.3 QA in Production

来源: Rouan Wilsenach / ThoughtWorks (*QA in Production*, 2017)

> "测试只能帮你发现你预期会发生的缺陷，但很多生产缺陷都是意外。"

| 手段 | 用途 |
|------|------|
| **日志 (Logging)** | 结构化日志 + 级别（ERROR/WARN/INFO）→ 可搜索日志栈 |
| **指标 (Metrics)** | statsd/DataDog/Prometheus → 聚合数据 |
| **预警 (Alerting)** | 阈值触发 → 通知团队 |
| **仪表盘 (Dashboards)** | 实时可视化 → 趋势发现 |
| **合成监控 (Synthetic Monitoring)** | 生产环境跑自动化测试 → 检测业务需求失败 |

---

## 3. 测试工程师的工作流

### 3.1 单轮测试执行（Google 模式 + ThoughtWorks 模式整合）

来源: Mike Bland ("How to Change a Culture") + Ham Vocke (Test Pyramid)

```
Step 1: 理解改动
   读 diff / 读需求文档 / 问开发 "这改了啥"
   输出: 改动的范围清单

Step 2: 设计场景
   列出必须验证的场景（正常/异常/边界/并发）
   优先用测试金字塔：先 Unit → 再 Integration → 最后 E2E
   输出: 场景列表

Step 3: 执行测试
   按场景逐一执行
   每个场景记录: 输入 → 预期 → 实际 → PASS/FAIL
   输出: 执行日志

Step 4: 定位 Bug（参考 goto fail 分析过程）
   发现 FAIL → 缩小范围 → 读相关代码 → 确定根因
   "不是'这bug怎么来的'，而是'为什么测试没发现'"
   输出: Bug 报告（复现步骤 + 根因 + 建议修复方向）

Step 5: 验证修复
   开发修完 → 重新执行场景 3-5 次确认修好
   验证回归: 相关场景也跑一遍确认没打坏
   输出: 修复验证结果

Step 6: 沉淀
   更新场景列表（新增测到的盲区）
   记录有用的断言/检查点
   参考 Google: 写一篇 Testing on the Toilet 级别的知识卡片
   输出: 知识沉淀
```

### 3.2 Bug 定级

| 级别 | 定义 | 行动 |
|------|------|------|
| P0 | 核心功能不可用 / 数据丢失 / 崩溃 | 立即停线修复 |
| P1 | 重要功能异常 / 严重性能退化 | 当天修复 |
| P2 | 边缘功能异常 / 轻微 UX 问题 | 一周内修复 |
| P3 | 文档问题 / 非功能性建议 | 有空再修 |

### 3.3 Bug 报告格式

参考 Heartbleed 根因分析报告风格（Mike Bland, 2014）：

```
## Bug: [brief title]

- 环境: (Python 版本 / OS / DB 状态)
- 严重性: P0/P1/P2/P3
- 复现步骤:
  1. ...
  2. ...
  3. ...
- 预期行为: ...
- 实际行为: ...
- 根因分析: (定位到代码位置 + 为什么这个bug通过了现有测试)
- 建议修复: (方向性建议)
```

### 3.4 测试退出标准

来源: Google Test Certified 体系 + ThoughtWorks QA 实践

**通过条件（全部满足才算通过）：**

- [ ] 所有测试场景全部 PASS
- [ ] P0/P1 bug 全部修复并已验证
- [ ] P2 bug 已记录到跟踪系统
- [ ] 回归验证无新的 P0/P1
- [ ] 测试场景列表已更新（发现盲区已补充）

---

## 4. 参考资料

| 来源 | 链接 | 核心内容 |
|------|------|---------|
| Martin Fowler - Testing Guide | martinfowler.com/testing/ | 测试分类框架全集 |
| Ham Vocke - Practical Test Pyramid | martinfowler.com/articles/practical-test-pyramid.html | 测试金字塔实现指南 |
| Mike Bland - Unit Testing Culture | martinfowler.com/articles/testing-culture.html | Google 测试文化变革 |
| Rouan Wilsenach - QA in Production | martinfowler.com/articles/qa-in-production.html | 生产环境 QA 实践 |
| Kent Beck - TDD | martinfowler.com/bliki/TestDrivenDevelopment.html | 测试驱动开发 |
| Martin Fowler - Self-Testing Code | martinfowler.com/bliki/SelfTestingCode.html | 自测试代码概念 |
| Martin Fowler - Test Coverage | martinfowler.com/bliki/TestCoverage.html | 测试覆盖率的正确理解 |
| Martin Fowler - Integration Test | martinfowler.com/bliki/IntegrationTest.html | 集成测试定义 |
| Martin Fowler - Contract Test | martinfowler.com/bliki/ContractTest.html | 契约测试 |
