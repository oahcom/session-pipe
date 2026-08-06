# ponytail: 本文件任务列表硬编码。重构方案：从 bus 未消费消息动态生成。
#!/usr/bin/env python3
"""dispatch_tasks.py — 通过 Sister Bus 向 CCS 角色推送有描述性标题的任务。

用法:
  python3 dispatch_tasks.py [--dry-run]

设计说明：
  - 每个任务标题 ≥8 字符，描述具体做什么
  - 任务在角色职责范围内（按 CLAUDE.md 定义的 输入信号/产出分类）
  - 通过 bus task_spec + ccs send 推送，路由层自动创建 workflow
"""
import subprocess, sys, time
from pathlib import Path

BUS_CLIENT = Path.home() / ".hermes" / "scripts" / "bus_client.py"
CCS_CLI = Path.home() / "session-launcher" / "src" / "ccs.py"

# 角色职责映射（标题必须描述性、在角色职责内）
ROLE_TASKS = {
    "engineer": [
        "审查 session-pipeline 路由层代码并修复潜在死锁",
        "为 workflow/client.py 添加去重防御性边界检查",
        "优化生存监控 L2 检测中 tmux pane 活动轮询频率",
        "重构 route_to_ccs 中 Bus 消费异常重试逻辑",
        "实现约束 based 任务标题门禁自动测试用例",
        "清理 session-pipeline 中遗留的 dead code import",
        "审查 lifecycle manager 状态机转换是否有遗漏路径",
        "为 task_evidence.py 添加按时间范围过滤的功能",
        "在 engine.py 中为 run_once 添加异常上下文日志",
        "检查 session-launcher 工作流模板 JSON Schema 完整性",
    ],
    "qa": [
        "为 survival_monitor L2 检测全覆盖编写 pytest 测试",
        "验证标题门禁 _title_is_placeholder 的边界条件测试",
        "测试 CCS 路由层在 router 空消息时的降级行为",
        "检查 workflow/client.py create_task_v2 异常路径覆盖率",
        "验证 maintainer 模板 timeout 15 分钟后自动取消逻辑",
        "验证 routing daemon WAL 模式下并发写安全",
        "测试 lifecycle manager 中 step_done_ready 状态推进",
        "回归测试：标题占位符被拒绝后角色是否收到失败通知",
        "验证 route_to_ccs 中短消息构造描述性标题的逻辑",
        "场景测试：3 个角色同时有 task_spec 时的路由正确性",
    ],
    "reviewer": [
        "审查 survival_monitor.py L2/L3 fallback 逻辑正确性",
        "审查 routes.py 标题构造变更是否有潜在兼容问题",
        "审核 engine.py 代码热更新检测的性能开销",
        "检查 lifecycle/manager.py 中事务回滚是否有遗漏",
        "审核 task_evidence.py 输出数据结构完整性",
        "审查 pipeflow/engine.py 中僵尸回收的条件是否合理",
        "审核 workflow_metrics.py 指标的 SQL 查询准确性",
        "检查 client.py 的去重逻辑在并行场景下的正确性",
        "审查 maintainer 模板超时参数调整的实际影响",
        "审核 session-pipeline 路由规则与角色定义的一致性",
    ],
    "writer": [
        "维护 session-pipeline 架构文档反映当前状态",
        "编写 survival_monitor L2 修复的技术说明文档",
        "编写标题门禁功能的使用指南和部署说明",
        "更新 CCS 工作流模板的创建和审核流程文档",
        "归档本 session 中完成的工作流优化记录",
        "维护 rdb.py 路由表持久化的接口文档",
        "为 workflow_metrics.py 编写命令行用法说明",
        "更新 chaos-engineering 部署清单中的端口配置文档",
        "记录 workflow daemon 的数据流和故障排除指南",
        "整合 routing 模块中所有异常处理的文档说明",
    ],
    "coordinator": [
        "审核 engineer 和 qa 的任务完成质量并推动闭环",
        "检查 reviewer 审查结果的分配和处理时效性",
        "检查各角色任务积压情况并调度优先级",
        "推动跨角色联调测试的协调和资源分配",
        "审查 P0 豁免通道使用是否符合策略",
    ],
    "scout": [
        "扫描 session-pipeline 仓库近期 commit 发现架构变更",
        "检查 Sister Bus 近期未读消息中发现工作流信号",
        "探测 routing 层是否有新分类消息路由异常",
        "扫描 lifecycle 数据库中积压的孤儿 workflow 实例",
        "侦测 9Router 新版本的分层路由潜在影响",
    ],
    "maintainer": [
        "检查 systemd 服务状态确保核心 daemon 无异常退出",
        "巡检 ~/.hermes/state 目录中 DB 文件大小和健康度",
        "验证 PID 锁文件完整性防止双进程竞争",
        "检查 cron-worker 轮询日志中的异常模式",
        "审计 sister agent 会话心跳是否有缺失记录",
    ],
    "closer": [
        "汇总本 dispatch 周期的已完成任务并写总结到 bus",
        "跟踪未闭环任务的状态并推动进入完成状态",
    ],
    "curator": [
        "检查 bus 消息中可归档的架构决策和配置变更",
        "为 session-pipeline 近期修改更新知识库条目",
    ],
}

DRY_RUN = "--dry-run" in sys.argv
sent = 0
errors = 0


def write_bus(role: str, title: str):
    global sent, errors
    prompt = f"/goal\n\n## 任务背景\n{title}\n\n## 框架\n1. 读取 ~/ccs-workspaces/{role}/TASKS.md 了解当前上下文\n2. 执行任务\n3. 产出写入对应 bus 分类标记完成\n\n## 完成\n产出物写入后标记完成。"
    try:
        if DRY_RUN:
            print(f"[dry-run] {role}: {title[:60]}")
            return
        # 写 bus task_spec → 路由 daemon 自动消费并创建 workflow
        r = subprocess.run(
            [str(BUS_CLIENT), "write", "task_spec", f"needs_implementation @{role} {title[:60]}"],
            input=prompt.encode(), capture_output=True, timeout=10,
        )
        if r.returncode != 0:
            print(f"  ERROR: bus write failed: {r.stderr.decode()[:100]}")
            errors += 1
            return
        sent += 1
        time.sleep(0.5)  # 防止消息堆积
    except Exception as e:
        print(f"  ERROR: {e}")
        errors += 1


def main():
    print(f"=== CCS 批量任务推送 ===\n")
    total_tasks = sum(len(tasks) for tasks in ROLE_TASKS.values())
    print(f"计划: {total_tasks} 个任务, {len(ROLE_TASKS)} 个角色\n")

    # 按角色写入（先写出产出高的角色）
    for role, tasks in ROLE_TASKS.items():
        print(f"[{role}] 推送 {len(tasks)} 个任务")
        for title in tasks:
            write_bus(role, title)

    print(f"\n=== 完成: sent={sent}, errors={errors} ===")
    if sent > 0:
        print(f"子 agent 将在任务完成后独立评估收益")


if __name__ == "__main__":
    main()
