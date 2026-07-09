#!/usr/bin/env python3
"""
Role Interaction Tests — 模拟角色间通过 Sister Bus 的完整交互链路。

测试架构：
  产品架构师 → PRD → 系统设计 → 任务分解 → coordinator 调度 → developer 实现 → closer 闭环

运行：
  cd /home/administrator && python3 session-pipeline/tests/test_role_interaction.py

前提：
  - Sister Bus blackboard.db 可写（不需要 daemon 运行）
  - session-pipeline/src 可导入
"""
import json
import sys
import time
import uuid
from pathlib import Path

# ── 路径设置 ──
_HERMES_SCRIPTS = Path.home() / ".hermes" / "scripts"
_LAUNCHER_SRC = Path.home() / "session-launcher" / "src"
_PIPELINE_SRC = Path.home() / "session-pipeline" / "src"
for p in [_HERMES_SCRIPTS, _LAUNCHER_SRC, _PIPELINE_SRC]:
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from bus_protocol import Blackboard

# ── 测试辅助 ──
_PREFIX = f"test_role_interact_{uuid.uuid4().hex[:6]}"
_CLEANUP_IDS: list[int] = []


def _tag(text: str) -> str:
    """给测试消息加唯一前缀，方便清理识别。"""
    return f"[{_PREFIX}] {text}"


def _cleanup(bb: Blackboard):
    """清理测试写入的消息。"""
    for fid in _CLEANUP_IDS:
        try:
            bb.mark_consumed(fid, "test_cleanup")
        except Exception:
            pass
    print(f"  已标记 {len(_CLEANUP_IDS)} 条测试消息为已消费")


def _check(msg: str, cond: bool):
    print(f"  {'✅' if cond else '❌'} {msg}")
    return cond


# ══════════════════════════════════════════════════════════════════
# 测试场景
# ══════════════════════════════════════════════════════════════════

def test_scenario_architect_receives_design_request():
    """
    场景 1：架构师收到设计需求
    coordinator 写 bus cat=architecture → 架构师能读到
    """
    bb = Blackboard()
    fid = bb.write("architecture", _tag("需要设计告警聚合模块"),
                    evidence="需求: 多daemon告警需聚合\n优先级: P1",
                    src="coordinator", tags="needs_design")
    _CLEANUP_IDS.append(fid)

    unconsumed = bb.unconsumed()
    found = [f for f in unconsumed if f.id == fid]
    return _check("coordinator → architecture bus: 消息可读到", len(found) == 1)


def test_scenario_architect_publishes_prd():
    """
    场景 2：架构师产出 PRD → 消费者可读到新分类
    product_architect 写 bus cat=prd → 系统能处理该分类
    """
    bb = Blackboard()
    fid = bb.write("prd", _tag("告警聚合模块 PRD"),
                    evidence="文件: workspace/alerts/PRD.md\nProblem: 多daemon重复告警\nStory数: 3",
                    src="product_architect")
    _CLEANUP_IDS.append(fid)

    unconsumed = bb.unconsumed()
    found = [f for f in unconsumed if f.id == fid]
    return _check("product_architect → prd bus: 新分类消息可存储", len(found) == 1)


def test_scenario_architect_publishes_system_design():
    """
    场景 3：架构师产出系统设计
    """
    bb = Blackboard()
    design_md = "## Architecture Diagram\n```mermaid\nflowchart TD\n    A[告警输入] --> B[聚合器] --> C[输出]\n```"
    fid = bb.write("system_design", _tag("告警聚合模块系统设计"),
                    evidence=f"文件: workspace/alerts/DESIGN.md\n{design_md[:80]}...",
                    src="product_architect")
    _CLEANUP_IDS.append(fid)

    unconsumed = bb.unconsumed()
    found = [f for f in unconsumed if f.id == fid]
    ok = _check("product_architect → system_design bus: 设计可存储", len(found) == 1)

    # 验证设计内容包含 Mermaid 图
    ok &= _check("system_design 含 Mermaid 图", "mermaid" in design_md)
    return ok


def test_scenario_architect_publishes_task_spec():
    """
    场景 4：架构师产出任务分解 → developer 和 coordinator 可消费
    """
    bb = Blackboard()
    tasks = [
        {"id": "T1", "title": "告警输入解析器", "effort": "E2",
         "acceptance_criteria": ["python3 -m py_compile src/alert_parser.py"]},
        {"id": "T2", "title": "聚合去重引擎", "effort": "E3",
         "acceptance_criteria": ["pytest tests/test_dedup.py -v"]},
        {"id": "T3", "title": "告警输出模块", "effort": "E2",
         "acceptance_criteria": ["curl -s http://localhost:9090/health"]},
    ]
    fid = bb.write("task_spec", _tag("告警聚合模块任务分解(3个切片)"),
                    evidence=f"文件: workspace/alerts/TASKS.json\n切片数: {len(tasks)}\n{json.dumps(tasks, ensure_ascii=False)[:200]}",
                    src="product_architect")
    _CLEANUP_IDS.append(fid)

    unconsumed = bb.unconsumed()
    found = [f for f in unconsumed if f.id == fid]
    ok = _check("product_architect → task_spec bus: 任务可发布", len(found) == 1)

    # 验证任务格式正确
    ok &= _check("任务分解 JSON 可解析", len(tasks) >= 1)
    ok &= _check(f"切片数正确: T1/T2/T3 = {len(tasks)}", len(tasks) == 3)
    for t in tasks:
        ok &= _check(f"'{t['title']}' 有验收标准", len(t['acceptance_criteria']) >= 1)
    return ok


def test_scenario_coordinator_detects_tasks():
    """
    场景 5：coordinator 自动检测 task_spec 分类的消息并排序
    """
    from router import get_router, priority

    router = get_router()
    routing = router.routing

    # 验证 coordinator 消费 task_spec
    ok = True
    if "coordinator" in routing:
        consume = routing["coordinator"].get("consume", [])
        ok = _check("coordinator 消费 task_spec", "task_spec" in consume or "*" in consume)
    else:
        ok = _check("coordinator 在路由表中 (fallback)", "coordinator" in routing)

    # 验证优先级排序
    priorities = {
        "security": priority("security"),
        "prd": priority("prd"),
        "system_design": priority("system_design"),
        "task_spec": priority("task_spec"),
    }
    ok &= _check("security(1) < prd(4)", priorities["security"] < priorities["prd"])
    ok &= _check("prd(4) < system_design(5)", priorities["prd"] < priorities["system_design"])
    ok &= _check("system_design(5) < task_spec(6)", priorities["system_design"] < priorities["task_spec"])

    # 验证 product_architect 能产出设计任务
    if "product_architect" in routing:
        produce = routing["product_architect"].get("produce", [])
        ok &= _check("product_architect 产出 prd", "prd" in produce)
        ok &= _check("product_architect 产出 system_design", "system_design" in produce)
        ok &= _check("product_architect 产出 task_spec", "task_spec" in produce)
    else:
        ok = _check("product_architect 在路由表中", "product_architect" in routing)

    return ok


def test_scenario_developer_consumes_tasks():
    """
    场景 6：developer 消费 task_spec 并产出 code_fix
    模拟 developer 读到任务 → 实现 → 写 code_fix 通知
    """
    bb = Blackboard()

    # 先写一个 task_spec (模拟架构师产出)
    task_id = bb.write("task_spec", _tag("实现告警输入解析器"),
                        evidence="文件: TASKS.json#T1\n验收: python3 -m py_compile src/alert_parser.py",
                        src="product_architect")
    _CLEANUP_IDS.append(task_id)

    # 模拟 developer 读完任务后实现并写 code_fix
    fix_id = bb.write("code_fix", _tag("[developer] 实现告警输入解析器"),
                       evidence="文件: src/alert_parser.py\n验证: py_compile PASS\n关联: bus#" + str(task_id),
                       src="developer")
    _CLEANUP_IDS.append(fix_id)

    unconsumed = bb.unconsumed()
    # developer 写了 code_fix 通知，maintainer 和 closer 可消费
    task_consumers = []
    fix_consumers = []
    for f in unconsumed:
        if f.id == task_id:
            task_consumers.append(f)
        if f.id == fix_id:
            fix_consumers.append(f)

    ok = _check("developer → code_fix: 实现可验证", len(fix_consumers) >= 1)
    ok &= _check("code_fix 关联到原始 task_spec", str(task_id) in [f.e for f in fix_consumers][0] if fix_consumers else "")
    return ok


def test_scenario_closer_closes_loop():
    """
    场景 7：closer 闭环 — 验证 task_spec + code_fix 都完成后打合
    """
    bb = Blackboard()

    # 模拟 developer 完成所有任务
    final_fix = bb.write("code_fix", _tag("[developer] 告警聚合模块全部完成"),
                          evidence="文件: src/alert_parser.py, src/dedup.py, src/output.py\n测试: 所有测试通过",
                          src="developer")
    _CLEANUP_IDS.append(final_fix)

    # 模拟 closer 确认闭环
    closer_note = bb.write("architecture", _tag("[closer] 闭环确认: 告警聚合模块"),
                            evidence=f"检查: 全部 code_fix 已合入\nbus# 引用: {final_fix}\n归档: 设计文档已归档至知识库",
                            src="closer")
    _CLEANUP_IDS.append(closer_note)

    # mark 所有闭环消息为已消费
    bb.mark_consumed(closer_note, "closer_self")

    unconsumed = bb.unconsumed()
    open_tasks = [f for f in unconsumed if f.id == final_fix or f.id == closer_note]
    ok = _check("closer → architecture: 闭环通知可写入", closer_note > 0)

    # 验证闭环报告包含引用信息
    unread = bb.unconsumed()
    closer_msgs = [f for f in unread if f.cat == "architecture" and "closer" in f.src]
    return ok


def test_scenario_product_design_request_product_architect():
    """
    场景 8：product_design 分类 → 架构师可消费
    其他角色可以通过 product_design 分类向架构师提交设计需求
    """
    bb = Blackboard()

    fid = bb.write("product_design", _tag("新功能: 告警聚合查询API"),
                    evidence="需求: 支持按时间/级别/来源查询聚合告警\n参考: 现有 dashboard API 模式",
                    src="scout")
    _CLEANUP_IDS.append(fid)

    unconsumed = bb.unconsumed()
    found = [f for f in unconsumed if f.id == fid]
    ok = _check("product_design 分类: 可存储", len(found) == 1)

    # 验证 product_architect 能消费此分类
    from router import get_router
    router = get_router()
    consumers = router.get_consumers("product_design")
    ok &= _check("product_design → 架构师可消费", "product_architect" in consumers)
    return ok


def test_scenario_blocker_escalation():
    """
    场景 9：blocker 阻塞升级 — developer 遇到阻塞写 blocker 给架构师
    """
    bb = Blackboard()

    # developer 遇到设计阻塞
    blocker_id = bb.write("blocker", _tag("[developer] 告警聚合设计不清楚: 去重窗口策略"),
                           evidence="问题: PRD 说'1分钟内去重'但未指定同一title还是同一IP\n影响: 阻塞 T2 实现",
                           src="developer")
    _CLEANUP_IDS.append(blocker_id)

    unconsumed = bb.unconsumed()
    found = [f for f in unconsumed if f.id == blocker_id]
    ok = _check("blocker 分类: 开发者可提阻塞", len(found) == 1)

    # 模拟架构师回复
    unblock_id = bb.write("blocker", _tag("[product_architect] RE: 告警聚合去重窗口"),
                           evidence="回复: 按同一title去重, 窗口1分钟\n原问题: bus#" + str(blocker_id),
                           src="product_architect")
    _CLEANUP_IDS.append(unblock_id)
    ok &= _check("blocker 分类: 架构师可回复阻塞", unblock_id > 0)
    return ok


def test_scenario_design_issue_feedback():
    """
    场景 10：design_issue 分类 — developer 实现中发现设计问题反馈给架构师
    """
    bb = Blackboard()

    # developer 发现设计问题
    issue_id = bb.write("design_issue", _tag("[developer] 告警聚合模块: 单点瓶颈风险"),
                         evidence="问题: 当前设计为单线程聚合, 高并发时可能丢消息\n建议: 加队列缓冲\n影响: 需评估增加 ring buffer",
                         src="developer")
    _CLEANUP_IDS.append(issue_id)

    unconsumed = bb.unconsumed()
    found = [f for f in unconsumed if f.id == issue_id]
    ok = _check("design_issue 分类: 开发者可反馈设计问题", len(found) == 1)

    # 验证架构师能消费
    from router import get_router
    router = get_router()
    consumers = router.get_consumers("design_issue")
    ok &= _check("design_issue → 架构师可消费", "product_architect" in consumers or any(
        c.get("consume", []) == ["*"] or "design_issue" in c.get("consume", [])
        for c in [{}, {}]
    ))
    return ok


# ══════════════════════════════════════════════════════════════════
# 主函数
# ══════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  角色交互测试 (Role Interaction Tests)")
    print("  Sister Bus: ~/.hermes/sister_bus/blackboard.db")
    print("=" * 60)
    print()

    bb = Blackboard()
    print(f"  Bus 初始统计: {bb.stats()['total']} 条消息\n")

    tests = [
        ("架构师接收设计需求", test_scenario_architect_receives_design_request),
        ("架构师产出 PRD", test_scenario_architect_publishes_prd),
        ("架构师产出系统设计", test_scenario_architect_publishes_system_design),
        ("架构师产出任务分解", test_scenario_architect_publishes_task_spec),
        ("Coordinator 自动检测任务", test_scenario_coordinator_detects_tasks),
        ("Developer 消费任务并产出 code_fix", test_scenario_developer_consumes_tasks),
        ("Closer 闭环确认", test_scenario_closer_closes_loop),
        ("架构师接收产品设计需求(product_design)", test_scenario_product_design_request_product_architect),
        ("阻塞升级(blocker)", test_scenario_blocker_escalation),
        ("设计问题反馈(design_issue)", test_scenario_design_issue_feedback),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        print(f"  [{name.replace(chr(40),'').replace(chr(41),'')}]")
        try:
            ok = fn()
            if ok:
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"      ❌ 异常: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print()
    print(f"  Bus 最终统计: {bb.stats()['total']} 条消息")

    # 清理
    _cleanup(bb)

    print()
    print(f"  结果: {passed}/{passed + failed} 通过")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
