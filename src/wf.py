#!/usr/bin/env python3
"""wf — 一步达 WorkflowClient CLI (like ccs)"""
import argparse, json, os, sys, textwrap, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from workflow_client import WorkflowClient, check

def main():
    p = argparse.ArgumentParser(prog="wf", formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--role", "-r", default=os.getenv("CCS_ROLE", ""), help="角色 (默认 $CCS_ROLE)")
    sub = p.add_subparsers(dest="cmd")

    sub.add_parser("check", help="检查是否有待办任务")

    p_create = sub.add_parser("create", help="创建任务")
    p_create.add_argument("title")
    p_create.add_argument("--assignee", "-a", default="")
    p_create.add_argument("--template", "-t", default="",
        help="模板名（不指定则按标题+角色自动推荐）")
    p_create.add_argument("--initiator", "-i", default="")

    p_complete = sub.add_parser("complete", help="完成任务")
    p_complete.add_argument("wf_id")
    p_complete.add_argument("--summary", "-s", default="done")

    p_fail = sub.add_parser("fail", help="标记失败")
    p_fail.add_argument("wf_id")
    p_fail.add_argument("--reason", "-r", default="")

    p_notify = sub.add_parser("notify", help="发 bus 通知")
    p_notify.add_argument("cat")
    p_notify.add_argument("title")
    p_notify.add_argument("--evidence", "-e", default="")

    p_task = sub.add_parser("task", help="查看任务详情")
    p_task.add_argument("task_id")

    p_logs = sub.add_parser("logs", help="查看日志")
    p_logs.add_argument("--wf", default="")
    p_logs.add_argument("--task", default="")

    p_cancel = sub.add_parser("cancel", help="取消工作流")
    p_cancel.add_argument("wf_id")
    p_cancel.add_argument("--reason", "-r", default="")

    p_my = sub.add_parser("my", help="我的任务列表")
    p_my.add_argument("--status", "-s", default="")

    p_confirm = sub.add_parser("confirm", help="确认步骤完成")
    p_confirm.add_argument("wf_id")
    p_confirm.add_argument("step_id")
    p_confirm.add_argument("--token", "-t", default="")

    p_analyze = sub.add_parser("analyze", help="call claude to analyze a stuck workflow")
    p_analyze.add_argument("wf_id", help="workflow instance id")

    p_health = sub.add_parser("health", help="工作流健康检查（检查卡住的工作流）")
    p_recover = sub.add_parser("recover", help="尝试恢复卡住的工作流")
    p_recover.add_argument("wf_id", help="工作流实例 ID")

    p_suggest = sub.add_parser("suggest", help="推荐模板")
    p_suggest.add_argument("title", help="任务标题")
    p_suggest.add_argument("--initiator", "-i", default="")
    p_suggest.add_argument("--assignee", "-a", default="", help="执行角色（空=推荐全部角色匹配的模板）")

    p_ls = sub.add_parser("ls", help="列出任务")
    p_ls.add_argument("--status", "-s", default="")
    p_ls.add_argument("--assignee", "-a", default="")

    sub.add_parser("kanban", help="看板视图")
    sub.add_parser("stats", help="统计")

    args = p.parse_args()
    role = args.role or os.getenv("CCS_ROLE", "")
    if not args.cmd:
        p.print_help(); return

    if args.cmd == "check":
        if role:
            with WorkflowClient(role) as wf:
                t = wf.check_task()
                print(json.dumps(t, indent=2, ensure_ascii=False) if t else "无待办")
        else:
            for r in ("pm", "pg", "qa", "engineer", "lr", "arch", "scout", "devops", "reviewer", "maintainer", "coordinator", "cx", "whale"):
                t = check(r)
                if t:
                    print(f"{r}: {t.get('instance_id','?')} — {t.get('template_id','?')}")

    elif args.cmd == "analyze":
        import sqlite3, json as _json, subprocess
        from pathlib import Path
        _db = Path.home() / ".hermes" / "state" / "workflows.db"
        if not _db.exists():
            print("workflows db not found"); return
        _conn = sqlite3.connect(str(_db))
        _conn.row_factory = sqlite3.Row
        _inst = _conn.execute("SELECT * FROM workflow_instances WHERE instance_id=?", (args.wf_id,)).fetchone()
        if not _inst:
            print("workflow " + args.wf_id + " not found"); return
        _inst = dict(_inst)
        _tpl = _conn.execute("SELECT * FROM workflow_templates WHERE template_id=?", (_inst["template_id"],)).fetchone()
        _tpl = dict(_tpl) if _tpl else {}
        _results = _json.loads(_inst.get("step_results") or "{}")
        _steps = _json.loads(_tpl.get("steps_json") or "[]") if _tpl else []
        _elapsed_h = int((time.time() - _inst["created_at"]) / 3600)
        _step_defs = "\n".join(
            "  " + s.get("step_id","?") + ": " + s.get("title","") + " (target=" + (s.get("target_role") or "?") + ", exit=" + (s.get("exit_condition",{}).get("bus_category") or "?") + ", timeout=" + str(s.get("exit_condition",{}).get("timeout_minutes","?")) + "min)"
            for s in _steps[:10]
        )
        _step_results = "\n".join("  " + k + ": " + _json.dumps(v, ensure_ascii=False) for k, v in _results.items())
        _bus_msgs = ""
        _bus_db = Path.home() / ".hermes" / "sister_bus" / "blackboard.db"
        if _bus_db.exists() and _steps:
            try:
                _bconn = sqlite3.connect(str(_bus_db))
                _cats = _json.dumps(list(set(
                    s.get("exit_condition",{}).get("bus_category","") for s in _steps if s.get("exit_condition")
                )))
                _msgs = _bconn.execute(
                    "SELECT cat, t, src FROM facts WHERE cat IN (SELECT value FROM json_each(?)) ORDER BY id DESC LIMIT 10",
                    (_cats,)
                ).fetchall()
                if _msgs:
                    _bus_msgs = "\n".join("[" + r[0] + "] " + (r[1] or "")[:80] + " (src=" + (r[2] or "?") + ")" for r in _msgs)
                _bconn.close()
            except Exception:
                pass
        _prompt = (
            "Analyze this stuck workflow:\n"
            "\nID: " + _inst["instance_id"] +
            "\nTemplate: " + (_inst["template_id"] or "?") +
            "\nStatus: " + (_inst["status"] or "?") +
            "\nCurrent step: " + (_inst.get("current_step_id") or "?") +
            "\nAssignee: " + (_inst.get("assignee") or "?") +
            "\nAge: " + str(_elapsed_h) + " hours" +
            "\nTemplate desc: " + (_tpl.get("description") or "none") +
            "\n\nStep definitions:\n" + _step_defs +
            "\n\nStep results:\n" + _step_results +
            "\n\nRecent bus messages in relevant categories:\n" + (_bus_msgs or "(none)") +
            "\n\nQuestions:\n"
            "1. Root cause? (template mismatch? CCS unresponsive? bus category mismatch? timeout too short?)\n"
            "2. Recommended recovery? (wf cancel? wf fail? retry? wait?)\n"
            "3. How to prevent in the future?"
        )
        try:
            _result = subprocess.run(["claude", "-p", _prompt], capture_output=True, text=True, timeout=120)
            if _result.returncode == 0:
                print(_result.stdout.strip())
            else:
                print("Claude analysis failed: " + (_result.stderr[:200] or str(_result.returncode)))
        except FileNotFoundError:
            print("claude CLI not found")
        except subprocess.TimeoutExpired:
            print("Claude analysis timeout (>120s)")
        _conn.close()

    elif args.cmd == "recover":
        import sqlite3, json as _json
        from pathlib import Path
        _db = Path.home() / ".hermes" / "state" / "workflows.db"
        if not _db.exists():
            print("no workflows db found"); return
        _conn = sqlite3.connect(str(_db))
        _wf = _conn.execute("SELECT * FROM workflow_instances WHERE instance_id=?", (args.wf_id,)).fetchone()
        if not _wf:
            print(f"workflow {args.wf_id} not found"); return
        _wf = dict(_wf)
        _tpl = _conn.execute("SELECT * FROM workflow_templates WHERE template_id=?", (_wf["template_id"],)).fetchone()
        _tpl = dict(_tpl) if _tpl else {}
        _now = __import__("time").time()
        _age = (_now - _wf["created_at"]) / 3600
        _max = _tpl.get("max_duration_hours", 24) or 24
        print(f"Workflow: {_wf['instance_id']}")
        print(f"  Template: {_wf['template_id']}")
        print(f"  Status: {_wf['status']}")
        print(f"  Current step: {_wf['current_step_id']}")
        print(f"  Age: {_age:.0f}h / limit {_max}h")
        if _wf["status"] in ("completed", "failed", "cancelled"):
            print("  This workflow is already finalized.")
        elif _age > _max * 2:
            print("  Recommendation: `wf cancel %s` — exceeded max duration by 2x" % args.wf_id)
        elif _wf["status"] == "pending":
            print("  Recommendation: `python3 -m pipeflow.engine daemon` — pending workflow needs engine to pick it up")
        elif _wf["current_step_id"]:
            print("  Recommendation: check step progress. If stuck, `wf fail %s --reason <reason>` or `wf cancel %s`" % (args.wf_id, args.wf_id))
        else:
            print("  Recommendation: `wf cancel %s`" % args.wf_id)
        _conn.close()

    elif args.cmd == "suggest":
        from template_registry import TemplateRegistry as _TR
        r = args.role or args.initiator or input("initiator role: ")
        _reg = _TR()
        if args.assignee:
            _results = _reg.recommend(args.title, initiator_role=r, assignee=args.assignee)
        else:
            _results = _reg.recommend(args.title, initiator_role=r)
        _reg.close()
        if _results:
            print(f"推荐模板（匹配{len(_results)}个）:")
            for _t in _results[:5]:
                _scenes = ", ".join(_t.get("trigger_scene",[])[:2])
                print(f"  {_t['template_id']:<35} {_t.get('name',''):<20} 场景: {_scenes}")
        else:
            print("无匹配模板，请用 --template 显式指定")

    elif args.cmd == "create":
        r = args.role or args.assignee
        if not r:
            print("--role 或 --assignee 必填"); return
        init = args.initiator or r
        with WorkflowClient(r) as wf:
            tid, wid = wf.create_task_v2(args.title, r, args.template, init)
            print(f"task={tid}  wf={wid}")

    elif args.cmd == "complete":
        r = role or input("role: ")
        with WorkflowClient(r) as wf:
            wf.complete(args.wf_id, args.summary)
            print(f"完成: {args.wf_id}")

    elif args.cmd == "fail":
        r = role or input("role: ")
        with WorkflowClient(r) as wf:
            wf.fail(args.wf_id, args.reason)
            print(f"失败标记: {args.wf_id}")

    elif args.cmd == "notify":
        r = role or input("role: ")
        with WorkflowClient(r) as wf:
            wf.notify(args.cat, args.title, evidence=args.evidence)
            print(f"通知已发: {args.cat} / {args.title}")

    elif args.cmd == "task":
        r = role or input("role: ")
        with WorkflowClient(r) as wf:
            t = wf.get_task(args.task_id)
            print(json.dumps(t, indent=2, ensure_ascii=False) if t else "未找到")

    elif args.cmd == "logs":
        r = role or input("role: ")
        with WorkflowClient(r) as wf:
            logs = wf.get_logs(wf_id=args.wf or None, task_id=args.task or None)
            for l in logs:
                print(f"[{l.get('ts','')}] {l.get('action','')} — {l.get('detail','')}")

    elif args.cmd == "cancel":
        r = role or input("role: ")
        with WorkflowClient(r) as wf:
            wf.cancel(args.wf_id, args.reason)
            print(f"已取消: {args.wf_id}")

    elif args.cmd == "my":
        r = role or input("role: ")
        with WorkflowClient(r) as wf:
            tasks = wf.list_my_tasks(status=args.status or None)
            for t in tasks:
                print(f"{t.get('instance_id','?'):24s} {t.get('status','?'):10s} {str(t.get('template_id') or '?'):12s} {t.get('current_step_id','?')}")

    elif args.cmd == "ls":
        r = role or input("role: ")
        with WorkflowClient(r) as wf:
            tasks = wf.list_tasks(status=args.status or None, assignee=args.assignee or None)
            for t in tasks:
                print(f"{t.get('task_id','?'):24s} {t.get('status','?'):10s} {t.get('assignee','?'):12s} {t.get('title','')}")

    elif args.cmd == "confirm":
        r = role or input("role: ")
        from lifecycle.manager import LifecycleManager
        lm = LifecycleManager(r)
        try:
            ok = lm.confirm_step(args.wf_id, args.step_id, token=args.token)
            print(f"确认 {'✅ 成功' if ok else '❌ 拒绝'}: {args.wf_id}/{args.step_id}")
        except (ValueError, PermissionError) as e:
            print(f"❌ 确认失败: {e}")
        finally:
            lm.close()

    elif args.cmd == "kanban":
        r = role or input("role: ")
        with WorkflowClient(r) as wf:
            b = wf.kanban_board()
            for lane in b:
                print(f"\n## {lane['lane']} ({lane['count']})")
                for item in lane['items'][:5]:
                    print(f"  {item.get('instance_id','?'):24s} {item.get('current_step_id',''):6s} {item.get('assignee','')}")

    elif args.cmd == "stats":
        r = role or input("role: ")
        with WorkflowClient(r) as wf:
            s = wf.workflow_stats()
            for k, v in s.items():
                print(f"{k}: {v}")
    elif args.cmd == "health":
        import sqlite3, json as _json
        from pathlib import Path
        _db = Path.home() / ".hermes" / "state" / "workflows.db"
        if not _db.exists():
            print("no workflows db found"); return
        _conn = sqlite3.connect(str(_db))
        _conn.row_factory = sqlite3.Row
        _rows = _conn.execute(
            "SELECT instance_id, template_id, status, current_step_id, created_at "
            "FROM workflow_instances WHERE status IN ('running','pending') "
            "ORDER BY created_at"
        ).fetchall()
        _templates = {r["template_id"]: dict(r) for r in _conn.execute(
            "SELECT template_id, max_duration_hours, quality_standards FROM workflow_templates"
        ).fetchall()}
        _now = __import__("time").time()
        _stuck = 0
        print(f"Workflow Health Check")
        print(f"{'='*50}")
        for r in _rows:
            _inst = dict(r)
            _tid = _inst["template_id"]
            _max_h = _templates.get(_tid, {}).get("max_duration_hours", 24) or 24
            _age_h = (_now - _inst["created_at"]) / 3600
            _status = "✅" if _age_h < _max_h else "⚠️ STUCK"
            if _status == "⚠️ STUCK":
                _stuck += 1
            print(f"  {_status} {_inst['instance_id'][:20]:<22} {_tid:<35} step={_inst['current_step_id'] or '?'} age={_age_h:.0f}h limit={_max_h}h")
        print(f"\n{_stuck} stuck workflows (running > max_duration_hours)")
        _conn.close()


