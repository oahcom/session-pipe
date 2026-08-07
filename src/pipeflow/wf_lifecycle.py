#!/usr/bin/env python3
"""WorkflowLifecycleMixin — 工作流级推进：僵尸回收、推进、子工作流、loop、重推、变量填充。

从 engine.py 拆出，方法签名和 self 访问完全不变。
"""

import json
import time
import uuid
import logging
from pathlib import Path

from pipeflow import engine as _engine_mod  # _engine_mod._sp — 保证 patch("pipeflow.engine._sp") 生效
from pipeflow.engine import Step, WorkflowDef
from paths import CCS_CLI as _CCS_CLI
from paths import CCS_WORKSPACES as _CCS_WORKSPACES

LOGGER = logging.getLogger("workflow.engine")


class WorkflowLifecycleMixin:
    """WorkflowEngine 的工作流生命周期 mixin。"""

    def _cleanup_stale_workflows(self, lm):
        """自动取消僵尸 workflow。

        3 级回收策略:
          - 开发/测试模板（_ZOMBIE_TEMPLATES）> 2h → cancel
          - 所有模板 > 24h → cancel
          - 有 timeout_count >= 6（约 3 小时超时）→ cancel
        """
        from pipeflow.engine import _ZOMBIE_TEMPLATES
        now = time.time()
        try:
            for row in lm.query(
                "SELECT instance_id, template_id, created_at, step_results "
                "FROM workflow_instances WHERE status='running'"
            ):
                d = dict(row)
                wf_id = d["instance_id"]
                tpl = d.get("template_id", "?")
                created = d.get("created_at", 0)
                age_h = (now - created) / 3600 if created else 0
                sr = json.loads(d.get("step_results") or "{}")

                max_tc = max(
                    (v.get("timeout_count", 0) for v in sr.values() if isinstance(v, dict)),
                    default=0,
                )

                # 最小存活时间保护：创建不足 5 分钟的 workflow 不自动回收。
                # 防止"刚通知角色就被 stale cleanup 误杀"（bus #158145：32s 被 cancel）。
                age_s = now - created if created else 0
                if age_s < 300:
                    continue

                _auto_cancel_at = 6
                reason = ""
                # 条件1: 超时重试耗尽
                if max_tc >= _auto_cancel_at:
                    reason = f"timeout_count={max_tc}（已达自动回收阈值{_auto_cancel_at}）"
                # 条件2: 测试/开发模板超2小时
                elif tpl in _ZOMBIE_TEMPLATES and age_h > 2:
                    reason = f"测试模板 {tpl} 运行{age_h:.0f}h > 2h阈值"
                # 条件3: 全局24小时
                elif age_h > 24:
                    reason = f"运行{age_h:.0f}h > 24h全局阈值"
                # 条件4: running 但 step_results 为空且 >1h（从未推进）
                elif age_h > 1 and not sr:
                    _step = d.get("current_step_id", "")
                    reason = f"running 状态 {_step} 无推进记录 {age_h:.1f}h"

                if reason:
                    LOGGER.warning("auto-cancel: %s (template=%s) — %s", wf_id, tpl, reason)
                    # ── 回收前抢救: 当前步骤已有 exit_messages(产出证据) → 先尝试闭合 ──
                    try:
                        _cur = d.get("current_step_id", "")
                        _sdata = sr.get(_cur, {}) if isinstance(sr, dict) else {}
                        if isinstance(_sdata, dict) and _sdata.get("exit_messages"):
                            self._lifecycle.complete_step(wf_id, _cur)
                            LOGGER.info("stale rescue: %s step %s exit_messages 已就绪 → 已闭合",
                                        wf_id, _cur)
                            continue
                    except Exception as _re:
                        LOGGER.debug("stale rescue failed for %s: %s", wf_id, _re)
                    try:
                        lm.execute(
                            "UPDATE workflow_instances SET status='cancelled' WHERE instance_id=?",
                            (wf_id,)
                        )
                        lm.execute(
                            "INSERT INTO workflow_logs (workflow_instance_id, task_id, action, actor, detail, ts) "
                            "VALUES (?, ?, 'cancelled', 'workflow_engine', ?, ?)",
                            (wf_id, "", f"僵尸回收: {reason}", time.time())
                        )
                        self._notify_role("maintainer",
                            f"僵尸工作流自动回收: {tpl}/{wf_id}",
                            f"原因: {reason}\n请排查该工作流为何没有正常完成。")
                    except Exception as e:
                        LOGGER.debug("stale cleanup write failed: %s", e)
            LOGGER.info("stale cleanup done")
        except Exception as e:
            LOGGER.warning("stale cleanup error: %s", e)

    def _advance_production_wf(self, wf_id: str, lm, wf_name: str, step_id: str,
                                results: dict = None):
        """推进生产工作流到下一步，并通知目标角色。"""
        from pipeflow.engine import _UNMATCHED_VAR_RE
        try:
            conn = lm._conn  # ponytail: 事务内批量操作，下一轮重构时统一用 execute_raw
            # 防重闭合守卫：launcher wf complete 全量覆写 step_results 会把已
            # completed 的工作流打回 step_done_ready，这里拒绝二次推进。
            _row = conn.execute(
                "SELECT status FROM workflow_instances WHERE instance_id=?", (wf_id,)
            ).fetchone()
            if _row and _row["status"] == "completed":
                LOGGER.debug("advance skipped: %s already completed", wf_id)
                return
            wf = self._workflows.get(wf_name)
            if not wf:
                return
            steps = wf.steps
            idx = next((i for i, s in enumerate(steps) if s.id == step_id), -1)
            if idx < 0:
                return
            # 最后一步 → 检查所有子工作流是否全部完成
            if idx + 1 >= len(steps):
                # 从 DB 实时读取 step_results（不依赖传入的 results 快照，避免
                # 主循环与 _scan_tasks 双路径传入不同副本导致竞态误判）
                _db_res = conn.execute(
                    "SELECT step_results FROM workflow_instances WHERE instance_id=?", (wf_id,)
                ).fetchone()
                _all_subs_done = True
                for _sid, _sdata in (json.loads(_db_res["step_results"]) if _db_res and _db_res["step_results"] else {}).items():
                    if not isinstance(_sdata, dict):
                        continue
                    _sf = _sdata.get("subflow_id", "")
                    if _sf:
                        _st = conn.execute("SELECT status FROM workflow_instances WHERE instance_id=?", (_sf,)).fetchone()
                        if not _st or _st['status'] not in ('completed', 'step_done_ready'):
                            _all_subs_done = False
                            break
                if _all_subs_done:
                    LOGGER.info("wf %s [%s] 完成: %d 步", wf_id[:8], wf_name, len(steps))
                    self._lifecycle.close_wf(wf_id, status='completed')
                    conn.commit()
                    # 部署自动闭环：把产出 cli/*.py 安装到 ~/.hermes/bin/，让工具可被
                    # cron/daemon/角色直接调用（"有实际收益"的最后一环——部署）。
                    self._deploy_production_outputs(wf_id)
                    self._bb.write("workflow",
                        f"[workflow] {wf_name} 完成: {len(steps)} 步",
                        src="workflow_engine")
                    # 食物链循环：loop 配置存在 → 检查是否继续下一轮
                    # 子 agent 价值审计文件存在且标记 FAIL → 停止循环（收益不足）
                    # 否则重启生产者角色（思路干净）并启动新迭代
                    if wf.loop:
                        self._handle_wf_loop(wf, wf_id, conn)
                return
            # 推进到下一步
            next_step = steps[idx + 1]
            # 条件更新：仅当当前步骤仍是 step_id 时才推进（并发防重）
            _adv = conn.execute("UPDATE workflow_instances SET current_step_id=? WHERE instance_id=? AND current_step_id=?",
                        (next_step.id, wf_id, step_id))
            if _adv.rowcount == 0:
                LOGGER.debug("advance skipped: %s/%s 已被其他路径推进", wf_id[:12], step_id)
                return
            # 推进后行 status 归位 running，避免 _scan_tasks 路径推进后
            # 主循环看到旧 step_done_ready 再次触发 advance
            conn.execute("UPDATE workflow_instances SET status='running' WHERE instance_id=?",
                        (wf_id,))
            new_results = dict(results or {})
            new_results[next_step.id] = {"status": "notified", "ts": time.time(), "notified_at": time.time(),
                                          "poll_since": time.time(), "bus_anchor": time.time()}
            conn.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(new_results, ensure_ascii=False), wf_id))
            # 通知目标角色（先在未提交事务里试跑 variable 检查——
            # 未替换变量时回滚推进，DB 保持原状，避免步骤卡死在 'notified'
            # 且角色收不到通知，timeout_count 递增到自动取消）
            task = conn.execute(
                "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                (wf_id,)
            ).fetchone()
            task_title = task["title"] if task else wf_name
            task_desc = task["description"] if task else ""
            prompt = self._fill_prompt_vars(
                next_step.prompt_template, next_step, wf_name,
                task_title, task_desc, results, prev_step_id=step_id)
            if _UNMATCHED_VAR_RE.search(prompt):
                unmatched = _UNMATCHED_VAR_RE.findall(prompt)
                conn.rollback()
                LOGGER.warning("advance skip notify: %s/%s step=%s 含未替换变量 %s，已回滚推进",
                              wf_id[:8], wf_name, next_step.id, unmatched)
                return
            conn.commit()
            # 变量完整 → 提交推进，通知目标角色
            self._send_to_role(next_step.target_role, prompt,
                               wf_id=wf_id, step_id=next_step.id)
            # 如果步骤有子工作流模板 → 自动创建子工作流
            if next_step.subflow_template:
                try:
                    # 防重：DB 中该步骤已有 subflow_id → 不重复创建
                    # （否则每 tick 推进同一历史步骤都会新建 subflow，垃圾任务堆积）
                    _db_sr = conn.execute(
                        "SELECT step_results FROM workflow_instances WHERE instance_id=?",
                        (wf_id,)).fetchone()
                    _db_sdata = (json.loads(_db_sr["step_results"]) if _db_sr and _db_sr["step_results"]
                                 else {}).get(next_step.id, {})
                    if isinstance(_db_sdata, dict) and _db_sdata.get("subflow_id"):
                        return
                    _sub_wf = self._workflows.get(next_step.subflow_template)
                    if _sub_wf and _sub_wf.steps and _sub_wf.steps[0].target_role == next_step.target_role:
                        _prev_done = ""
                        if idx >= 0:
                            _prev_done = results.get(step_id, {}).get("completed_by", "")
                        if not _prev_done:
                            _prev_wf = conn.execute("SELECT assigner FROM workflow_instances WHERE instance_id=?", (wf_id,)).fetchone()
                            _prev_done = _prev_wf["assigner"] if _prev_wf else "pm"
                        _task_id = f"task_sub_{uuid.uuid4().hex[:8]}"
                        _wf_id = f"wf_sub_{uuid.uuid4().hex[:12]}"
                        conn.execute("INSERT OR IGNORE INTO tasks (task_id,title,description,assigner,assignee,status,created_at,updated_at,template_id) VALUES (?,?,?,?,?,'in_progress',?,?,?)",
                            (_task_id, f"{wf_name}/{next_step.id}: {next_step.title}", task_desc, _prev_done, next_step.target_role, time.time(), time.time(), next_step.subflow_template))
                        conn.execute("INSERT OR IGNORE INTO workflow_instances (instance_id,template_id,task_id,assigner,assignee,status,current_step_id,step_results,created_at) VALUES (?,?,?,?,?,'pending','s1',?,?)",
                            (_wf_id, next_step.subflow_template, _task_id, _prev_done, next_step.target_role, json.dumps({}), time.time()))
                        conn.commit()
                        new_results[next_step.id]["subflow_id"] = _wf_id
                        conn.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                            (json.dumps(new_results, ensure_ascii=False), wf_id))
                        conn.commit()
                except Exception as _e:
                    LOGGER.error("subflow creation failed for %s: %s", wf_id, _e)
        except Exception as e:
            LOGGER.warning("_advance_production_wf failed %s: %s", wf_id, e)

    def _deploy_production_outputs(self, wf_id: str):
        """全链路完成后自动部署产出物到 ~/.hermes/bin/。

        扫描 engineer workspace 下的 cli/*.py，复制到生产路径。
        部署后写 bus cat=code_fix 通知维护者。
        """
        try:
            eng_ws = _CCS_WORKSPACES / "engineer" / "cli"
            if not eng_ws.exists():
                return
            deploy_dir = Path.home() / ".hermes" / "bin"
            deploy_dir.mkdir(parents=True, exist_ok=True)
            deployed = []
            for f in eng_ws.glob("*.py"):
                if f.name.startswith("_") or f.name.startswith("test_"):
                    continue
                import shutil
                dest = deploy_dir / f.name
                shutil.copy2(str(f), str(dest))
                deployed.append(f.name)
            if deployed:
                self._bb.write("code_fix",
                    f"[deploy] {wf_id[:12]} 产出已部署: {', '.join(deployed)}",
                    evidence=f"路径: ~/.hermes/bin/{'/'.join(deployed)}",
                    src="workflow_engine")
                LOGGER.info("deployed %d outputs from %s", len(deployed), wf_id[:12])
        except Exception as e:
            LOGGER.debug("deploy_production_outputs failed: %s", e)

    def _handle_wf_loop(self, wf: WorkflowDef, wf_id: str, conn):
        """食物链循环处理：检查价值门禁 → 重启生产者 → 创建新迭代。

        loop 配置格式：
          {
            "enabled": true,
            "max_iterations": 3,          # 0=无限
            "producer_roles": ["scout"],   # 每轮重启的角色（思路干净）
            "value_gate": {
              "file": "VALUE_AUDIT.md",     # 审计角色产出物
              "workspace": "lr"             # 审计角色 workspace
            }
          }
        """
        loop_cfg = wf.loop or {}
        if not loop_cfg.get("enabled", True):
            return
        try:
            # 从实例 context 读取当前迭代数
            _ctx_row = conn.execute(
                "SELECT context FROM workflow_instances WHERE instance_id=?", (wf_id,)
            ).fetchone()
            _ctx = json.loads(_ctx_row["context"]) if _ctx_row and _ctx_row["context"] else {}
            _iteration = int(_ctx.get("loop_iteration", 0) or 0)
            _max_iter = int(loop_cfg.get("max_iterations", 0) or 0)
            if _max_iter > 0 and _iteration >= _max_iter:
                LOGGER.info("wf %s loop: 达到最大迭代 %d，停止", wf_id[:8], _max_iter)
                self._bb.write("workflow",
                    f"[workflow] {wf.name} 循环结束: 达到最大迭代 {_max_iter}",
                    src="workflow_engine")
                return

            # 价值门禁：审计角色产出文件标记 FAIL → 停止循环
            _vg = loop_cfg.get("value_gate") or {}
            _vg_file = _vg.get("file", "")
            _vg_ws = _vg.get("workspace", "")
            if _vg_file and _vg_ws:
                _audit_path = Path.home() / "ccs-workspaces" / _vg_ws / _vg_file
                if _audit_path.exists():
                    _text = _audit_path.read_text(encoding="utf-8", errors="ignore")
                    if "FAIL" in _text.upper() or "REJECT" in _text.upper():
                        LOGGER.info("wf %s loop: 价值审计 FAIL，停止循环", wf_id[:8])
                        self._bb.write("workflow",
                            f"[workflow] {wf.name} 循环停止: 价值审计未达标",
                            src="workflow_engine")
                        return

            # 重启生产者角色（tmux kill + ccs start）——保证思路干净
            _producers = loop_cfg.get("producer_roles") or []
            for _role in _producers:
                try:
                    _engine_mod._sp.run(["python3", str(_CCS_CLI), "stop", _role],
                            capture_output=True, timeout=30)
                    _engine_mod._sp.run(["python3", str(_CCS_CLI), "start", _role, "--no-attach",
                             "--drive", "infinite"],
                            capture_output=True, timeout=30)
                    LOGGER.info("wf %s loop: 生产者 %s 已重启 (iteration %d)",
                                wf_id[:8], _role, _iteration + 1)
                except (ValueError, KeyError, TypeError) as _e:
                    LOGGER.warning("loop producer restart failed %s: %s", _role, _e)

            # 创建新迭代：新任务 + 新工作流实例（同一模板）
            _task_id = f"task_loop_{uuid.uuid4().hex[:8]}"
            _new_wf_id = f"wf_{uuid.uuid4().hex[:12]}"
            _task_title = f"{wf.title} — 迭代 {_iteration + 1}"
            _task_desc = f"{wf.description}\n[loop] 迭代 {_iteration + 1}/{_max_iter or '∞'}"
            _now = time.time()
            try:
                conn.execute(
                    "INSERT INTO tasks (task_id, title, description, assigner, assignee, "
                    "priority, status, created_at, updated_at) VALUES (?,?,?,?,?,0,'in_progress',?,?)",
                    (_task_id, _task_title, _task_desc, "workflow_engine",
                     wf.allowed_executors[0] if wf.allowed_executors else "", _now, _now))
                conn.execute(
                    "INSERT INTO workflow_instances (instance_id, template_id, task_id, assigner, "
                    "assignee, status, current_step_id, step_results, context, created_at) "
                    "VALUES (?,?,?,?,?,'pending','s1',?,?,?)",
                    (_new_wf_id, wf.name, _task_id, "workflow_engine",
                     wf.allowed_executors[0] if wf.allowed_executors else "",
                     json.dumps({}),
                     json.dumps({"loop_iteration": _iteration + 1, "parent_loop_wf": wf_id}),
                     _now))
                conn.commit()
                self._bb.write("workflow",
                    f"[workflow] {wf.name} 新一轮迭代 {_iteration + 1} 已启动: {_new_wf_id}",
                    src="workflow_engine")
            except Exception as _e:
                LOGGER.error("loop new iteration failed %s: %s", wf_id, _e)
                conn.rollback()
        except Exception as _e:
            LOGGER.warning("_handle_wf_loop failed %s: %s", wf_id, _e)

    def _fill_prompt_vars(self, prompt: str, step: Step, wf_name: str,
                          task_title: str, task_desc: str, results: dict,
                          prev_step_id: str = "") -> str:
        """填充 prompt 变量：任务上下文 + 步骤间数据传递（上一步 exit_messages）。
        供 _advance_production_wf / _kick_stalled_roles / run_once 复用，保证
        重推与首推拿到的 prompt 一致（避免重推丢上下文）。"""
        prompt = prompt.replace("{title}", task_title)
        prompt = prompt.replace("{description}", task_desc)
        prompt = prompt.replace("{assignee}", step.target_role)
        prompt = prompt.replace("{task_definition}", task_title or task_desc or wf_name)
        prompt = prompt.replace("{acceptance_criteria}", task_desc or "按模板要求完成产出并写入对应bus分类")
        prompt = prompt.replace("{topic}", task_title)
        # workspace 摘要：给角色上下文（首步生产扫描在调用前已替换，这里统一兜底）
        try:
            _ws_dir = Path.home() / "ccs-workspaces" / step.target_role
            prompt = prompt.replace("{workspace_summary}",
                                    self._collect_workspace_summary(_ws_dir))
        except Exception:
            pass
        # 步骤间数据传递：上一步骤的 exit_messages 填充后续步骤变量
        prev_sr = (results or {}).get(prev_step_id, {}) if prev_step_id else {}
        prev_msgs = prev_sr.get("exit_messages", []) if isinstance(prev_sr, dict) else []
        if prev_msgs:
            prev_text = "\n".join(str(m) for m in prev_msgs)[:3000]
            for var in ("{target}", "{findings}", "{results}", "{backlog}",
                        "{exception_info}", "{focus_area}"):
                prompt = prompt.replace(var, prev_text)
        else:
            fallback = task_desc or task_title or wf_name
            for var in ("{target}", "{findings}", "{results}", "{backlog}",
                        "{exception_info}", "{focus_area}", "{changes}",
                        "{task_list}", "{assignments}", "{workspace_summary}", "{project}"):
                if var in prompt:
                    prompt = prompt.replace(var, fallback)
        return prompt

    def _write_step_prompt(self, run, step: Step, extra_prompt: str = ""):
        ctx = {**run.context, "workflow_id": run.id, "step_id": step.id}
        if "{workspace_summary}" in step.prompt_template:
            ws_dir = Path.home() / ".hermes" / "workspace" / ctx.get("project_name", "")
            ctx["workspace_summary"] = self._collect_workspace_summary(ws_dir)
        # 从 SQLite 补 task 上下文
        if "{title}" in step.prompt_template or "{description}" in step.prompt_template:
            try:
                lm = self._lifecycle
                task_rows = lm.query(
                    "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                    (run.id,)
                )
                task = task_rows[0] if task_rows else None
                if task:
                    ctx["title"] = task["title"]
                    ctx["description"] = task["description"]
                    ctx["assignee"] = step.target_role
            except Exception as _e:
                LOGGER.exception("_write_step_prompt task context fetch failed for %s", run.id)
        prompt = step.prompt_template + "\n" + extra_prompt if extra_prompt else step.prompt_template
        for k, v in ctx.items():
            prompt = prompt.replace(f"{{{k}}}", str(v))
        # 语义兜底（与生产扫描块一致）：context 缺失时防止未替换变量导致发送跳过
        fallback = ctx.get("title") or ctx.get("task_title") or run.workflow_name or step.title or ""
        prompt = prompt.replace("{focus_area}", step.title or fallback)
        for var in ("{target}", "{findings}", "{results}", "{backlog}",
                    "{exception_info}", "{title}", "{description}", "{topic}",
                    "{task_definition}", "{acceptance_criteria}", "{project}",
                    "{changes}", "{task_list}", "{assignments}"):
            if var in prompt:
                prompt = prompt.replace(var, fallback)
        self._send_to_role(step.target_role, prompt)

    def _collect_workspace_summary(self, ws_dir: Path) -> str:
        if not ws_dir.exists():
            return "workspace 不存在"
        parts = []
        for fname in ["PRD.md", "DESIGN.md", "TASKS.json", "INTAKE.md"]:
            fpath = ws_dir / fname
            if fpath.exists():
                content = fpath.read_text()
                parts.append(f"[{fname}] {content[:200]}...")
        return "\n".join(parts) if parts else "workspace 为空"
