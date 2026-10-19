#!/usr/bin/env python3
"""
lifecycle_manager.py — 生命周期管理器

管理工作流状态流转：pending→running→step_done_ready→completed。
5 种步骤类型（handoff/review/single/gate/notify）各有不同完成策略。
并发安全：使用 threading.RLock + SQLite WAL 模式 + BEGIN IMMEDIATE。
"""

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List, Optional

from paths import WORKFLOWS_DB as DB_PATH  # ponytail: direct import, switch to relative when moved to subpackage



# ── gate 判据路径工具（与 step_handler.py:817 的 glob 惯用法一致）──
_PATH_CHARS = ("/", "\\", "~", ".", "*")


def _is_checkable_path(p: str) -> bool:
    """该 output_exists 条目是否像路径（而非 "变更代码文件" 这类描述）。

    模板里混入描述性文字是既成事实（技术实现.json 的 "deploy_success"），
    纯中文描述永远 exists()==False，照直判会让 gate 永久 blocked。
    """
    return bool(p) and any(c in p for c in _PATH_CHARS)


def _output_present(p: str) -> bool:
    """条目指向的文件是否存在；含 * 时按 glob 匹配（同 step_handler 的完成判据）。"""
    fp = Path(p).expanduser()
    if "*" in p:
        return any(fp.parent.glob(fp.name))
    return fp.exists()


class LifecycleManager:
    """生命周期管理器：状态机 + 步骤执行 + 推进逻辑。

    方法分区（2026-08 标注，按职责分）:
    - DB 连接/查询: query/query_one/execute/_conn_tx/ping
    - 模板 CRUD: upsert_template/get_wf/get_step/_get_template
    - 实例生命周期: start_wf/complete_step/fail_step/rollback_step/close_wf
    - 审批流: get_approval_token/confirm_step/_issue_approval_token/_notify_result
    - 内部工具: _get_wf_unsafe/_parse_results/_set_step_result_unsafe/_log_unsafe
    """

    WF_STATUSES = {"pending", "running", "completed", "failed", "cancelled"}
    STEP_STATUSES = {"pending", "running", "step_done_ready", "completed", "failed"}
    STEP_TYPES = {"handoff", "review", "single", "gate", "notify"}

    def __init__(self, role: str, db_path: str = None, on_advance: callable = None):
        self.role = role
        self.db_path = Path(db_path) if db_path else DB_PATH
        self._on_advance = on_advance  # callback(next_role) → 拉起下一棒角色，不等 daemon tick
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.db_path), timeout=10)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA wal_autocheckpoint=1000")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._ensure_schema()

    # ── 状态查询 ─────────────────────────────────

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        """安全的 DB 查询代理（替代外部直接访问 _conn）。"""
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def query_one(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        """单行查询代理。无匹配返回 None。"""
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def execute(self, sql: str, params: tuple = ()) -> None:
        """安全的 DB 写入代理（替代外部直接访问 _conn）。"""
        with self._lock:
            self._conn.execute(sql, params)
            self._conn.commit()


    def execute_raw(self, sql: str, params: tuple = ()) -> None:
        """执行 SQL（不自动提交，用于事务内操作）。"""
        with self._lock:
            self._conn.execute(sql, params)

    def commit(self) -> None:
        """提交当前事务。"""
        with self._lock:
            self._conn.commit()

    def rollback(self) -> None:
        """回滚当前事务。"""
        with self._lock:
            self._conn.rollback()

    def begin(self) -> None:
        """开始事务（IMMEDIATE），已在事务中时跳过。"""
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
            except Exception:
                pass  # ponytail: 已在事务中时静默 RLock 已保证线程安全

    @contextmanager
    def _conn_tx(self) -> Iterator[sqlite3.Connection]:
        """事务连接上下文：持有 RLock 期间返回原始连接，退出时释放锁。

        用于需要在单个事务中执行多次读写 + 检查结果（fetchone/rowcount）
        的场景（如 _advance_production_wf）。RLock 可重入，事务内仍可调
        self.commit()/rollback()（它们也加同一把锁，可重入不阻塞）。
        ponytail: 直接暴露原始连接；升级路径：execute_raw 返回 cursor 后
        改拆成多次 execute_raw 调用。"""
        self._lock.acquire()
        try:
            yield self._conn
        finally:
            self._lock.release()

    def ping(self) -> bool:
        """检查连接是否存活。"""
        try:
            with self._lock:
                self._conn.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False

    def upsert_template(self, template_id: str, name: str,
                         description: str, steps: list[dict],
                         trigger_scene: list[str] = None,
                         allowed_initiators: list[str] = None,
                         allowed_executors: list[str] = None,
                         max_duration_hours: int = 24,
                         quality_standards: str = "") -> None:
        """写入或更新工作流模板定义（含角色适配元数据）。"""
        self._conn.execute("""
            INSERT OR REPLACE INTO workflow_templates
            (template_id, name, description, steps_json, created_at, is_active,
             trigger_scene, allowed_initiators, allowed_executors,
             max_duration_hours, quality_standards)
            VALUES (?, ?, ?, ?, ?, 1,
                    ?, ?, ?,
                    ?, ?)
        """, (template_id, name, description,
              json.dumps(steps, ensure_ascii=False), time.time(),
              json.dumps(trigger_scene or [], ensure_ascii=False),
              json.dumps(allowed_initiators or [], ensure_ascii=False),
              json.dumps(allowed_executors or [], ensure_ascii=False),
              max_duration_hours, quality_standards))
        self._conn.commit()

    def get_wf(self, wf_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM workflow_instances WHERE instance_id=?", (wf_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_step(self, wf_id: str, step_id: str) -> Optional[dict]:
        wf = self.get_wf(wf_id)
        if not wf:
            return None
        template_id = wf.get("template_id")
        template = self._get_template(template_id)
        if not template:
            return None
        for step in template.get("steps", []):
            # DB 存储时用 id，但 legacy 可能存 step_id —— 两者都兼容
            if step.get("id") == step_id or step.get("step_id") == step_id:
                return step
        return None

    def _get_template(self, template_id: Optional[str]) -> Optional[dict]:
        if not template_id:
            return None
        # 实例存裸名（子链 takeover 写 tgt_step.subflow_template），表里注册的是
        # WF- 前缀名。缺回退时 _get_assigner_for_step 拿不到模板 → 审批人解析为空，
        # 但 _complete_review_unsafe 仍先落 token → engine 审批门控只看 token 存在
        # 就 continue，白等 TTL+grace 90min（实测 architect_full_design/s4 卡 65min）。
        row = self._conn.execute(
            "SELECT * FROM workflow_templates WHERE template_id=?", (template_id,)
        ).fetchone()
        if not row and not template_id.startswith("WF-"):
            row = self._conn.execute(
                "SELECT * FROM workflow_templates WHERE template_id=?",
                (f"WF-{template_id}",)
            ).fetchone()
        if not row:
            return None
        d = dict(row)
        steps = d.get("steps_json")
        if isinstance(steps, str):
            try:
                d["steps"] = json.loads(steps)
            except (json.JSONDecodeError, TypeError):
                d["steps"] = []
        return d

    def _log(self, wf_id: str, task_id: str = None,
             action: str = "", detail: str = ""):
        self._conn.execute(
            "INSERT INTO workflow_logs (workflow_instance_id, task_id, "
            "action, actor, detail, ts) VALUES (?, ?, ?, ?, ?, ?)",
            (wf_id, task_id, action, self.role, detail, time.time())
        )
        self._conn.commit()

    # ── 核心操作（并发安全） ─────────────────────

    def start_wf(self, wf_id: str, current_step_id: str = "s1",
                  template_id: str = "", context: dict = None,
                  initiator: str = None, assignee: str = None) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                ctx_json = json.dumps(context or {}, ensure_ascii=False)
                cur = self._conn.execute(
                    "UPDATE workflow_instances SET status='running', "
                    "current_step_id=?, context=? WHERE instance_id=? AND status='pending'",
                    (current_step_id, ctx_json, wf_id)
                )
                if cur.rowcount == 0:
                    # engine 直接启动的工作流（无前置 task）→ 创建真实 task 父行，避免孤儿实例
                    import uuid as _uuid
                    _ctx = context or {}
                    _task_id = _ctx.get("task_id") or ("task_" + _uuid.uuid4().hex[:8])
                    _now = time.time()
                    # assignee 取真实执行者（首步 target_role）。写死 workflow_engine 会
                    # 让实例/任务归属于引擎自身，而引擎不是可投递角色：下游按 assignee
                    # 算审批人、算忙闲、算归属时全部错位（实测 s0 排 10 次无法投递）。
                    # engine.start 调用方可显式传 assignee；未传时回退旧值。
                    _assignee = assignee or "workflow_engine"
                    self._conn.execute("""
                        INSERT OR IGNORE INTO tasks
                        (task_id, title, description, assigner, assignee, priority,
                         status, created_at, updated_at, template_id)
                        VALUES (?, ?, ?, 'workflow_engine', ?, 0,
                                'in_progress', ?, ?, ?)
                    """, (_task_id, template_id or wf_id, "", _assignee, _now, _now,
                          template_id or None))
                    self._conn.execute("""
                        INSERT OR IGNORE INTO workflow_instances
                        (instance_id, task_id, assigner, assignee, status, current_step_id,
                         template_id, created_at, context)
                        VALUES (?, ?, 'workflow_engine', ?, 'running',
                                ?, ?, ?, ?)
                    """, (wf_id, _task_id, _assignee, current_step_id,
                          template_id or None, time.time(), ctx_json))
                self._conn.commit()
                wf = self.get_wf(wf_id)
                task_id = wf.get("task_id") if wf else None
                # 真人发起 → 写 assigner 列使审批人得以算出，否则 assign器=workflow_engine 被豁免审批。
                # 写在 UPDATE 后（无论 rowcount）: engine.start 路径在 L237 UPDATE 命中，
                # 必须在此同步改写 assigner 列，否则实例永久 assigner=workflow_engine。
                _assigner = initiator or self.role
                if _assigner in ("workflow_engine", "system", ""):
                    _assigner = "workflow_engine"
                if _assigner != "workflow_engine":
                    self._conn.execute(
                        "UPDATE workflow_instances SET assigner=? WHERE instance_id=?",
                        (_assigner, wf_id))
                self._log(wf_id, task_id, "wf_started",
                          detail=f"workflow started by {self.role}, initiator={initiator or self.role}, step={current_step_id}")
                return True
            except Exception:
                self._conn.rollback()
                raise

    def complete_step(self, wf_id: str, step_id: str) -> str:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                wf = self._get_wf_unsafe(wf_id)
                if not wf:
                    raise ValueError(f"workflow 不存在: {wf_id}")
                if wf.get("current_step_id") != step_id:
                    raise ValueError(
                        f"步骤不匹配: 当前步骤={wf['current_step_id']}, 传入={step_id}")

                results = self._parse_results(wf)
                cur_status = results.get(step_id, {}).get("status")
                if cur_status in ("step_done_ready", "completed"):
                    self._conn.commit()
                    return "already_completed"

                step = self.get_step(wf_id, step_id)
                if not step:
                    raise ValueError(f"步骤定义不存在: {step_id}")
                step_type = step.get("type", "single")
                task_id = wf.get("task_id")

                # ── 双节点完成机制（2026-08-09 需求）──
                # 步骤声明 dual_node: true 时，第一步完成（bus 到）只标记 node1_done，
                # 引擎注入 codex 审查指令后，第二条消息到才真正完成。
                # 角色不知双节点——第二步由引擎在 _tick 中注入。
                if step.get("dual_node"):
                    _n1 = results.get(step_id, {}).get("node1_done", False)
                    if not _n1:
                        # 第一节点到达：标记 node1_done，删除 status 字段防止
                        # 残留 step_done_ready 驱动 _advance 推进（2026-08-09 根因）
                        _sr = results.setdefault(step_id, {})
                        _sr["node1_done"] = True
                        _sr["node1_at"] = time.time()
                        # 保持 status=notified（不 pop）——status 缺失会让引擎下次 tick
                        # 重建覆盖双节点状态（2026-08-10 根因：status pop 后 node1_done 丢失）
                        _sr["status"] = "notified"
                        self._conn.execute(
                            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                            (json.dumps(results, ensure_ascii=False), wf_id))
                        self._conn.commit()
                        return "node1_done"
                    # 第二节点已到 → 走正常完成

                handler = {
                    "single": self._complete_single_unsafe,
                    "handoff": self._complete_handoff_unsafe,
                    "review": self._complete_review_unsafe,
                    "gate": self._complete_gate_unsafe,
                    "notify": self._complete_notify_unsafe,
                    "subflow": self._complete_subflow_unsafe,
                }
                fn = handler.get(step_type)
                if not fn:
                    raise ValueError(f"未知步骤类型: {step_type}")
                result = fn(wf_id, step_id, step, task_id, results)
                self._conn.commit()
                return result
            except Exception:
                self._conn.rollback()
                raise

    def _get_assigner_for_step(self, wf: dict, step: dict, step_id: str) -> str:
        """返回应确认此步骤的角色（分配者链）。

        handoff: 第1步→发起者, 第N步→第N-1步的target_role
        review: 上一步的 target_role；同角色则回退发起者，仍无则放行
        subflow: 同 handoff 的链式规则（子链产出必须经父链确认才落地）
        single/gate/notify: 无需确认, 返回空

        review 绝不能返回 target_role 自身：执行者自审 = 无人可批，token 必然
        烂在 step_results 里等 TTL(1h)+grace(30min)。实测 architect_full_design/s4
        因此卡死 61 分钟，同时把 product_architect 判忙，锁死同角色 3 个其它工作流
        （排队 7~21 次）。返回空表示「无审批人」→ 不发 token → 引擎直接推进。

        返回值有两处消费：发放 approval_token，以及 confirm_step 校验调用者
        是否有权确认/驳回（本文件 :404-410）。handoff 不发 token（见
        _complete_handoff_unsafe），但此处的计算必须保留 —— 抽空会让
        confirm_step 的授权校验失效，任何角色都能把 handoff 打成 rejected。
        """
        st = step.get("type", "")
        target = step.get("target_role", "")
        if st == "review":
            template = self._get_template(wf.get("template_id"))
            if not template:
                return ""
            steps = template.get("steps", [])
            for i, s in enumerate(steps):
                if s.get("step_id") != step_id:
                    continue
                # 首步 review 没有「上一步产出」可审 → 语义上就是自审。
                # 旧实现落到 wf.assigner，而引擎自动建的返工链把 assigner 写死
                # "reviewer"（wf_lifecycle._create_rework_subflow）——reviewer
                # 从不知道这条链存在，token 无人消费，白等 TTL+grace 90 分钟
                # （实测 wf_rw_46e42f725e74/s1）。
                if i == 0:
                    return ""
                prev = steps[i - 1].get("target_role", "")
                if prev and prev != target and prev not in ("workflow_engine", "system"):
                    return prev
                assigner = wf.get("assigner", "")
                if assigner and assigner != target and assigner not in ("workflow_engine", "system"):
                    return assigner
                return ""
            return ""
        if st in ("handoff", "subflow"):
            template = self._get_template(wf.get("template_id"))
            if not template:
                return ""
            steps = template.get("steps", [])
            for i, s in enumerate(steps):
                if s.get("step_id") == step_id:
                    if i == 0:
                        assigner = wf.get("assigner", "")
                        # 系统角色（workflow_engine）不是真人，无法审批 → 跳过 approval
                        if assigner in ("workflow_engine", "system", ""):
                            return ""
                        return assigner
                    prev = steps[i - 1].get("target_role", "")
                    if prev in ("workflow_engine", "system"):
                        return ""
                    return prev
        return ""


    def get_approval_token(self, wf_id: str, step_id: str) -> Optional[str]:
        """获取步骤的当前审批密钥（测试辅助）。"""
        wf = self._get_wf_unsafe(wf_id)
        if not wf:
            return None
        results = self._parse_results(wf)
        return results.get(step_id, {}).get("approval_token")

    def confirm_step(self, wf_id: str, step_id: str, token: str = "",
                     approved: bool = True, reason: str = "", allow_expired: bool = False) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                wf = self._get_wf_unsafe(wf_id)
                if not wf:
                    raise ValueError(f"workflow 不存在: {wf_id}")

                results = self._parse_results(wf)
                current = results.get(step_id, {})
                if current.get("status") != "step_done_ready":
                    raise ValueError(
                        f"步骤 {step_id} 状态为 {current.get('status','?')}，非 step_done_ready")

                # ── 审批密钥验证 ──
                stored_token = current.get("approval_token")
                if stored_token:
                    if not token:
                        raise PermissionError("approval token required but not provided")
                    if current.get("approval_consumed_at"):
                        raise PermissionError("token already used")
                    if time.time() > current.get("approval_token_ttl", 0) and not allow_expired:
                        raise PermissionError("token expired")
                    if token != stored_token:
                        raise PermissionError("invalid token")
                    current["approval_consumed_at"] = time.time()
                    current["approval_token"] = None
                else:
                    step = self.get_step(wf_id, step_id)
                    if step:
                        assigner = self._get_assigner_for_step(wf, step, step_id)
                        if assigner and self.role != assigner:
                            raise PermissionError(
                                f"only assigner can confirm: {self.role} != {assigner}")

                # ── subflow 步骤: 确认前检查子工作流是否完成 ──
                step_def = self.get_step(wf_id, step_id)
                if step_def and step_def.get("type") == "subflow":
                    sub_wf_id = current.get("sub_wf_id") or current.get("subflow_id", "")
                    if sub_wf_id:
                        child = self._get_wf_unsafe(sub_wf_id)
                        if not child:
                            raise ValueError(f"子工作流 {sub_wf_id} 不存在")
                        if child["status"] == "failed":
                            raise ValueError(
                                f"子工作流 {sub_wf_id} 已失败，无法审批父步骤")
                        if child["status"] != "completed":
                            raise ValueError(
                                f"子工作流 {sub_wf_id} 未完成 (status={child['status']})")

                completed_by = current.get("completed_by", "")
                task_id = wf.get("task_id")

                if approved:
                    current["status"] = "completed"
                    current["confirmed_at"] = time.time()
                    current["confirmed_by"] = self.role
                    results[step_id] = current
                    self._conn.execute(
                        "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(results, ensure_ascii=False), wf_id)
                    )
                    self._advance_unsafe(wf_id, task_id)
                    self._log_unsafe(wf_id, task_id, "step_confirmed",
                                     detail=f"{step_id} confirmed by {self.role}")
                    self._conn.commit()
                    self._notify_result(completed_by, wf_id, step_id, True, reason)
                    return True
                else:
                    current["status"] = "rejected"
                    current["rejected_at"] = time.time()
                    current["rejected_by"] = self.role
                    current["rejection_reason"] = reason
                    results[step_id] = current
                    self._conn.execute(
                        "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(results, ensure_ascii=False), wf_id)
                    )
                    self._log_unsafe(wf_id, task_id, "step_rejected",
                                     detail=f"{step_id} rejected by {self.role}: {reason}")
                    self._conn.commit()
                    self._notify_result(completed_by, wf_id, step_id, False, reason)
                    return False
            except Exception:
                self._conn.rollback()
                raise

    def _notify_result(self, executor: str, wf_id: str, step_id: str,
                       approved: bool, reason: str):
        """通知原角色审批结果"""
        import logging as _log
        logger = _log.getLogger("lifecycle.notify")
        if not executor:
            return
        try:
            from workflow.client import get_ccs_cli as _get_ccs_cli
            import subprocess as _sp
            status = "✅ 通过" if approved else "❌ 拒绝"
            msg = (
                f"[workflow] 审批结果通知\n\n"
                f"工作流: {wf_id}\n"
                f"步骤: {step_id}\n"
                f"审批人: {self.role}\n"
                f"结果: {status}\n"
            )
            if reason:
                msg += f"原因: {reason}\n"
            if not approved:
                msg += "\n请根据拒绝原因修改后重新提交。"
            _ccs_cli = str(_get_ccs_cli()) if not isinstance(_get_ccs_cli(), str) else _get_ccs_cli()
            result = _sp.run(
                ["python3", _ccs_cli, "send", executor, msg,
                 "--from", "workflow_engine"],
                capture_output=True, timeout=15)
            if result.returncode != 0:
                logger.warning("notify result failed for %s: %s", executor,
                               result.stderr.decode()[:200])
        except Exception as e:
            logger.warning("notify result error: %s", e)

    def cancel_step(self, wf_id: str, step_id: str) -> bool:
        """取消步骤: 标记 cancelled, 工作流不推进（角色不响应时中止等待）。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                wf = self._get_wf_unsafe(wf_id)
                if not wf:
                    raise ValueError(f"workflow 不存在: {wf_id}")
                if wf.get("current_step_id") != step_id:
                    raise ValueError(
                        f"步骤不匹配: 当前={wf['current_step_id']}, 传入={step_id}")
                results = self._parse_results(wf)
                # 合并而非覆写：旧实现 `results[step_id] = {...}` 整体替换，
                # 抹掉 subflow_id / queued / exit_messages 等字段。后果实测：
                # 父链步骤被 cancel 后 subflow_id 丢失 → 子链完成后的回流通道
                # （task_generator._scan_tasks 按 step_results[step].subflow_id 找子链）
                # 断裂 → 只能退回等 exit_condition 文本匹配，实测延迟 60 分钟才闭合
                # （wf_0e8885c48cbb/s2：子链 11:31 被误取消，pm 11:42 已写出
                # s2-complete，父链却 12:42 才完成）。
                _prev_sr = results.get(step_id, {})
                results[step_id] = {
                    **(_prev_sr if isinstance(_prev_sr, dict) else {}),
                    "status": "cancelled", "cancelled_at": time.time(),
                    "cancelled_by": self.role,
                    "reason": "角色持续不响应（tmux 信号自动取消）",
                }
                # 同步关闭工作流：cancel_step 的语义是"中止等待"，步骤已终结则该实例
                # 不会再推进。只写 step_results 会让行级 status 永远停在 running——
                # 回收器（_cleanup_stale_workflows）按行级 status 取候选，而这些实例的
                # 步骤级已是 cancelled，不会被任何条件命中 → 永久滞留。
                # 实测 2026-10-17：WL-dev_with_collab 14 台 @s4 行级 running / 步骤级
                # cancelled 达 5.5-6.1h，是 running 存量里最大的一块死重。
                self._conn.execute(
                    "UPDATE workflow_instances SET step_results=?, status='cancelled', "
                    "completed_at=? WHERE instance_id=?",
                    (json.dumps(results, ensure_ascii=False), time.time(), wf_id)
                )
                self._log_unsafe(wf_id, wf.get("task_id"), "step_cancelled",
                                 detail=f"{step_id}: 角色不响应自动取消")
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def fail_step(self, wf_id: str, step_id: str,
                  reason: str = "", allow_retry: bool = False) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                wf = self._get_wf_unsafe(wf_id)
                if not wf:
                    raise ValueError(f"workflow 不存在: {wf_id}")
                if wf.get("current_step_id") != step_id:
                    raise ValueError(
                        f"步骤不匹配: 当前={wf['current_step_id']}, 传入={step_id}")

                results = self._parse_results(wf)
                task_id = wf.get("task_id")

                if allow_retry:
                    results[step_id] = {"status": "failed", "failed_at": time.time(),
                                        "failed_by": self.role, "reason": reason}
                    self._conn.execute(
                        "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(results, ensure_ascii=False), wf_id)
                    )
                    self._log_unsafe(wf_id, task_id, "step_failed", detail=f"{step_id}: {reason}")
                    self._conn.commit()
                    return True
                else:
                    self._conn.execute(
                        "UPDATE workflow_instances SET status='failed', completed_at=? "
                        "WHERE instance_id=?", (time.time(), wf_id)
                    )
                    # 级联取消子工作流
                    self._cascade_cancel_unsafe(wf_id, reason)
                    self._sync_task_unsafe(task_id)
                    self._log_unsafe(wf_id, task_id, "wf_failed", detail=f"step {step_id}: {reason}")
                    self._conn.commit()
                    return False
            except Exception:
                self._conn.rollback()
                raise

    def rollback_step(self, wf_id: str, step_id: str) -> bool:
        """回滚已完成步骤到 running。

        仅分配者或 coordinator 可调用。
        将指定步骤状态重置为 running，后续步骤状态重置为 pending。
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                wf = self._get_wf_unsafe(wf_id)
                if not wf:
                    raise ValueError(f"workflow 不存在: {wf_id}")
                if self.role not in (wf.get("assigner", ""), "coordinator"):
                    raise PermissionError(
                        f"only assigner/coordinator can rollback: {self.role}")
                step_def = self.get_step(wf_id, step_id)
                if not step_def:
                    raise ValueError(f"步骤不存在: {step_id}")
                results = self._parse_results(wf)
                if results.get(step_id, {}).get("status") not in ("completed", "step_done_ready"):
                    raise ValueError(f"步骤 {step_id} 未完成，不可回滚")
                results[step_id] = {"status": "running", "rolled_back": True,
                                    "rolled_back_at": time.time(), "rolled_back_by": self.role}
                # 删除后续所有步骤状态
                steps = self._get_all_steps_unsafe(wf.get("template_id"))
                found = False
                for s in steps:
                    if s.get("step_id") == step_id:
                        found = True
                    elif found:
                        results.pop(s.get("step_id"), None)
                self._conn.execute(
                    "UPDATE workflow_instances SET current_step_id=?, step_results=?, status='running' "
                    "WHERE instance_id=?", (step_id, json.dumps(results, ensure_ascii=False), wf_id)
                )
                self._log_unsafe(wf.get("instance_id"), wf.get("task_id"), "step_rolled_back",
                                 detail=f"{step_id} rolled back by {self.role}")
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def close_wf(self, wf_id: str, status: str = "completed"):
        with self._lock:
            # 可能已在调用方的事务中（_advance_production_wf 的 _conn_tx 块内先做过
            # DML，sqlite3 默认 isolation_level='' 已隐式 BEGIN）。此时再 BEGIN
            # IMMEDIATE 会抛 "cannot start a transaction within a transaction"——
            # 实测生产 wf_3d7210c2bc57 与 wf_ae31fd1c4c11 收尾时各触发过一轮，
            # 导致 conn.commit() 从未执行、完成状态不落库。
            # in_transaction 为真时复用外层事务，提交权交还调用方。
            _own_tx = not self._conn.in_transaction
            if _own_tx:
                self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "UPDATE workflow_instances SET status=?, completed_at=? "
                    "WHERE instance_id=?", (status, time.time(), wf_id)
                )
                wf = self.get_wf(wf_id)
                task_id = wf.get("task_id") if wf else None
                self._sync_task_unsafe(task_id)
                self._log_unsafe(wf_id, task_id, "wf_closed", detail=f"workflow {status}")
                if _own_tx:
                    self._conn.commit()
                return True
            except Exception:
                if _own_tx:
                    self._conn.rollback()
                raise

    # ── 子工作流级联操作 ───────────────────────────

    def _cascade_cancel_unsafe(self, wf_id: str, reason: str = ""):
        """级联取消所有子工作流（迭代实现，避免递归栈溢出）。"""
        _stack = [wf_id]
        while _stack:
            _current = _stack.pop()
            _children = self._conn.execute(
                "SELECT instance_id, task_id FROM workflow_instances "
                "WHERE parent_wf_id=? AND status NOT IN ('completed', 'cancelled', 'failed')",
                (_current,)
            ).fetchall()
            for _child in _children:
                _cid = _child["instance_id"]
                _ctid = _child["task_id"]
                self._conn.execute(
                    "UPDATE workflow_instances SET status='cancelled', completed_at=? "
                    "WHERE instance_id=?", (time.time(), _cid))
                self._conn.execute(
                    "UPDATE tasks SET status='cancelled' WHERE task_id=?", (_ctid,))
                self._log_unsafe(_cid, _ctid, "wf_cancelled",
                                 detail=f"cascade from parent {_current}: {reason}")
                _stack.append(_cid)


    # ── 内部 unsafe 方法（在已开启的事务中调用） ──

    def _get_wf_unsafe(self, wf_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM workflow_instances WHERE instance_id=?", (wf_id,)
        ).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _parse_results(wf: dict) -> dict:
        if wf.get("step_results"):
            try:
                return json.loads(wf["step_results"])
            except (json.JSONDecodeError, TypeError):
                pass  # must-silent: step_results may not be valid JSON yet
        return {}

    def _log_unsafe(self, wf_id: str, task_id: str = None,
                    action: str = "", detail: str = ""):
        self._conn.execute(
            "INSERT INTO workflow_logs (workflow_instance_id, task_id, "
            "action, actor, detail, ts) VALUES (?, ?, ?, ?, ?, ?)",
            (wf_id, task_id, action, self.role, detail, time.time())
        )

    def _set_step_result_unsafe(self, wf_id: str, step_id: str,
                                 status: str, results: dict):
        _existing = results.get(step_id, {})
        if isinstance(_existing, dict):
            _existing.update({"status": status, "completed_at": time.time(),
                              "completed_by": self.role})
            results[step_id] = _existing
        else:
            results[step_id] = {"status": status, "completed_at": time.time(),
                                "completed_by": self.role}
        self._conn.execute(
            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
            (json.dumps(results, ensure_ascii=False), wf_id)
        )

    # ── 步骤类型处理 ────────────────────────────

    def _complete_single_unsafe(self, wf_id, step_id, step, task_id, results):
        self._set_step_result_unsafe(wf_id, step_id, "completed", results)
        self._advance_unsafe(wf_id, task_id)
        self._log_unsafe(wf_id, task_id, "step_completed",
                         detail=f"single auto-advance: {step_id}")
        return "completed_and_advanced"

    def _complete_handoff_unsafe(self, wf_id, step_id, step, task_id, results):
        """交接步骤置 step_done_ready，**不发**审批 token。

        handoff 是交接不是闸门：发 token 会让 engine 拦住每一步等 confirm，
        而实测真实 confirm 率仅 1.4%（1347 步中 425 步持未消费 token、真
        confirm 8 次）→ 每步白等 TTL+grace 90min，早于步骤超时上限被
        auto-cancel，WL-full_dev_pipeline 21 实例 0 走完。
        无 token 时 engine 审批门控放行 → _advance_production_wf 接手推进，
        子链创建/loop/部署三件事照常执行（若在此直接 advance，防重守卫会因
        current_step_id 已变而 return，这三件全部跳过）。
        """
        self._set_step_result_unsafe(wf_id, step_id, "step_done_ready", results)
        self._log_unsafe(wf_id, task_id, "step_done_ready",
                         detail=f"handoff ready to advance: {step_id}")
        return "step_done_ready"

    def _complete_review_unsafe(self, wf_id, step_id, step, task_id, results):
        self._set_step_result_unsafe(wf_id, step_id, "step_done_ready", results)
        self._log_unsafe(wf_id, task_id, "step_done_ready",
                         detail=f"review waiting approval: {step_id}")
        self._issue_approval_token(wf_id, step_id, step, task_id, results)
        return "step_done_ready"

    def _issue_approval_token(self, wf_id, step_id, step, task_id, results):
        """生成一次性审批密钥，存入 step_results，ccs send-safe 给 assigner。"""
        import secrets as _secrets
        import logging as _log
        logger = _log.getLogger("lifecycle.approval")

        # 确定审批人。必须在写 token **之前**判：无审批人却先落 token，
        # engine 的审批门控只看"token 存在"（engine.py 审批门控段）→ 照样拦住
        # 推进，等 TTL+grace 90 分钟。实测 architect_full_design/s4 因此卡死。
        wf = self._get_wf_unsafe(wf_id)
        assigner = self._get_assigner_for_step(wf, step, step_id)
        if not assigner:
            return  # 无审批人（系统发起/自审/首步）→ 不落 token，engine 直接放行

        token = _secrets.token_urlsafe(32)
        ttl = 3600  # 1 小时

        step_results = self._parse_results(wf)
        step_results.setdefault(step_id, {}).update({
            "approval_token": token,
            "approval_token_ttl": time.time() + ttl,
            "approval_consumed_at": None,
        })
        self._conn.execute(
            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
            (json.dumps(step_results, ensure_ascii=False), wf_id))

        # ── 先提交，再 spawn 子进程 ──
        # complete_step 在外层持有 BEGIN IMMEDIATE 写锁；ccs send 子进程会经
        # routing.gatekeeper._log_cross_role_send 向同一 DB 写 cross_role_send 日志，
        # 争同一把写锁 → child 等 busy_timeout(10s) 后抛 database is locked。
        # 实测：持锁时 10/10 子进程失败（journal 里 ccs send failed 耗时恒 ~10s 即指纹），
        # 不持锁时 rc=0。审批请求因此长期走 bus 降级（token 被脱敏，审批人拿不到密钥）。
        # 本方法是事务内最后一步，此处 commit 不影响后续原子性。
        self._conn.commit()

        # 构建审批提示词
        approval_prompt = step.get("approval_prompt", "")
        if not approval_prompt:
            step_title = step.get('title', '')
            completed_by = results.get(step_id, {}).get('completed_by', 'unknown')
            step_prompt = (step.get('prompt_template', '') or '')[:800]
            approval_prompt = (
                f"## 审批请求: 步骤 {step_id} - {step_title}\n\n"
                f"### 基本信息\n"
                f"- 工作流: {wf_id}\n"
                f"- 步骤: {step_id} - {step_title}\n"
                f"- 执行人: {completed_by}\n"
                f"- 审批密钥: {token}（有效期 1 小时）\n\n"
                f"### 审查要点\n"
                f"1. 完整性 - 是否完成步骤要求的所有产出？\n"
                f"2. 正确性 - 产出物是否符合质量标准？\n"
                f"3. 可追溯性 - 产出物路径和来源是否可验证？\n"
                f"4. 风险 - 是否存在未提及的风险或副作用？\n\n"
                f"### 步骤原始要求\n"
                f"{step_prompt}\n"
            )

        # ccs send-safe 给 assigner
        try:
            from workflow.client import get_ccs_cli as _get_ccs_cli
            import subprocess as _sp
            msg = (
                f"[workflow] 步骤审批请求\n\n"
                f"工作流: {wf_id}\n"
                f"步骤: {step_id} — {step.get('title', '')}\n"
                f"密钥: {token}\n"
                f"有效期: 1小时\n\n"
                f"审批提示:\n{approval_prompt}"
            )
            _ccs_cli = str(_get_ccs_cli()) if not isinstance(_get_ccs_cli(), str) else _get_ccs_cli()
            result = _sp.run(
                ["python3", _ccs_cli, "send", assigner, msg,
                 "--from", "workflow_engine"],
                capture_output=True, timeout=15)
            if result.returncode != 0:
                logger.warning("ccs send failed for %s->%s: %s", self.role, assigner,
                               result.stderr.decode()[:200])
                # 降级兜底：ccs send 失败时写 bus，确保审批请求不丢失
                # （WL-P2-03：ccs send 失败 → 自动降级为 bus 消息写入）
                try:
                    from workflow.client import get_ccs_cli  # noqa: F401 复用路径解析
                    self._bb_write_fallback(wf_id, step_id, token, approval_prompt, assigner)
                except Exception as _fb_err:
                    logger.warning("bus fallback failed: %s", _fb_err)
        except Exception as e:
            logger.warning("approval token CCS send error: %s", e)

    def _bb_write_fallback(self, wf_id, step_id, token, approval_prompt, assigner):
        """ccs send 失败时的 bus 降级写入（WL-P2-03 通知降级）。

        确保审批请求不因 send 失败而丢失：写入 bus cat=workflow，
        assigner 角色通过 bus 轮询可收到。
        token 脱敏：bus 为共享通道，明文泄漏 = 任意角色可伪造 confirm_step。
        """
        # 脱敏：approval_prompt 内部也嵌有 token 明文（见 _issue_approval_token L648）
        safe_prompt = approval_prompt.replace(token, "***")
        from hermes_bus.blackboard import Blackboard
        bb = Blackboard()
        bb.write(
            "workflow",
            f"[审批降级] 工作流 {wf_id} 步骤 {step_id} 审批请求（ccs send 失败，经 bus 投递）",
            evidence=f"审批人: {assigner}\n密钥: ***(仅通过 ccs send 安全投递)\n有效期: 1小时\n提示: {safe_prompt}",
            src="workflow_engine",
        )

    def _complete_subflow_unsafe(self, wf_id, step_id, step, task_id, results):
        """创建子工作流 + 启动，返回 step_done_ready（等 confirm 时检查子状态）。"""
        sub_tpl_id = step.get("subflow_template", "")
        # target_role 即为子工作流的 assignee
        sub_assignee = step.get("subflow_assignee", "") or step.get("target_role", "")
        if not sub_tpl_id or not sub_assignee:
            self._set_step_result_unsafe(wf_id, step_id, "failed", results)
            self._log_unsafe(wf_id, task_id, "subflow_failed",
                             detail="missing subflow_template or subflow_assignee/target_role")
            return "failed"

        # 构建子任务描述：父 prompt + 步骤上下文
        step_prompt = step.get("prompt_template", "")
        parent_wf = self._get_wf_unsafe(wf_id)
        parent_task_id = parent_wf.get("task_id", "") if parent_wf else ""
        sub_title = f"[subflow] {step.get('title', sub_tpl_id)}"
        sub_desc = f"Parent: {wf_id} / {task_id}\nStep: {step_id}\nTemplate: {sub_tpl_id}\nPrompt:\n{step_prompt}"

        # 创建子 workflow_instance（通过 raw SQL，避免触发 Gate 递归）
        sub_wf_id = f"wf_{int(time.time()*1000) % 100000000}"
        child_instance_id = f"task_{__import__('uuid').uuid4().hex[:8]}"
        now = time.time()
        self._conn.execute("""
            INSERT INTO tasks (task_id, title, description, assigner, assignee,
                               status, created_at, updated_at, parent_task_id)
            VALUES (?, ?, ?, ?, ?, 'created', ?, ?, ?)
        """, (child_instance_id, sub_title, sub_desc, self.role, sub_assignee,
              now, now, parent_task_id))
        self._conn.execute("""
            INSERT INTO workflow_instances
                (instance_id, template_id, task_id, assigner, assignee,
                 status, current_step_id, created_at,
                 parent_wf_id, subflow_source_step_id)
            VALUES (?, ?, ?, ?, ?, 'pending', 's1', ?, ?, ?)
        """, (sub_wf_id, sub_tpl_id, child_instance_id, self.role, sub_assignee,
              now, wf_id, step_id))

        # 启动子工作流
        self._conn.execute(
            "UPDATE workflow_instances SET status='running' WHERE instance_id=?",
            (sub_wf_id,))
        self._conn.commit()

        # 记录到 step_results
        results[step_id] = {
            "status": "step_done_ready",
            "sub_wf_id": sub_wf_id,
            "sub_task_id": child_instance_id,
            "sub_template_id": sub_tpl_id,
            "completed_at": now,
            "completed_by": self.role,
        }
        self._conn.execute(
            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
            (json.dumps(results, ensure_ascii=False), wf_id))
        self._log_unsafe(wf_id, task_id, "subflow_created",
                         detail=f"→ {sub_wf_id} ({sub_tpl_id})")
        self._issue_approval_token(wf_id, step_id, step, task_id, results)
        return "step_done_ready"

    def _complete_gate_unsafe(self, wf_id, step_id, step, task_id, results):
        check = step.get("completion_check", {})
        passed, msg = self._check_gate_condition(check)
        if passed:
            self._set_step_result_unsafe(wf_id, step_id, "completed", results)
            self._advance_unsafe(wf_id, task_id)
            self._log_unsafe(wf_id, task_id, "gate_passed",
                             detail=f"gate passed: {step_id}: {msg}")
            return "completed_and_advanced"
        else:
            self._set_step_result_unsafe(wf_id, step_id, "blocked", results)
            self._log_unsafe(wf_id, task_id, "gate_blocked",
                             detail=f"gate blocked: {step_id}: {msg}")
            return "gate_blocked"

    def _complete_notify_unsafe(self, wf_id, step_id, step, task_id, results):
        target = step.get("target_role", "")
        self._set_step_result_unsafe(wf_id, step_id, "completed", results)
        self._advance_unsafe(wf_id, task_id)
        self._log_unsafe(wf_id, task_id, "notify_sent",
                         detail=f"notify sent to {target}: {step_id}")
        return "completed_and_advanced"

    def _check_gate_condition(self, check: dict) -> tuple:
        if not check:
            return (True, "no conditions")
        output_exists = check.get("output_exists", [])
        if output_exists:
            checkable = [p for p in output_exists if _is_checkable_path(p)]
            if not checkable:
                # 全部是不可判定条目（如 "变更代码文件"、"deploy_success"）→ 无可校验。
                # 放行而非永久 blocked：blocked 不在 STEP_STATUSES/_ACTIVE_STEP_STATUS
                # 内，回收扫描覆盖不到，会让步骤与实例双双搁浅（仅 24h 全局阈值兜底）。
                # 放行但留痕——模板需修正为真实路径。
                import logging as _lg
                _lg.getLogger("lifecycle.gate").warning(
                    "gate 的 output_exists 无可用路径判据，跳过检查（请修正模板）: %s",
                    output_exists)
                return (True, "no checkable path in output_exists")
            missing = [p for p in checkable if not _output_present(p)]
            if missing:
                return (False, f"output not found: {', '.join(missing)}")
        return (True, "conditions satisfied")

    def _advance_unsafe(self, wf_id: str, task_id: Optional[str]) -> bool:
        wf = self._get_wf_unsafe(wf_id)
        if not wf:
            return False
        steps = self._get_all_steps_unsafe(wf.get("template_id"))
        current = wf.get("current_step_id", "")
        idx = -1
        for i, s in enumerate(steps):
            # 模板步骤可能用 id 键（模式 ^s\d+$）而非 step_id；与 L194 get_step 约定一致
            if (s.get("step_id") or s.get("id")) == current:
                idx = i
                break
        if idx == -1 or idx + 1 >= len(steps):
            # 末步：不自行 close。直接写 completed 会绕开引擎的收尾阶段
            # （子链检查 + close_wf + 锁外部署 + 食物链 loop 自触发），
            # 实测单步链 deploy=0、loop 永不启动。改为返回 False 让上层
            # （step_handler → engine 主循环）按 step_done_ready 走
            # _advance_production_wf 的 is_last 分支，拿到完整收尾。
            # idx == -1（步骤不在模板）仍自行收尾：引擎对该情况 idx<0
            # 直接 return，交出去会永久悬空。
            if idx >= 0:
                return False
            self._conn.execute(
                "UPDATE workflow_instances SET status='completed', completed_at=? "
                "WHERE instance_id=?", (time.time(), wf_id)
            )
            self._sync_task_unsafe(task_id)
            return False
        next_step = steps[idx + 1]
        # 返工门禁：评审步骤（review/gate）被驳回 → 搁置当前步，不推进。
        # 为什么在这里：本函数是「角色调 complete_step」的推进路径，与
        # pipeflow/wf_lifecycle._advance_production_wf（引擎 tick 路径）是
        # 两套独立实现，原门禁只装在后者 → 走本路径的驳回被无视，流水线
        # 带着未修复的产物继续下滑（实测 wf_98a18e738d：s3 判 CHANGES_REQUESTED
        # 却推进到 s4；回归守卫 tests/test_workcycle_e2e.py::test_gate_rejection_blocks_advance）。
        #
        # 只 return 不够：本函数四个调用点（433/707/887/900）均
        # 不检查返回值，调用方会照常记 step_completed 并 commit。
        # 故必须把 step 状态落库为 rejected（调用方无法覆盖），
        # 让下游看到真实结果而非"已完成并推进"。
        # 关键词 "fix" 不列入：它在正常文案高频出现（bugfix/prefix/fixture），
        # 会把「已修复并通过」误判为驳回。
        _cur_step = steps[idx]
        # 不限定 step.type（对齐 pipeflow/wf_lifecycle.py:298 的 R41 修复）：旧实现
        # 只查 review/gate，实测 handoff/single 的驳回无人消费——wf_3565289ca783/s3
        # (handoff) 与 wf_0830e5a05e/s1 (single) 判"未通过"/REJECTED 后仍推进至 completed。
        # 驳回语义由 exit_messages 文本判定，与 type 无关。
        _sr = self._parse_results(wf)
        _msgs = " ".join(
            m.get("text", "") for m in
            (_sr.get(current, {}).get("exit_messages") or [])
            if isinstance(m, dict)
        ).lower()
        # 词表与 wf_lifecycle.py:312 保持一致："未通过"/"驳回"/"not_approved"
        # 此前只在后者存在，本路径漏检（wf_3565289ca783/s3 用"判定: 未通过"表达驳回）。
        if any(kw in _msgs for kw in
               ("changes_requested", "rejected", "需修复", "回退",
                "未通过", "not_approved", "驳回")):
            _sr.setdefault(current, {})["status"] = "rejected"
            self._conn.execute(
                "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                (json.dumps(_sr, ensure_ascii=False), wf_id))
            import logging as _lg
            _lg.getLogger("lifecycle.review_gate").warning(
                "review rejected: %s/%s → 返工门禁触发，保持当前步", wf_id[:8], current)
            return False
        # assignee 跟随当前步的执行者：它只在建实例时设一次（首步角色），
        # 之后推进从不更新 → 实测 8/9 running 实例的 assignee 与当前步要求
        # 的角色不一致（如 WL-dev_with_collab 走到 s4 要求 closer 却仍归
        # engineer）。而 assignee 被背压计数与任务生成消费 → 配额长期失真。
        _next_sid = next_step.get("step_id") or next_step.get("id")
        _next_role = next_step.get("target_role")
        if _next_role:
            self._conn.execute(
                "UPDATE workflow_instances SET current_step_id=?, assignee=? "
                "WHERE instance_id=?", (_next_sid, _next_role, wf_id))
        else:
            self._conn.execute(
                "UPDATE workflow_instances SET current_step_id=? WHERE instance_id=?",
                (_next_sid, wf_id))
        # 旧步骤的 notified 状态误判角色 busy（2026-08-10 观测：
        # s1 推进到 s2 后，_is_role_busy 扫到 s1 的 notified → pm 误判 busy）
        try:
            _sr = self._parse_results(wf)
            _old_sid = current
            if _old_sid and _sr.get(_old_sid, {}).get("status") != "completed":
                _sr[_old_sid]["status"] = "completed"
                self._conn.execute(
                    "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                    (json.dumps(_sr, ensure_ascii=False), wf_id))
        except Exception:
            pass  # 非关键：旧步骤 status 残留仅影响 busy 判断，不影响推进
        # ── advance 时同步拉起下一棒角色，不等 daemon tick 轮询 ──
        _next_role = next_step.get("target_role", "")
        if _next_role and _next_role != wf.get("assignee", "") and self._on_advance:
            try:
                self._on_advance(_next_role)
            except Exception:
                pass  # 拉起失败不阻塞 advance
        return True

    def _sync_task_unsafe(self, task_id: Optional[str]):
        if not task_id:
            return
        from workflow.sync import sync_task_status
        sync_task_status(self._conn, task_id)

    def _get_all_steps_unsafe(self, template_id: Optional[str]) -> list:
        t = self._get_template(template_id)
        return t.get("steps", []) if t else []

    def get_assigned_workflows(self, role: str) -> List[dict]:
        """获取分配给指定角色的所有活跃工作流"""
        rows = self._conn.execute(
            """SELECT wi.*, wt.name as template_name, wt.steps_json
               FROM workflow_instances wi
               JOIN workflow_templates wt ON wi.template_id = wt.template_id
               WHERE wi.status = 'running'
               ORDER BY wi.created_at DESC""",
        ).fetchall()
        result = []
        for r in rows:
            inst = dict(r)
            steps = json.loads(inst.get("steps_json", "[]"))
            current = inst.get("current_step_id", "")
            for s in steps:
                if s.get("target_role") == role and s.get("step_id") == current:
                    result.append(inst)
                    break
        return result

    def _ensure_schema(self):
        """初始化数据库 schema（如不存在）+ 迁移已有表。

        从 workflow/db.py 导入核心 schema 避免重复定义。"""
        try:
            from workflow.db import SCHEMA_SQL as _SCHEMA_SQL
            self._conn.executescript(_SCHEMA_SQL)
        except ImportError:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS workflow_templates (
                    template_id TEXT PRIMARY KEY, name TEXT, description TEXT, steps_json TEXT,
                    created_at REAL, is_active INTEGER DEFAULT 1,
                    trigger_scene TEXT, allowed_initiators TEXT, allowed_executors TEXT,
                    max_duration_hours INTEGER DEFAULT 24, quality_standards TEXT DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS workflow_instances (
                    instance_id TEXT PRIMARY KEY, template_id TEXT, task_id TEXT,
                    assigner TEXT, assignee TEXT, status TEXT DEFAULT 'pending',
                    current_step_id TEXT DEFAULT 's1', step_results TEXT, context TEXT, created_at REAL,
                    completed_at REAL, parent_wf_id TEXT, subflow_source_step_id TEXT
                );
                CREATE TABLE IF NOT EXISTS workflow_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workflow_instance_id TEXT, task_id TEXT,
                    action TEXT, actor TEXT, detail TEXT, ts REAL
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY, title TEXT, description TEXT,
                    assigner TEXT, assignee TEXT, priority INTEGER DEFAULT 0, status TEXT,
                    template_id TEXT,
                    current_workflow_id TEXT,
                    progress TEXT DEFAULT '{}',
                    created_at REAL, updated_at REAL, completed_at REAL,
                    tags TEXT DEFAULT '[]',
                    context TEXT DEFAULT '{}',
                    parent_task_id TEXT
                );
            """)
        # 迁移：为已有表添加缺失列
        for table, col, col_def in [
            ("workflow_templates", "is_active", "INTEGER DEFAULT 1"),
            ("workflow_templates", "trigger_scene", "TEXT"),
            ("workflow_templates", "allowed_initiators", "TEXT"),
            ("workflow_templates", "allowed_executors", "TEXT"),
            ("workflow_templates", "max_duration_hours", "INTEGER DEFAULT 24"),
            ("workflow_templates", "quality_standards", "TEXT DEFAULT ''"),
            ("workflow_templates", "is_subflow", "INTEGER DEFAULT 0"),
            ("workflow_instances", "context", "TEXT DEFAULT '{}'"),
            ("workflow_instances", "parent_wf_id", "TEXT"),
            ("workflow_instances", "subflow_source_step_id", "TEXT"),
            # 步骤-角色重定向持久化（如 engineer↔pg 开发角色池按忙闲择一）。
            # 存在实例级而非改 step.target_role：模板是权威定义不动，且分流决定
            # 需随实例存活、对下游全部消费点可见（exit_messages 过滤、busy 门控、
            # exit_schema 校验目录、codex cwd 等 20+ 处）。JSON: {"s4": "pg"}
            ("workflow_instances", "effective_roles", "TEXT DEFAULT '{}'"),
            ("tasks", "parent_task_id", "TEXT"),
            ("tasks", "template_id", "TEXT"),
        ]:
            existing = {r[1] for r in self._conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if col not in existing:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")
        self._conn.commit()

    def close(self):
        self._conn.close()
