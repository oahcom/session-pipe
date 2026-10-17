#!/usr/bin/env python3
"""
Workflow Engine --- 数据驱动的对话工作流执行引擎。

通过 Sister Bus 与 CCS 会话交互，执行 JSON 定义的工作流。
工作流来自 ~/.hermes/workflows/*.json，运行状态持久化到 runs/*.json，
同时扫描 SQLite 中的 production 工作流实例并自动推进。
"""

import json
import re
import sys
import time
import uuid
import logging
import subprocess as _sp

from pathlib import Path
LOGGER = logging.getLogger("workflow.engine")
from typing import Any, Optional
from types import SimpleNamespace

from paths import ensure_paths
ensure_paths()
from paths import HERMES_WORKFLOWS as _WORKFLOWS_DIR
from paths import SESSION_ROLES_PERSONAS

from bus_protocol import Blackboard

from dataclasses import dataclass

ZOMBIE_TEMPLATES = frozenset({
    "test", "hr", "back_ccs", "busy_ccs", "stuck_ccs", "heartbeat",
    "ho", "research", "dev", "prompt_test", "cancel_test", "cond_test",
    "cond_block", "timeout_test", "retry_test", "fast_ccs", "ws_test",
    "single_wf", "two_step", "handoff_wf", "reject_wf", "cancel_wf",
    "alpha", "beta", "ok", "quick", "valid", "empty",
})

_TUNING_PATH = Path.home() / ".hermes" / "workflow_tuning.json"
def _load_tuning() -> dict:
    try:
        return json.loads(_TUNING_PATH.read_text()) if _TUNING_PATH.exists() else {}
    except Exception:
        return {}
_TUNING = _load_tuning()

# 软回退快照：daemon 在低分时指向的 last-known-good 来源
_TUNING_SNAPSHOT = Path.home() / ".hermes" / "workflow_tuning.snapshot.json"

def _resolve_eval_gate(bb) -> str | None:
    """轮 9 软回退：tuning.json 含 eval_gate_blocked + 有效 rollback_target 时，
    复制 last-known-good 快照回 tuning.json（auto_rollback 去重防回退死循环），
    写 bus code_fix 记录后放行 start()。无 rollback_target 才硬拒（兼容轮 8 旧数据）。
    只读 engine 旋钮，不触碰任何已 running 的 workflow（红线：不得中断运行中实例）。"""
    try:
        _t = json.loads(_TUNING_PATH.read_text()) if _TUNING_PATH.exists() else {}
        if not _t.get("eval_gate_blocked"):
            return None
        _target = _t.get("rollback_target")
        if _target and _TUNING_SNAPSHOT.exists():
            if _t.get("auto_rollback"):  # 已回退过则不再重复（防死循环）
                return None
            _snap = json.loads(_TUNING_SNAPSHOT.read_text())
            _snap.update({"eval_gate_blocked": True,
                          "eval_gate_reason": _t.get("eval_gate_reason", ""),
                          "eval_gate_ts": _t.get("eval_gate_ts", ""),
                          "rollback_target": _target,
                          "auto_rollback": True})
            _TUNING_PATH.write_text(json.dumps(_snap, ensure_ascii=False, indent=2))
            try:
                bb.write("code_fix",
                         f"eval 质量门禁软回退到 last-known-good 参数 (snapshot {_target})",
                         src="pipeflow-engine",
                         evidence=json.dumps({"action": "auto_rollback",
                                              "reverted_to": "snapshot", "snapshot_hash": _target}))
            except Exception:
                pass
            return None  # 已回退至 good 参数，放行新建 workflow
    except Exception:
        pass
    # 无 rollback_target（轮 8 旧数据 / 快照缺失）→ 硬拒，保持原语义
    try:
        _t = json.loads(_TUNING_PATH.read_text()) if _TUNING_PATH.exists() else {}
        return _t.get("eval_gate_reason", "eval_gate_blocked")
    except Exception:
        return "eval_gate_blocked"

_POLL_SLEEP = _TUNING.get("poll_sleep", 5)
_REMINDER_INTERVAL = _TUNING.get("reminder_interval", 120)

# 未替换变量检测：含 {xxx} 模板变量的 prompt 不应发送，否则角色 /goal 用它作
# StopHook 条件时 bool([]) 为 False → 死循环
UNMATCHED_VAR_RE = re.compile(r'\{[a-z_]+\}')


@dataclass
class Step:
    id: str
    title: str
    target_role: str
    prompt_template: str
    exit_condition: dict
    type: str = "single"  # handoff/review/single/gate/notify
    completion_check: dict = None
    max_retries: int = _TUNING.get("default_max_retries", 0)
    condition: str = ""
    rollback_to: str = ""
    verify: str = ""
    failure_patterns: list[str] = None
    subflow_template: str = ""  # 该步骤对应的子工作流模板名
    # 协作能力声明（旧 ccs.py --partner/--bus-track 收编）
    # guard: 额外守护的角色（target_role 已由引擎自动守护，watch 定义 target_role 之外的）
    #   {"watch": ["role_a"], "auto_restart": true}
    # track: 步骤执行期间的轮次追踪（与 exit_condition 一次性退出不同，这是持续轮询）
    #   {"bus_category": "cat", "timeout_sec": 300, "notify_roles": ["role_c"]}
    dual_node: bool = False  # 双节点完成机制：第一节点写 bus 后引擎注入 codex 审查，第二条 bus 才推进
    exit_schema: dict = None  # 退出校验 schema（文件校验/内容校验/自定义校验）
    # 主从模式：guard 声明额外守护角色（target_role 已自动守护），
    # 引擎每 tick 检查存活，死了拉起（_collect_guard_roles + _ensure_guard_roles）
    guard: dict = None  # CCS 守护声明 {"watch": ["role_a"], "auto_restart": true}
    # 对等模式：track 声明轮次追踪，超时提醒 notify_roles，节流防重复，
    # 伙伴恢复（新消息到达）自动停止告警（_tick track 段）
    track: dict = None  # 轮次追踪声明 {"bus_category": "cat", "timeout_sec": 300, "notify_roles": [...]}
    # 仲裁模式：wait_roles 声明协作门——所有角色都产出匹配消息才推进
    # （_check_exit 门控；source_contains/relaxed-advance/tmux 信号均不绕过）
    wait_roles: list = None  # 协作门：等这些角色都产出匹配消息才推进（仲裁/对等模式）


@dataclass
class WorkflowDef:
    name: str
    title: str
    description: str
    steps: list[Step]
    workflow_id: str = ""
    trigger_scene: list[str] = None
    allowed_initiators: list[str] = None
    allowed_executors: list[str] = None
    max_duration_hours: int = 24
    quality_standards: str = ""
    loop: Optional[dict] = None
    is_subflow: bool = False  # True = 仅限子工作流调用，不能独立 start
    auto_advance: bool = False  # True = daemon 自动审批 handoff/review 步骤（自驱动模板）


@dataclass
class WorkflowRun:
    id: str
    workflow_name: str
    context: dict
    current_step: str = ""
    step_retries: dict[str, int] = None
    status: str = "running"
    created_at: float = None
    updated_at: float = None
    step_results: dict[str, Any] = None

    def __post_init__(self):
        self.step_retries = self.step_retries or {}
        self.step_results = self.step_results or {}
        self.created_at = self.created_at or time.time()
        self.updated_at = self.updated_at or time.time()


# ── 混入模块：方法签名和 self 访问完全不变 ─────────────────────────
# 注意：混入导入必须放在 dataclass 定义之后，mixin 顶层依赖这些类。
from pipeflow.step_handler import StepHandlerMixin
from pipeflow.role_lifecycle import RoleLifecycleMixin
from pipeflow.task_generator import TaskGeneratorMixin
from pipeflow.wf_lifecycle import WorkflowLifecycleMixin


class WorkflowEngine(WorkflowLifecycleMixin, TaskGeneratorMixin, RoleLifecycleMixin, StepHandlerMixin):
    """工作流引擎主类。各 mixin 提供具体职责，此类保留编排与 CLI。"""

    # _sp: 子进程引用，供 mixin 方法通过 self._sp 访问（测试 patch("pipeflow.engine._sp") 可替换）
    _sp = _sp

    def __init__(self, workflows_dir: Path = _WORKFLOWS_DIR, db_path: str = None):
        self.workflows_dir = Path(workflows_dir).expanduser()
        self.runs_dir = self.workflows_dir / "runs"
        # 非默认目录（测试/沙箱）→ 使用目录内独立 DB，避免污染生产 workflows.db
        # bus 隔离由 tests/conftest.py 完成：它 patch hermes_bus.blackboard_legacy
        # 与 hermes_bus.config 的 BLACKBOARD_DB 快照，故此处无参构造的 Blackboard()
        # 在 pytest 下指向 /tmp/pipeline-test-state-*/blackboard.db。
        # 实测（2026-10-16）：bb.db_path = /tmp/pipeline-test-state-*/blackboard.db，
        # 未命中生产路径。裸跑（不经 conftest）仍连生产 —— 这是 Blackboard() 无参
        # 构造的固有语义，不由本处负责。
        self._db_path = db_path or (
            str(self.workflows_dir / "test_workflows.db")
            if self.workflows_dir != Path(_WORKFLOWS_DIR)
            else None
        )
        self._bb = Blackboard()
        self._workflows: dict[str, WorkflowDef] = {}
        self._lm = None
        self._consumed_exit_ids: set[int] = set()
        self._load_workflows()

    @property
    def _lifecycle(self) -> 'LifecycleManager':
        """延迟初始化 LifecycleManager（首次访问时创建）。"""
        if self._lm is None:
            from lifecycle.manager import LifecycleManager
            self._lm = LifecycleManager("workflow_engine", db_path=self._db_path,
                                        on_advance=self._ensure_role_alive)
        return self._lm

    def _load_workflows(self):
        """加载工作流模板到内存 _workflows 字典。

        路径 1（优先）：扫描 workflows_dir/*.json → 结构校验 → 质量检查 → 写入内存
        路径 2（DB 补充，仅默认目录）：读 workflow_templates 表 → JSON 同名时跳过 DB 版

        为何路径 1 优先：JSON 文件由模板所有者手工维护，是权威源；DB 模板由引擎运行时
        写入（_upsert_template），是简化快照。JSON 优先防止简化版覆盖完整定义
        （bus #167084 根因：s4 步骤 exit_condition 变空 → 工作流卡死）。
        """
        if not self.workflows_dir.exists():
            return
        # 角色 collab 缓存（persona 声明，模板未显式声明时注入）
        # 直接读 JSON 避免跨项目 import（pipeline 不依赖 launcher 的 routing）
        # 按目录 mtime 缓存：persona 变更频率远低于模板，避免每 tick 重读 29 个 JSON
        _role_collab: dict[str, dict] = {}
        _roles_dir = SESSION_ROLES_PERSONAS
        if _roles_dir.exists():
            _dir_mtime = _roles_dir.stat().st_mtime
            if (getattr(self, "_collab_mtime", None) != _dir_mtime
                    or not getattr(self, "_role_collab_cache", None)):
                self._collab_mtime = _dir_mtime
                self._collab_reload: dict[str, dict] = {}
                for _pf in _roles_dir.glob("persona_*.json"):
                    try:
                        _pd = json.loads(_pf.read_text())
                        _name = _pd.get("name", "")
                        _collab = _pd.get("collab")
                        if _name and _collab:
                            self._collab_reload[_name] = _collab
                    except Exception:
                        LOGGER.debug("persona 读取失败: %s", _pf.name)
                self._role_collab_cache = dict(self._collab_reload)  # 浅拷贝防引用变异
            _role_collab = self._role_collab_cache
        for f in sorted(self.workflows_dir.glob("*.json")):
            if f.parent == self.runs_dir:
                continue
            try:
                data = json.loads(f.read_text())
                steps = []
                for _s in data.get("steps", []):
                    _clean = {k: v for k, v in _s.items() if k in Step.__dataclass_fields__}
                    # persona collab 注入：模板未显式声明 guard/track 时，
                    # 用 target_role persona 的 collab 字段（声明一次全模板生效）
                    # 仅当 guard 和 track 都未声明时注入：模板显式声明任一字段
                    # = 作者意图"此步骤仅此能力"，persona 不应补充另一项（2026-08-12 codex P1）
                    _tr = _clean.get("target_role", "")
                    if _tr and not _clean.get("guard") and not _clean.get("track"):
                        _rc = _role_collab.get(_tr) or {}
                        if not _clean.get("guard") and _rc.get("guard"):
                            _g = _rc["guard"]
                            # 结构校验：watch 必须是字符串数组，否则遍历字符会拉起不存在的角色
                            if isinstance(_g, dict) and isinstance(_g.get("watch"), list) \
                                    and all(isinstance(r, str) for r in _g.get("watch", [])):
                                # 深拷贝注入: _g 是 persona 缓存引用, 运行期修改会污染缓存
                                _clean["guard"] = json.loads(json.dumps(_g))
                            else:
                                LOGGER.warning("collab guard 结构非法，跳过注入: %s", _tr)
                        if not _clean.get("track") and _rc.get("track"):
                            _t = _rc["track"]
                            if isinstance(_t, dict) and isinstance(_t.get("bus_category"), str) \
                                    and _t.get("bus_category"):
                                _clean["track"] = json.loads(json.dumps(_t))
                            else:
                                LOGGER.warning("collab track 结构非法，跳过注入: %s", _tr)
                    # 统一结构校验（覆盖模板显式声明 + persona 注入两来源）：
                    # 非法 watch/notify_roles 会在消费点逐字符遍历 → 拉起不存在角色
                    _g2 = _clean.get("guard")
                    if isinstance(_g2, dict) and (not isinstance(_g2.get("watch"), list)
                                                  or not all(isinstance(r, str) for r in _g2.get("watch", []))):
                        LOGGER.warning("guard 结构非法，丢弃: %s/%s", f.name, _tr)
                        _clean["guard"] = None
                    _t2 = _clean.get("track")
                    if isinstance(_t2, dict):
                        if not isinstance(_t2.get("bus_category"), str) or not _t2.get("bus_category"):
                            LOGGER.warning("track 结构非法，丢弃: %s/%s", f.name, _tr)
                            _clean["track"] = None
                        elif not isinstance(_t2.get("notify_roles", []), list) \
                                or not all(isinstance(r, str) for r in _t2.get("notify_roles", [])):
                            LOGGER.warning("track.notify_roles 非法，丢弃: %s/%s", f.name, _tr)
                            _clean["track"] = None
                    _wr = _clean.get("wait_roles")
                    if _wr is not None and (not isinstance(_wr, list)
                                            or not all(isinstance(r, str) for r in _wr)):
                        LOGGER.warning("wait_roles 结构非法，丢弃: %s/%s", f.name, _tr)
                        _clean["wait_roles"] = None
                    elif isinstance(_wr, list) and len(_wr) == 0:
                        LOGGER.warning("wait_roles 空列表（语义=无门控），建议删掉字段: %s/%s", f.name, _tr)
                        _clean["wait_roles"] = None
                    steps.append(Step(**_clean))
                # 模板结构校验（allow_minimal=True 仅校验核心字段）：
                # 不通过 → error + 拒绝加载。防止缺 id/target_role/exit_condition 的坏步骤
                # 进入 _workflows 后运行期卡死或误判退出（如僵尸模板 ws_test 曾进 ZOMBIE 名单）
                try:
                    from template_registry import _validate_schema as _vschema
                    _vr = _vschema(data, allow_minimal=True)
                    if not _vr.passed:
                        LOGGER.error("[wf] 模板 %s 结构校验不通过，拒绝加载:", f.name)
                        for _e in _vr.errors[:4]:
                            LOGGER.error("        - %s", _e)
                        continue  # 跳过该模板，不注册到 _workflows
                except (ValueError, KeyError, TypeError):
                    LOGGER.debug("workflow schema validation failed for %s", f.name)
                self._workflows[data["name"]] = WorkflowDef(
                    name=data["name"], title=data.get("title", ""),
                    description=data.get("description", ""), steps=steps,
                    workflow_id=data.get("workflow_id", "WL-" + data["name"]),
                    trigger_scene=data.get("trigger_scene") or [],
                    allowed_initiators=data.get("allowed_initiators") or [],
                    allowed_executors=data.get("allowed_executors") or [],
                    max_duration_hours=data.get("max_duration_hours", 24),
                    quality_standards=data.get("quality_standards", ""),
                    loop=data.get("loop"),
                    is_subflow=data.get("is_subflow", False),
                    auto_advance=data.get("auto_advance", False),
                )
            except Exception as e:
                    LOGGER.warning("[wf] 加载 %s 失败: %s", f.name, e)
        # 将内存中的 JSON 模板 upsert 到 DB（供 lifecycle manager 查询）。
        # 这解决了 subflow 模板未注册到 live DB 的问题（bus #288866 根因）。
        # 只在默认目录且非测试 DB 时执行，避免测试污染生产 DB。
        # hasattr 兼容测试手动构造的实例（未执行 __init__）。
        if (self.workflows_dir == _WORKFLOWS_DIR
                and getattr(self, '_db_path', None) is None):
            try:
                for name, wf in self._workflows.items():
                    # 使用 workflow_id 作为 template_id（与 DB 约定一致）。
                    # 规范化去除所有连续 WF- 前缀（防 DB 回读后多次 upsert 叠加 WF-
                    # 导致 WF-WF-...-WL-... 非收敛死循环，实测 4335 行污染）。
                    tid = re.sub(r'^(?:WF-)+', '', wf.workflow_id or name)
                    if not tid.startswith(('WL-', 'WF-')):
                        tid = 'WF-' + tid
                    self._upsert_template(tid, wf)
            except Exception as e:
                LOGGER.warning("[wf] 启动时 upsert JSON 模板到 DB 失败: %s", e)

        # 也从 SQLite 加载模板（仅默认目录时）
        if self.workflows_dir != _WORKFLOWS_DIR:
            return
        try:
            lm = self._lifecycle
            rows = lm.query(
                "SELECT template_id, name, description, steps_json FROM workflow_templates"
            )
            for row in rows:
                t = dict(row)
                tid = t["template_id"]
                # JSON 定义优先：DB 中已有同名模板（简化版/旧版）→ 跳过，防止
                # upsert 的简化 steps（无 exit_condition）覆盖完整 JSON 定义
                # （bus #167084 根因：去掉此检查后 s4 exit_condition 变空 → 工作流卡死）
                if tid in self._workflows:
                    continue
                raw = json.loads(t.get("steps_json") or "[]")
                if not raw or not isinstance(raw, list) or not isinstance(raw[0], dict):
                    continue
                steps = []
                for s in raw:
                    s_clean = {}
                    for k, v in s.items():
                        if k == "step_id":
                            s_clean["id"] = v
                        elif k in Step.__dataclass_fields__:
                            s_clean[k] = v
                    if "exit_condition" not in s_clean:
                        s_clean["exit_condition"] = {}
                    if "target_role" not in s_clean:
                        continue
                    # DB 路径同样校验协作字段结构（与 JSON 路径 L218-233 一致，
                    # 2026-08-12 codex P2：防 DB 脏数据拉起不存在角色）
                    _d_wr = s_clean.get("wait_roles")
                    if _d_wr is not None and (not isinstance(_d_wr, list)
                                              or not all(isinstance(r, str) for r in _d_wr)):
                        s_clean["wait_roles"] = None
                    _d_t = s_clean.get("track")
                    if isinstance(_d_t, dict) and (not isinstance(_d_t.get("notify_roles", []), list)
                                                   or not all(isinstance(r, str) for r in _d_t.get("notify_roles", []))):
                        s_clean["track"] = None
                    try:
                        steps.append(Step(**s_clean))
                    except (ValueError, KeyError, TypeError):
                        continue
                if not steps:
                    continue
                self._workflows[tid] = WorkflowDef(
                    name=tid, title=t.get("name", tid),
                    description=t.get("description", ""), steps=steps,
                    workflow_id=t.get("template_id", tid),
                    trigger_scene=json.loads(t.get("trigger_scene") or "[]") if isinstance(t.get("trigger_scene"), str) else (t.get("trigger_scene") or []),
                    allowed_initiators=json.loads(t.get("allowed_initiators") or "[]") if isinstance(t.get("allowed_initiators"), str) else (t.get("allowed_initiators") or []),
                    allowed_executors=json.loads(t.get("allowed_executors") or "[]") if isinstance(t.get("allowed_executors"), str) else (t.get("allowed_executors") or []),
                    max_duration_hours=t.get("max_duration_hours", 24),
                    quality_standards=t.get("quality_standards", ""),
                    is_subflow=t.get("is_subflow", False),
                    auto_advance=t.get("auto_advance", False),
                )
        except Exception as e:
            LOGGER.warning("[wf] SQLite 模板加载失败: %s", e)

    def list_workflows(self) -> list[str]:
        return sorted(name for name, wf in self._workflows.items() if not wf.is_subflow)

    def active_workflow_count(self) -> int:
        """当前活跃工作流数（pending/running/step_done_ready）。"""
        try:
            rows = self._lifecycle.query(
                "SELECT COUNT(*) AS cnt FROM workflow_instances "
                "WHERE status IN ('pending', 'running', 'step_done_ready')"
            )
            return rows[0]["cnt"] if rows else 0
        except Exception as e:
            LOGGER.warning("活跃工作流查询失败: %s", e)
            return 0

    def has_active_template(self, template_id: str) -> bool:
        """指定模板是否已有活跃实例（防双路径竞态：daemon 饥饿触发与 wf_loop 同时创建）。"""
        try:
            rows = self._lifecycle.query(
                "SELECT COUNT(*) AS cnt FROM workflow_instances "
                "WHERE template_id=? AND status IN ('pending', 'running', 'step_done_ready')",
                (template_id,),
            )
            return (rows[0]["cnt"] if rows else 0) > 0
        except Exception as e:
            LOGGER.warning("活跃模板查询失败 %s: %s", template_id, e)
            return True  # 查询失败时保守跳过（宁可少创建，不可重复创建）

    def start(self, name: str, context: dict = None, initiator: str = None) -> str:
        # 轮 9：eval 质量门禁软回退 —— 只在入口检查**新建** workflow，不读取运行实例状态，
        # 因此不会中断任何已在 running 的 workflow（红线）。blocked 且有 rollback_target
        # 时会 auto_rollback 到 last-known-good 参数并放行；其余情况返回 reason 拦截。
        _gate = _resolve_eval_gate(self._bb)
        if _gate:
            LOGGER.warning("[wf] eval 质量门禁拦截新建 workflow=%s: %s", name, _gate)
            try:
                self._bb.write("code_fix",
                               f"eval 质量门禁拦截新建 workflow {name}: {_gate}",
                               src="pipeflow-engine",
                               evidence=json.dumps({"blocked": True, "reason": _gate, "workflow": name}))
            except Exception:
                pass
            raise RuntimeError(f"eval 质量门禁已启用，新建 workflow 被拦截: {_gate}")
        wf = self._workflows.get(name)
        if wf:
            if wf.is_subflow:
                raise ValueError(f"工作流 {name} 是子工作流模板，不能直接启动")
            if not wf.steps:
                raise ValueError(f"工作流 {name} 无步骤")
            run_id = f"wf_{uuid.uuid4().hex[:12]}"
            run = WorkflowRun(
                id=run_id,
                workflow_name=name, context=context or {},
                current_step=wf.steps[0].id,
            )
            self._upsert_template(name, wf)
            try:
                self._lifecycle.start_wf(run.id, current_step_id=wf.steps[0].id,
                                          template_id=name, context=context,
                                          initiator=initiator)
            except Exception as e:
                LOGGER.error("[wf] LM start_wf 失败: %s", e)
            self._write_step_prompt(run, wf.steps[0])
            return run.id

        raise ValueError(f"未知工作流: {name}")

    def _upsert_template(self, name: str, wf: WorkflowDef):
        """把模板定义写入 DB（start() 时调用，供实例推进依赖 DB 模板行）。

        三道防线（2026-08 绕过防护确立）：
        1. 结构校验：无 exit_condition 的简化模板拒绝写 DB（防污染）
        2. JSON 优先：DB 已有同名 → 不覆盖（防简化版覆盖完整 JSON 定义）
        3. DB 查询失败 → 不写（宁缺勿滥，不静默降级）
        统一入口 register() 是唯一"完整校验"写库路径；此处是运行时兜底。
        """
        # start() 后实例推进依赖 DB 模板行（_load_workflows 从 DB 补充加载）。
        # 防绕过：写 DB 前过结构校验（allow_minimal）——未校验的简化模板
        # （无 exit_condition 的步骤）会被拒绝，不能污染 DB。
        # JSON 优先守卫保留：DB 已有同名 → 不覆盖（防简化版覆盖完整定义）。
        try:
            from template_registry import _validate_schema as _vschema
            _steps_dicts = [
                {"id": s.id, "title": s.title, "target_role": s.target_role,
                 "prompt_template": s.prompt_template, "exit_condition": s.exit_condition}
                for s in wf.steps
            ]
            _vr = _vschema({"name": name, "steps": _steps_dicts}, allow_minimal=True)
            if not _vr.passed:
                LOGGER.error("[wf] upsert 拒绝: %s 结构校验不通过: %s",
                             name, _vr.errors[:3])
                return
        except Exception as _e:
            # 校验器自身出错 → 记日志后放行（不阻断启动，但暴露问题）
            # （2026-08-12 codex P2：静默 pass 会掩盖校验器 bug → 未校验模板写 DB）
            LOGGER.warning("[wf] upsert 校验异常（放行但需关注）: %s: %s", name, _e)
        try:
            _existing = self._lifecycle.query(
                "SELECT 1 FROM workflow_templates WHERE template_id=?", (name,))
            if _existing:
                # DB 已有同名 → 不整体覆盖（防简化版覆盖完整 JSON 定义），
                # 但必须补齐 JSON 后新增的步骤：complete_step → get_step 查 DB
                # 模板，缺步骤时抛"步骤定义不存在"被 DEBUG 吞掉 → 引擎能投递
                # 该步(从 JSON)却永远无法完成它 → 步骤永久卡死。
                # 实测 food_chain_loop/s0：JSON 有 8 步，DB 只有 s1-s7，
                # 8 次运行 6 次死于此步。仅追加缺失步骤，已有步骤保持 DB 版本。
                try:
                    _row = self._lifecycle.query(
                        "SELECT steps_json FROM workflow_templates WHERE template_id=?",
                        (name,))
                    _db_steps = json.loads(_row[0][0]) if _row and _row[0][0] else []
                    _have = {(_s.get("step_id") or _s.get("id"))
                             for _s in _db_steps}
                    _missing = [s for s in wf.steps if s.id not in _have]
                    if _missing:
                        LOGGER.warning("[wf] upsert 补齐 %s 缺失步骤: %s",
                                       name, [s.id for s in _missing])
                        _db_steps.extend({
                            "step_id": s.id, "title": s.title, "type": s.type,
                            "target_role": s.target_role,
                            "prompt_template": s.prompt_template,
                            "max_retries": s.max_retries, "verify": s.verify,
                            "subflow_template": s.subflow_template,
                            "dual_node": s.dual_node,
                            "exit_condition": s.exit_condition,
                            "exit_schema": s.exit_schema, "guard": s.guard,
                            "track": s.track,
                            "wait_roles": getattr(s, "wait_roles", None),
                            "failure_patterns": getattr(s, "failure_patterns", None),
                        } for s in _missing)
                        self._lifecycle.execute(
                            "UPDATE workflow_templates SET steps_json=? WHERE template_id=?",
                            (json.dumps(_db_steps, ensure_ascii=False), name))
                except Exception as _me:
                    LOGGER.error("[wf] upsert 补齐缺失步骤失败 %s: %s", name, _me)
                return
        except Exception:
            pass  # DB 查询失败时允许 upsert（兼容首次初始化）
        try:
            steps = [
                {"step_id": s.id, "title": s.title, "type": s.type,
                 "target_role": s.target_role,
                 "prompt_template": s.prompt_template,
                 "max_retries": s.max_retries,
                 "verify": s.verify,
                 "subflow_template": s.subflow_template,
                 "dual_node": s.dual_node,
                 "exit_condition": s.exit_condition,
                 "exit_schema": s.exit_schema,
                 "guard": s.guard,
                 "track": s.track,
                 "wait_roles": getattr(s, "wait_roles", None),
                 "failure_patterns": getattr(s, "failure_patterns", None)}
                for s in wf.steps
            ]
            self._lifecycle.upsert_template(
                name, wf.title, wf.description, steps,
                trigger_scene=wf.trigger_scene,
                allowed_initiators=wf.allowed_initiators,
                allowed_executors=wf.allowed_executors,
                max_duration_hours=wf.max_duration_hours,
                quality_standards=wf.quality_standards,
            )
        except Exception as e:
            LOGGER.error("[wf] upsert template 失败: %s", e)

    def status(self, wid: str) -> dict:
        try:
            rows = self._lifecycle.query(
                "SELECT instance_id, template_id, status, current_step_id, step_results, created_at "
                "FROM workflow_instances WHERE instance_id=?", (wid,)
            )
            row = rows[0] if rows else None
        except (ValueError, KeyError, TypeError, IndexError):
            row = None
        if not row:
            return {"error": "不存在"}
        try:
            d = dict(row)
        except Exception:
            return {"error": "行数据异常"}
        try:
            results = json.loads(d.get("step_results") or "{}")
        except (ValueError, TypeError):
            results = {}
        return {
            "id": d.get("instance_id", wid), "workflow": d.get("template_id", ""),
            "status": d.get("status", "unknown"), "current_step": d.get("current_step_id", ""),
            "retries": {}, "results": results,
        }

    def cancel(self, wid: str) -> bool:
        try:
            rows = self._lifecycle.query(
                "SELECT status, created_at FROM workflow_instances WHERE instance_id=?",
                (wid,),
            )
            row = rows[0] if rows else None
        except (ValueError, KeyError, TypeError, IndexError):
            row = None
        if not row or row["status"] in ("completed", "cancelled", "failed"):
            return False
        # 最小存活时间保护：创建不足 5 分钟的工作流禁止 cancel。
        # 防止"通知角色后 32 秒即被取消"的误杀（bus #158145：s1 响应窗口仅 32s）。
        # ponytail: 5 分钟是保守值，正式角色多数 5 分钟内会响应；超时取消不受此限制（走 _tick 路径）。
        _created = row["created_at"] or 0
        if time.time() - _created < 300:
            LOGGER.warning("cancel rejected: %s 创建不足 5 分钟 (age=%.0fs)，拒绝取消", wid, time.time() - _created)
            return False
        self._lifecycle.close_wf(wid, status="cancelled")
        return True

    def tick(self) -> int:
        self.run_once()
        return 0

    def run_once(self):
        # 每次 tick 重载模板（支持热加载，DB 修改即时生效）
        self._load_workflows()

        # 恢复 delivery_blocked：若目标角色已恢复在线，重置 step_results 允许重试
        # 修复：Round 2 修复后 delivery_blocked 正确停止重刷，但角色恢复后无自动重试机制
        # 现在每 tick 检查 delivery_blocked 步骤，若目标角色已在线 → 重置 step_results 允许重试
        try:
            self._recover_delivery_blocked()
        except Exception as e:
            LOGGER.debug("delivery_blocked recovery failed: %s", e)

        # 先清理僵尸 workflow
        try:
            self._cleanup_stale_workflows(self._lifecycle)
        except Exception as e:
            LOGGER.debug("stale cleanup in run_once failed: %s", e)

        # 确保工作流涉及的角色存活（assignee + target_role + guard.watch）
        self._ensure_guard_roles()
        # SQLite production 工作流 — 复用 _lifecycle 连接避免泄漏
        lm = self._lifecycle
        try:
            lm.ping()
        except (ValueError, KeyError, TypeError):
            LOGGER.debug("lifecycle ping failed during run_once")
        try:
            rows = lm.query(
                # task_id 必选：下方 run context 补齐（integrity 提醒填充任务正文）依赖它
                "SELECT instance_id, template_id, current_step_id, step_results, created_at, "
                "status, task_id "
                "FROM workflow_instances WHERE status IN ('pending','running','step_done_ready')"
            )
            for row in rows:
                inst = dict(row)
                if inst.get("status") == "pending":
                    lm.execute("UPDATE workflow_instances SET status='running' WHERE instance_id=?", (inst["instance_id"],))
                wf_name = inst.get("template_id") or ""
                wf = self._workflows.get(wf_name)
                if not wf:
                    continue
                step_id = inst.get("current_step_id", "")
                step = next((s for s in wf.steps if s.id == step_id), None)
                if not step:
                    # current_step 不在模板中（模板更新遗留/数据损坏）→ 回收
                    LOGGER.warning("wf %s/%s current_step=%s 不在模板中，自动回收",
                                   inst["instance_id"][:12], wf_name, step_id)
                    try:
                        self._lifecycle.close_wf(inst["instance_id"], status="cancelled")
                    except Exception:
                        pass
                    continue
                results = json.loads(inst.get("step_results") or "{}")
                step_status = results.get(step_id, {}).get("status", "")
                # 行 status 与 step_results JSON 之间可能存在分歧：
                # 客户端（workflow/client.py）写行 status 但不写 step_results，
                # 引擎读 step_results 但行 status 才是真相源。
                # 当 step_results 空或 notified 但行 status 更高级时，以行 status 为准。
                if not step_status and inst.get("status") in ("step_done_ready", "completed"):
                    step_status = inst["status"]
                elif step_status == "notified" and inst.get("status") == "step_done_ready":
                    step_status = "step_done_ready"

                # Fix 2: 立即 sync 行 status → step_results JSON，寫回 DB
                # 避免 results 過時導致後續判斷使用 stale 值
                if step_status and step_status != results.get(step_id, {}).get("status"):
                    _existing_sr = results.get(step_id, {}) or {}
                    results[step_id] = {
                        **_existing_sr,
                        "status": step_status,
                    }
                    lm.execute(
                        "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(results, ensure_ascii=False), inst["instance_id"])
                    )

                # 步骤尚未开始（首次检测到）→ 发初始提示词给角色
                if not step_status:
                    task_rows = lm.query(
                        "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                        (inst["instance_id"],)
                    )
                    task = task_rows[0] if task_rows and len(task_rows) > 0 else None
                    task_title = task["title"] if task else wf_name
                    task_desc = task["description"] if task else ""
                    # 任务上下文注入：把 title/desc 放到 prompt 头部，角色立刻知道干什么
                    task_header = (
                        f"[任务] {task_title}\n"
                        f"[工作流] {wf_name} | 步骤: {step.id} — {step.title}\n"
                        f"[工作流目标] {step.title}\n"
                        + (f"[任务描述] {task_desc}\n" if task_desc else "")
                        + "\n"
                    )
                    prompt = task_header + step.prompt_template
                    prompt = prompt.replace("{title}", task_title)
                    prompt = prompt.replace("{description}", task_desc)
                    prompt = prompt.replace("{assignee}", step.target_role)
                    prompt = prompt.replace("{topic}", task_title)
                    # 补充替换：12个JSON模板使用{task_definition}/{acceptance_criteria}
                    prompt = prompt.replace("{task_definition}", task_title or task_desc or wf_name)
                    prompt = prompt.replace("{acceptance_criteria}", task_desc or "按模板要求完成产出并写入对应bus分类")
                    # 首步无上一步产出，用任务标题作变量的语义默认值，避免空变量
                    fallback = task_title or task_desc or step.title or wf_name
                    # focus_area 语义是"聚焦范围"，step.title 比 task_title 更精确
                    prompt = prompt.replace("{focus_area}", step.title or fallback)
                    for var in ("{target}", "{findings}", "{results}", "{backlog}",
                                "{exception_info}", "{project}", "{changes}",
                                "{task_list}", "{assignments}"):
                        prompt = prompt.replace(var, fallback)
                    # 此处原为无守卫的 TASKS.md 写入，已删除：紧随其后的
                    # _send_to_role 会走 role_lifecycle 的**带守卫**写入（角色已有其他
                    # 活跃任务时不覆盖、只追加排队标记）。两处写同一文件，且无守卫者先执行
                    # → 守卫形同虚设。实测 2026-10-16：pytest fixture 的 "test" 任务
                    # 覆盖了 scout/engineer 的真实 TASKS.md（16:22:50/52）。
                    prompt = prompt.replace("{workspace_summary}", self._collect_workspace_summary(Path.home() / "ccs-workspaces" / step.target_role) if hasattr(self,'_collect_workspace_summary') else "")
                    _delivered = self._send_to_role(step.target_role, prompt, wf_id=inst["instance_id"], step_id=step_id)
                    # 仅当真实投递成功才盖戳 notified；投递被并发守卫/变量校验拦截时
                    # 不盖戳，让 step 保持空 sr 状态，引擎下一 tick 会重试而非误判为"已通知"
                    # 进而触发 _cleanup_stale_workflows 的"running 且 sr 空>1h"静默误杀
                    # busy 是**真值字符串**，必须用 == 判三态，不能用 not。
                    # 历史缺陷（4fe4086 引入，2026-10-17 定位）：原写成 `if not _delivered:`
                    # 且 busy 判断嵌在块内——`not "busy"` 恒为 False，该块永不进入，
                    # busy 径直落到下方盖戳分支，把**从未投递**的 step 标记为 notified，
                    # 角色没收到任务却被判"已通知"，只能等超时回收（实测 48 条 cancelled）。
                    # 同仓 task_generator.py:171 / wf_lifecycle.py:491 已用 is not False /
                    # is not True 处理三态，仅此主派发路径漏改。
                    if _delivered == "busy":
                        # 角色正忙（正常排队），非故障：不计失败、不盖戳，下一 tick 重试。
                        # 但**必须写排队账本**——本步 status 留空（投递重试所需），
                        # 而回收器条件4 的判据要求 `timeout_count>=1` 或
                        # `queued>=_QUEUE_DEGRADE_AT 且停滞>1h`，两者都只在 _tick 内累加，
                        # 而 _tick 入口是 `step_status in ("notified","running")` ——
                        # 不写账本则该实例对回收器完全不可见，只能等 24h 全局阈值
                        # （codex 三方审查 P1-1，2026-10-17）。写 queued 而非盖 status：
                        # 盖 notified 会让本分支下轮不再重试投递（投递永久悬空）。
                        _q_sr = results.get(step_id, {}) or {}
                        results[step_id] = {
                            **_q_sr,
                            "queued": (_q_sr.get("queued", 0) or 0) + 1,
                            # queued_since 只在首次排队写入：回收器据它算真实停滞起点，
                            # 随 ts 刷新会让回收判据永远看到"刚排队"
                            "queued_since": _q_sr.get("queued_since") or time.time(),
                        }
                        lm.execute(
                            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                            (json.dumps(results, ensure_ascii=False), inst["instance_id"])
                        )
                        LOGGER.debug("run_once: %s/%s 角色忙，排队 %s 次，跳过计数",
                                     inst["instance_id"][:12], step_id,
                                     results[step_id]["queued"])
                        continue
                    if _delivered is not True:
                        # 幂等护栏：已 notified 的 step 不再重复计数。
                        # 根因：_send_to_role 在 goal 过短等路径返回 None（falsy），
                        # 已成功盖戳 notified 的 step 每 tick 重走本分支 → delivery_fail_count
                        # 从旧值继续涨（观测 wf_sub_06b9e/s1 count=35 且 status=notified），
                        # 且把 status 覆盖回 "" 制造"未通知"假象。已通知则跳过，仅真未通知才计数。
                        _cur_sr = results.get(step_id, {}) or {}
                        if _cur_sr.get("status") == "notified":
                            LOGGER.debug("run_once: %s/%s 已 notified，跳过投递失败计数",
                                         inst["instance_id"][:12], step_id)
                            continue
                        # 记录投递失败次数，防止静默无限重试
                        _fail_count = _cur_sr.get("delivery_fail_count", 0) + 1
                        # 达阈值设终态 delivery_blocked，停止每 tick 重刷 blocker
                        # （旧文案"已停止重试"失真：continue 仅跳本 tick，下 tick 仍重刷）
                        _reached_cap = _fail_count >= 3
                        # status 留空是**投递重试机制本身**：run_once 的投递分支入口是
                        # `if not step_status`，空状态才会每 tick 重试投递；写 "notified"
                        # 会让重试永不发生（任务悬空且无终态）。投递失败的兜底是
                        # _reached_cap 后的 delivery_blocked 终态 + blocker，不是 _tick。
                        results[step_id] = {
                            **_cur_sr,
                            "delivery_fail_count": _fail_count,
                            "status": "delivery_blocked" if _reached_cap else "",
                        }
                        lm.execute(
                            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                            (json.dumps(results, ensure_ascii=False), inst["instance_id"])
                        )
                        if _reached_cap:
                            self._bb.write("blocker",
                                f"[workflow] {inst['instance_id'][:12]}/{step_id} 投递连续失败 {_fail_count} 次，已达上限暂停重试",
                                src="workflow_engine")
                            LOGGER.error("run_once: %s/%s 投递失败达上限，暂停重试",
                                         inst["instance_id"][:12], step_id)
                            # 收敛行级 status 列：否则实例永久计为 running，
                            # has_active_template/cancel 扫描据此永远拦重建、且不接受 cancel
                            # （需 completed/cancelled/failed）。step_results 终态已置 delivery_blocked，
                            # 此处把行级列也收口，消除软泄漏（三方审查 WARN-15）。
                            try:
                                self._lifecycle.close_wf(inst["instance_id"], status="delivery_blocked")
                            except Exception:
                                LOGGER.debug("close_wf delivery_blocked 失败: %s", inst["instance_id"][:12])
                        else:
                            LOGGER.warning("run_once: %s/%s 投递失败 %d/3 次，保持未通知状态待重试",
                                           inst["instance_id"][:12], step_id, _fail_count)
                        continue
                    # 标记已通知（若已有旧状态如 node1_done，必须合并保留——2026-08-10 观测：
                    # 状态机对 status 缺失时重建覆盖，丢失 exit_messages/node1_done）
                    _existing_sr = results.get(step_id, {}) or {}
                    results[step_id] = {
                        # queued 与 queued_since 一起清：二者是"本轮排队"的计价器，
                        # 投递成功即本轮排队结束。若只清 queued_since 而留 queued，
                        # 下一轮排队会带着旧计数（如 3+1=4）立即满足回收判据 queued>=3，
                        # 使本该"重新排 3 次"的宽限被跳过（2026-10-17 推演确认）。
                        **{k: v for k, v in _existing_sr.items()
                           if k not in ('status', 'queued_since', 'queued')},
                        "status": "notified", "ts": time.time(), "notified_at": time.time()
                    }
                    lm.execute(
                        "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(results, ensure_ascii=False), inst["instance_id"])
                    )
                    step_status = "notified"  # 更新状态，让后续 exit_condition/超时检查能执行

                # 步骤已完成 → 自动推进到下一步
                # step_done_ready/completed 由角色或外部系统标记，无需额外的 completion_check
                if step_status in ("step_done_ready", "completed"):
                    # 用户要求"每个步骤"都提醒 → step_done_ready 路径也补发完整性提醒
                    try:
                        from pipeflow.integrity import send_bypass_reminder as _sbr
                        _sbr_run = SimpleNamespace(id=inst["instance_id"], step_results=results)
                        _sbr_step = SimpleNamespace(
                            id=step_id, target_role=step.target_role,
                            prompt_template=step.prompt_template
                        )
                        def _persist_step_done(*args):
                            """步骤完成回调：将 bypass 提醒状态持久化到 DB（daemon 重启后不重复）。"""
                            # bypass flag 真落库（daemon 重启后不重复提醒）
                            try:
                                lm.execute(
                                    "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                                    (json.dumps(results, ensure_ascii=False), inst["instance_id"])
                                )
                            except Exception:
                                LOGGER.debug("step_done_ready persist sr failed for %s", inst["instance_id"])
                        _sbr(_sbr_run, _sbr_step, "step_done_ready",
                             send_to_role=self._send_to_role,
                             sync_step_results=_persist_step_done)
                    except Exception:
                        LOGGER.debug("step_done_ready integrity send failed for %s", inst["instance_id"])
                    self._advance_production_wf(inst["instance_id"], lm, wf_name, step_id, results)
                    # 推进后立即把行 status 改为 running（推进到非最后一步时），
                    # 避免角色重复 wf complete 把行 status 打回 step_done_ready
                    lm.execute("UPDATE workflow_instances SET status='running' "
                               "WHERE instance_id=? AND status='step_done_ready'",
                               (inst["instance_id"],))
                    continue

                # 所有 notified/running 步骤都进入 _tick() 处理超时/催办/升级
                # exit_condition 可选：有 bus_category 时检查 bus 消息匹配退出条件
                # 无 bus_category 时退化为纯超时驱动
                if step_status in ("notified", "running"):
                    _created = inst.get("created_at") or time.time()
                    # context 从 tasks 表补齐：实例行的 context 列恒为 {}（实测），
                    # 真实任务上下文在 tasks.title/description。空 context 会让
                    # integrity 提醒的"用户指令：{description}"填充落空 → 被
                    # _UNMATCHED_VAR_RE 拦下 → 用户要求的"重复提醒"机制整体失效
                    # （实测生产 blocker 2026-10-17 06:42 仍在报 {description} 未替换）。
                    #
                    # description 为空时必须回退 **title**：实测 675 个 task 中 616 个
                    # description 为空（91%）——只补 description 会让 91% 的提醒仍带
                    # 未替换变量、继续被守卫拦下（2026-10-17 07:10 实测复发）。title
                    # 由调用方必填（create_task_v2 的 title 参数），是唯一可靠兜底。
                    #
                    # task 行缺失时（task_id 悬空：实测有 1 个实例如此）退回 **step.title**：
                    # 它是模板里的步骤名，恒非空。不退回则 context 全空 → {description}
                    # 泄漏 → 提醒被守卫拦下（终审标注的边界，生产可达但罕见）。
                    _run_ctx: dict = {}
                    try:
                        _trow = lm.query(
                            "SELECT title, description FROM tasks WHERE task_id=?",
                            (inst.get("task_id"),))
                        if _trow:
                            _title = _trow[0]["title"] or ""
                            _desc = _trow[0]["description"] or ""
                            _fallback = _desc or _title
                            _run_ctx = {"title": _title, "description": _fallback,
                                        "task_definition": _fallback}
                            _run_ctx["topic"] = _title or _fallback
                    except Exception as _ctx_e:
                        LOGGER.debug("run context 补齐失败(降级为空): %s", _ctx_e)
                    if not _run_ctx.get("description"):
                        # step 可能为 None（上面 next() 未命中）→ getattr 兜 "" → wf_name
                        _fb = (getattr(step, "title", "") if step else "") or wf_name
                        _run_ctx = {"title": _fb, "description": _fb,
                                    "task_definition": _fb, "topic": _fb}
                    run = WorkflowRun(
                        id=inst["instance_id"], workflow_name=wf_name,
                        context=_run_ctx, current_step=step_id,
                        status="running", step_results=results,
                        created_at=_created, updated_at=_created,
                    )
                    self._tick(run, step)
        except Exception as _sqle:
            import logging as _lg
            import traceback as _tb
            _lg.getLogger("engine.sqlite_block").warning("生产工作流扫描异常: %s", _sqle)
            _tb.print_exc()


        self._scan_tasks()
        LOGGER.debug("run_once: _scan_tasks done, querying workflow status...")

        # 报告当前工作流概况
        try:
            lm = self._lifecycle
            # 总数摘要 — 始终打印，含零
            summary = lm.query(
                "SELECT status, COUNT(*) as cnt FROM workflow_instances GROUP BY status"
            )
            total = sum(r["cnt"] for r in summary)
            parts = [f"{r['status']}={r['cnt']}" for r in summary]
            LOGGER.info("workflow 概况: %d 个 — %s", total, ", ".join(parts))
            # 逐条列出活跃 workflow 进度
            runs = lm.query(
                "SELECT instance_id, template_id, current_step_id, assignee, created_at "
                "FROM workflow_instances WHERE status IN ('running','pending','step_done_ready')"
            )
            for wf in runs:
                LOGGER.info("wf %s [%s] → %s step=%s (创建于 %s)",
                            wf["instance_id"][:8], wf["template_id"][:30],
                            wf["assignee"] or "?", wf["current_step_id"],
                            str(wf["created_at"])[:16] if wf["created_at"] else "?")
        except Exception as e:
            LOGGER.warning("workflow 概况查询异常: %s", e, exc_info=True)

        # 角色停滞保护：检测有积压 task 但无 progress 的角色
        try:
            self._kick_stalled_roles()
        except Exception as _e:
            LOGGER.debug("kick_stalled_roles failed: %s", _e)

        # 上下文溢出检测 → 自动发 /compact
        try:
            self._check_context_overflow()
        except Exception:
            LOGGER.debug("context overflow check failed")

    def _collect_guard_roles(self) -> set[str]:
        """收集所有活跃工作流步骤需要守护的角色。

        来源: assignee + target_role + guard.watch + wait_roles。
        wait_roles 也收集（2026-08-11 codex P0）——协作门角色不存活 →
        门永远不通过 → 工作流死锁；拉起后角色可产出消息开门。

        过滤掉内部标识符（如 'workflow_engine'），仅保留有效的 CCS 角色名。
        """
        _alive_roles: set[str] = set()
        _INTERNAL_ROLES = {"workflow_engine"}  # 内部标识，非 CCS 角色
        _ROLE_NAME_RE = re.compile(r'^[a-zA-Z0-9_-]+$')
        try:
            for _wf_row in self._lifecycle.query("SELECT DISTINCT assignee FROM workflow_instances WHERE status IN ('running','pending')"):
                if _wf_row["assignee"] and _wf_row["assignee"] not in _INTERNAL_ROLES:
                    _alive_roles.add(_wf_row["assignee"])
            for _wf_row in self._lifecycle.query("SELECT instance_id, template_id, current_step_id FROM workflow_instances WHERE status IN ('running','pending')"):
                _wf = self._workflows.get(_wf_row["template_id"])
                if not _wf:
                    continue
                _step = next((s for s in _wf.steps if s.id == _wf_row["current_step_id"]), None)
                if _step:
                    _tr = _step.target_role
                    if _tr and _tr not in _INTERNAL_ROLES and _ROLE_NAME_RE.match(_tr):
                        _alive_roles.add(_tr)
                    if _step.guard:
                        for _g_role in _step.guard.get("watch", []):
                            if _step.guard.get("auto_restart", True) and _g_role not in _INTERNAL_ROLES and _ROLE_NAME_RE.match(_g_role):
                                _alive_roles.add(_g_role)
                    # 协作门角色也要守护：wait_roles 角色不产出 → 门永远不通过
                    # ponytail: 仅守护 running/pending 的 workflow；step_done_ready（等待审批）
                    # 不拉起 —— 审批阶段角色状态不归引擎管，拉起反而干扰人工审批流程
                    if _step.wait_roles:
                        for _wr in _step.wait_roles:
                            if _wr not in _INTERNAL_ROLES and _ROLE_NAME_RE.match(_wr):
                                _alive_roles.add(_wr)
        except Exception as e:
            LOGGER.debug("guard 收集异常（非致命，返回已收集的角色）: %s", e)
        return _alive_roles

    def _ensure_guard_roles(self) -> set[str]:
        """收集并拉起所有活跃工作流的守护角色。"""
        _alive_roles = self._collect_guard_roles()
        for _role in _alive_roles:
            # 单角色异常不得中断整轮：本函数在 run_once 里无 try 保护，
            # 一处抛错则同 tick 的其它角色全部不被拉起。
            try:
                self._ensure_role_alive(_role)
            except Exception as _e:
                LOGGER.warning("ensure_role_alive %s 失败: %s", _role, _e)
        return _alive_roles


    def _recover_delivery_blocked(self) -> None:
        """恢复 delivery_blocked：若目标角色已恢复在线，重置 step_results 允许重试。

        修复：Round 2 修复后 delivery_blocked 正确停止重刷 blocker，但角色恢复后无自动重试机制。
        现在每 tick 检查 delivery_blocked 步骤，若目标角色已在线 → 重置 step_results 允许重试。
        """
        try:
            lm = self._lifecycle
            # 以行级 status 为准选取候选，不用 step_results LIKE：
            # 旧版恢复会先把 step 内 status 抹成 ""，标记随之消失 → LIKE 第二次
            # 必失配（自致盲）。实测 15 条历史实例因此永久搁浅（8-27 的恢复日志
            # 成功但行状态未变，10 秒后概况仍 delivery_blocked=12）。
            # 行级 status='delivery_blocked' 是 close_wf 打的，恢复不改它，稳定可查。
            rows = lm.query(
                "SELECT instance_id, step_results, current_step_id FROM workflow_instances "
                "WHERE status = 'delivery_blocked'"
            )
            for row in rows:
                inst_id = row["instance_id"]
                sr = json.loads(row["step_results"] or "{}")
                # 只检查当前步：历史步的遗留标记不应触发恢复
                _cur = row["current_step_id"]
                # isinstance 守卫：生产 DB 实测 408 行 step_results 值是非法形状
                # （"$.s1.timeout_count" 这种 JSONPath 键 / str / int），非 dict 直接
                # v.get 抛 AttributeError，被外层 except 吞掉 → 本函数当轮完全跳过
                # （不是跳过该行，是整批 delivery_blocked 实例都得不到恢复），
                # 连带让 recover_count>=3 → failed 的护栏失效。同文件 1009 与
                # wf_lifecycle.py:101、health_check.py:39 都已有此守卫。
                _cand = {_cur: sr[_cur]} if _cur in sr else {
                    k: v for k, v in sr.items()
                    if isinstance(v, dict) and v.get("status") == "delivery_blocked"}
                for step_id, step_sr in _cand.items():
                    # 兼容两种签名：未被恢复过（status=delivery_blocked）与
                    # 已被旧版恢复重置过（status='' 且 fail_count=0）
                    _is_blocked_sig = (
                        step_sr.get("status") == "delivery_blocked"
                        or (step_sr.get("status") == ""
                            and step_sr.get("delivery_fail_count") == 0)
                    )
                    if not _is_blocked_sig:
                        continue
                    # 获取目标角色
                    wf_row = lm.query(
                        "SELECT template_id, current_step_id FROM workflow_instances WHERE instance_id=?",
                        (inst_id,)
                    )
                    if not wf_row:
                        continue
                    wf_name = wf_row[0]["template_id"]
                    cur_step_id = wf_row[0]["current_step_id"]
                    if cur_step_id != step_id:
                        continue
                    # 获取目标角色
                    wf = self._workflows.get(wf_name)
                    if not wf:
                        continue
                    step = next((s for s in wf.steps if s.id == step_id), None)
                    if not step:
                        continue
                    target_role = step.target_role
                    # 恢复次数护栏：同一实例反复 恢复→再blocked 会成死循环
                    # （目标角色被主链/他链长期占用时，恢复后投递仍失败，3 次又 blocked）。
                    # 上限 3 次：够覆盖"角色短暂离线后恢复"，又不会无限重刷 blocker。
                    _rc = step_sr.get("recover_count", 0)
                    if _rc >= 3:
                        # 达上限 → 收敛终态。留 delivery_blocked 会让实例既不推进
                        # 也不被回收（run_once 只查 pending/running/step_done_ready，
                        # _cleanup_stale_workflows 只查 running/step_done_ready）
                        # → 永久孤儿（实测 wf_sub_ee176a2b55a1 停在 s1）。
                        # 记 blocker 供人介入，而非静默留下。
                        LOGGER.warning("recover 上限已满，收敛为 failed: %s/%s (%d 次)",
                                       inst_id[:12], step_id, _rc)
                        lm.execute(
                            "UPDATE workflow_instances SET status='failed' "
                            "WHERE instance_id=? AND status='delivery_blocked'",
                            (inst_id,))
                        try:
                            self._bb.write(
                                "blocker",
                                f"工作流投递恢复耗尽: {inst_id[:12]}/{step_id}",
                                evidence=(f"角色 {target_role} 连续 {_rc} 次恢复后投递仍失败，"
                                          f"已收敛为 failed，需人工介入（模板 {wf_name}）"),
                                src="workflow_engine")
                        except Exception as _be:
                            LOGGER.warning("recover 耗尽写 blocker 失败: %s", _be)
                        continue
                    # 检查目标角色是否已在线
                    if self._ensure_role_alive(target_role):
                        # 角色已在线 → 重置 step_results 允许重试
                        LOGGER.info("recover: %s/%s delivery_blocked → 角色 %s 已在线，重置重试 (第 %d 次)",
                                    inst_id[:12], step_id, target_role, _rc + 1)
                        new_sr = dict(sr)
                        new_sr[step_id] = {
                            **sr[step_id],
                            "status": "",
                            "delivery_fail_count": 0,
                            "recover_count": _rc + 1,
                        }
                        # 必须同时把行级 status 还原为 running：
                        # close_wf 打终态时置 delivery_blocked（行级），而主处理循环
                        # 只查 status IN ('pending','running','step_done_ready')
                        # （engine.py:648）→ 仅重置 step_results 的话实例对主循环
                        # 永不可见，重置白做。
                        # 另：本函数的扫描条件依赖 step_results LIKE '%delivery_blocked%'
                        # （:944），而上面已把 step 内 status 改为 ""，标记随之消失 →
                        # 第二次扫描必失配（自致盲，只能生效一次）。
                        # 实测 2026-08-27 日志：5 条被恢复且日志成功，10 秒后概况仍是
                        # delivery_blocked=12，行状态纹丝未动；8-27 后不再命中。
                        lm.execute(
                            "UPDATE workflow_instances SET step_results=?, status='running' "
                            "WHERE instance_id=?",
                            (json.dumps(new_sr, ensure_ascii=False), inst_id)
                        )
        except Exception as e:
            LOGGER.debug("delivery_blocked recovery scan failed: %s", e)

def main():
    import argparse, sys, os, signal
    p = argparse.ArgumentParser(description="Workflow Engine")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("start")
    s.add_argument("name")
    s.add_argument("--context", default="{}")

    sub.add_parser("list")

    s2 = sub.add_parser("status")
    s2.add_argument("workflow_id")

    s3 = sub.add_parser("cancel")
    s3.add_argument("workflow_id")

    sub.add_parser("tick")

    daemon = sub.add_parser("daemon")
    daemon.add_argument("--interval", type=int, default=10, help="轮询间隔秒数")

    args = p.parse_args()
    eng = WorkflowEngine()

    _PID_FILE = Path.home() / ".hermes" / "run" / "workflow-engine-daemon.pid"

    def _daemon_loop(interval: int):
        """守护进程主循环：PID 锁单实例，驱动 run_once 推进所有工作流。"""
        if _PID_FILE.exists():
            try:
                old = int(_PID_FILE.read_text())
                os.kill(old, 0)
                print(f"Daemon PID={old} 已在运行")
                sys.exit(1)
            except (OSError, ValueError):
                LOGGER.debug("PID file stale or missing")
        _PID_FILE.write_text(str(os.getpid()))
        print(f"Workflow Engine daemon PID={os.getpid()}, interval={interval}s")

        shutdown = False
        def _handler(s, f):
            """SIGTERM/SIGINT 信号处理：置 shutdown 标志让主循环优雅退出。"""
            nonlocal shutdown; shutdown = True
        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

        try:
            while not shutdown:
                eng.run_once()
                time.sleep(interval)
        finally:
            _PID_FILE.unlink(missing_ok=True)
            print("Daemon stopped")

    if args.cmd == "list":
        for wf in eng.list_workflows():
            print(wf)
    elif args.cmd == "start":
        ctx = json.loads(args.context)
        print(eng.start(args.name, ctx))
    elif args.cmd == "status":
        print(json.dumps(eng.status(args.workflow_id), ensure_ascii=False))
    elif args.cmd == "cancel":
        ok = eng.cancel(args.workflow_id)
        print("OK" if ok else "NOT FOUND")
    elif args.cmd == "tick":
        eng.run_once()
        print("Tick done")
    elif args.cmd == "daemon":
        _daemon_loop(args.interval)


if __name__ == "__main__":
    main()
