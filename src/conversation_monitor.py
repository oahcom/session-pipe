"""
Conversation Monitor — 对话增量监控模块

从 CCS 角色的 tmux pane 中增量读取对话内容，提取三个信号：
  1. 步骤完成（step_completed）
  2. 角色空闲（idle_complaint）
  3. 越界操作（violation）——基于实际执行的 shell 命令检测

所有信号都是 bus-based exit_condition 的补充，不替代现有逻辑。
"""
from pathlib import Path

import hashlib
import logging
import re
import subprocess
import threading
from dataclasses import dataclass, field
# ponytail: import Optional could be added if signature type hints are needed later

LOGGER = logging.getLogger("conversation_monitor")

# ── 角色禁区：运行时从 session-launcher 加载，缓存到文件避免重复 import ──
_FORBIDDEN_OPS: dict[str, list[str]] = {}
_DEFAULT_FORBIDDEN = ["run_tests", "edit_config", "deploy",
                      "start_ccs", "edit_persona_json"]

# 缓存文件路径
_FORBIDDEN_CACHE = Path.home() / ".hermes" / "run" / "forbidden_ops_cache.json"
_forbidden_ops_loaded = False
_forbidden_ops_lock = threading.Lock()

def _load_forbidden_ops_from_launcher() -> dict:
    """从 session-launcher 的 routing.roles 拉取禁区定义（仅用于首次生成缓存）"""
    try:
        from pathlib import Path
        import importlib.util
        from paths import SESSION_LAUNCHER_SRC
        launcher_src = str(SESSION_LAUNCHER_SRC)
        spec = importlib.util.spec_from_file_location(
            "launcher_roles", str(Path(launcher_src) / "routing" / "roles.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return dict(mod._FORBIDDEN_MAP)
    except Exception as e:
        LOGGER.warning("failed to load forbidden_ops from roles.py: %s", e)
        return {}

def _launcher_roles_mtime() -> float:
    """launcher 的 roles.py mtime（禁区定义的权威源）；取不到则返回 0。

    返回 0 会让「缓存是否新鲜」判为 True（0 <= 缓存 mtime），即维持旧行为 ——
    宁可继续用缓存，也不因跨项目路径不可用而每 tick 反复加载。
    """
    try:
        from paths import SESSION_LAUNCHER_SRC
        return (Path(SESSION_LAUNCHER_SRC) / "routing" / "roles.py").stat().st_mtime
    except Exception:
        return 0.0


def _ensure_forbidden_ops():
    global _forbidden_ops_loaded, _FORBIDDEN_OPS
    if _forbidden_ops_loaded:
        return
    with _forbidden_ops_lock:
        if _forbidden_ops_loaded:
            return
        # 优先读缓存文件——但源码更新后必须失效重算。
        # 原实现只要缓存存在就用，改了 launcher 的 _FORBIDDEN_MAP 也不生效：
        # 实测 2026-10-20 修正 9 个角色禁区后，reviewer 仍被判 run_tests 越界。
        # 缓存版本号同时参与判断：语义变更但 mtime 未变（如 git checkout）也能失效。
        _CACHE_VERSION = 2
        if _FORBIDDEN_CACHE.exists():
            try:
                import json
                cache_data = json.loads(_FORBIDDEN_CACHE.read_text())
                _src_fresh = _launcher_roles_mtime() <= _FORBIDDEN_CACHE.stat().st_mtime
                if cache_data.get("version") == _CACHE_VERSION and _src_fresh:
                    _FORBIDDEN_OPS = cache_data.get("forbidden_ops", {})
                    _forbidden_ops_loaded = True
                    LOGGER.debug("forbidden_ops loaded from cache (%d roles)", len(_FORBIDDEN_OPS))
                    return
                LOGGER.info("forbidden_ops 缓存过期（源码已更新或版本不符），重新生成")
            except Exception:
                pass  # 缓存损坏，回退到重新生成
        # 无缓存或损坏 → 从 launcher 读取并写缓存
        ops = _load_forbidden_ops_from_launcher()
        _FORBIDDEN_OPS = ops
        _forbidden_ops_loaded = True
        # 原子写缓存
        try:
            _FORBIDDEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
            import json as _json
            tmp = _FORBIDDEN_CACHE.with_suffix(".tmp")
            tmp.write_text(_json.dumps({"forbidden_ops": ops, "version": 1}))
            tmp.replace(_FORBIDDEN_CACHE)
        except Exception:
            pass


@dataclass
class ConversationSignal:
    """结构化对话信号"""
    step_completed: bool = False
    completion_evidence: str = ""
    idle_complaint: bool = False
    idle_count: int = 0          # 连续 N 轮 idle_complaint — 用于判断是否该回收僵尸
    idle_evidence: str = ""
    violation_found: bool = False
    violation_commands: list[str] = field(default_factory=list)
    violation_role: str = ""
    content_void: bool = False
    void_evidence: str = ""

# ── Shell 命令特征 → 操作类型 ──
_SHELL_SIGNATURES: dict[str, list[str]] = {
    "deploy": [
        r"\bkubectl (?:apply|create|delete|replace|patch|rollout|scale|autoscale|drain|cord)\b",
        r"\bhelm (?:install|upgrade|delete|rollback)\b",
        r"\bgit push\b", r"\bgit merge\b",
        r"\bdocker compose up\b", r"\bdocker-compose up\b",
        r"\bterraform apply\b", r"\bgh pr merge\b",
        r"\bsystemctl restart\b", r"\brsync .*prod\b",
        r"\bccs\.py start\b",
    ],
    "run_tests": [
        r"\bpytest\b", r"\bunittest\b", r"\bgo test\b",
        r"\bcargo test\b", r"\bnpm test\b", r"\bnpx jest\b",
        r"\bmvn test\b", r"\bgradle test\b",
    ],
    "write_code": [
        r"\bgit add\b.*\.py", r"\bsed -i\b.*\.py",
        r"\becho .*>.*\.py\b", r"\bcat .*>.*\.py\b",
    ],
    "edit_config": [
        # 只认**写配置类**子命令（enable/disable/mask/edit… 改的是 unit 配置与符号链接）。
        # WHY: 旧式 `\bsystemctl\b(?! restart)` 把只读查询与生命周期动作也当改配置——实测
        # engineer 排障 `systemctl status user@1000`、maintainer 恢复 `systemctl start
        # user@1000` 均被误报「越界: edit_config」，引擎反复注入停止指令卡住 s1（bus #397969/
        # #398702）。status/is-active/cat/show 是读；start/stop/restart 是生命周期（restart 归 deploy）。
        # 允许动词前有可选 flags（--user/-q/--now…）：`systemctl --user disable x`
        # 是本生态主导写法（scripts/ 内 80 处），旧式 \s+ 要求动词紧跟 systemctl
        # → flag 前缀写配置全部漏判（reviewer P1-1，dev_implement/s2 审查）。
        r"\bsystemctl\s+(?:--?[\w-]+\s+)*(?:enable|disable|mask|unmask|edit|set-property|preset|"
        r"link|revert|reenable)\b",
        r"\bsed -i\b.*\.(json|yaml|yml|conf|toml|ini)",
        r"\b(?:vim|vi)\b .*\.(json|yaml|yml|conf|env|toml|ini)",
        r"\bnano .*\.(json|yaml|yml|conf|env)",
    ],
    "edit_persona_json": [r"persona_.*\.json"],
    # 角色豁免必须按**调用角色**动态注入：静态写死 (?!engineer) 只免了 engineer，
    # 而 write_other_workspace 实际只配给 pg（session-launcher/src/routing/roles.py:54）
    # —— pg 读写自己的 ccs-workspaces/pg/ 每次都命中 → 判越界 → 注入撤销指令耗掉
    # 一轮（实测 5 角色收到误报）。{_self} 在 check_violations 里替换为角色名。
    "write_other_workspace": [r"/home/[^/]+/ccs-workspaces/(?!{_self})[^/]+/"],
}

# ── 信号 1：步骤完成 ──
# 旧第一项 `(s\d+ .*完成)` 是**裸子串**匹配，实测会把下列句子全判成完成：
#   「s2 还没完成」「s1 完成了吗？」「我在检查 s4 完成的证据」
# 后果是 tmux 信号在投递后几秒就推进步骤（wf_7045df494d/s1：98 秒、
# exit_messages 为空）。现在：s 与完成词之间允许说明文字（「s1 理解需求已完成」），
# 但不得跨句（。，,\n）也不得出现否定/将来词；完成词后不得跟疑问/的/前/后。
_COMPLETION_PATTERNS = [
    r"(s\d+(?![^。，,\n]{0,20}(?:还没|未|待|将|稍后))[^。，,\n]{0,20}?(?:已|已经)?\s*(?:完成|闭环)"
    r"(?!\s*(?:了?吗|？|\?|的|前|后)))",
    r"(step_done_ready)",
    r"(确认.*完成|已.*确认)",
    r"(产出完整|产出齐全)",
    r"( Goal achieved)",
]

# ── 信号 2：空闲 ──
_IDLE_PATTERNS = [
    r"(Same generic template|No change)",
    r"(不再.*空模板.*输出)",
    r"(没有.*真实 task)",
    r"(无.*任务.*待办)",
    r"(休眠|idle|Idle)",
]
# 排除（有意义的等待 / 外部阻塞 → 不是角色空闲）。
# API 限流类：9router 免费额度耗尽后角色 pane 只剩 429/403 错误文案，
# 无新内容 → 旧逻辑判 idle → 连续 5 次触发 cancel_step，把「等配额恢复」
# 误判成「角色卡死」而掐断步骤（实测 29 个步骤被 tmux 信号取消，
# 其中 WL-dev_with_collab 15 台、maintainer_health_monitor 8 台）。
# 角色仍在消耗算力（pane 有 ↓Nk tokens 计数器）也一并排除。
_EXCLUDE_IDLE = [
    r"waiting for",
    r"等待.*回复|等待.*确认|等待.*修复|waiting.*coordinator",
    r"Rate limit exceeded|rate.?limit",
    r"API Error: 4\d\d",
    r"Please run /login",
    r"↓\s*\d+(\.\d+)?k tokens",
]

# ── 内容 hash 去重（pane 无变化时跳过解析）──
_last_hash: dict[str, str] = {}
_last_hash_lock = threading.Lock()

def capture_role_pane(role: str, lines: int = 80, timeout: float = 5.0) -> str:
    """读取角色对话 pane 的最后 N 行。

    返回值：
      - 非空字符串：有新内容（内容 hash 与上次不同）
      - 空字符串：pane 不存在、读取失败、或无新内容
    """
    try:
        r = subprocess.run(
            ["tmux", "capture-pane", "-t", f"ccs-{role}", "-p", "-S", f"-{lines}"],
            capture_output=True, text=True, timeout=timeout,
        )
        if r.returncode != 0:
            # WARNING 而非 DEBUG：读失败与"无新内容"共用空串返回，
            # DEBUG 级下运维无法区分"角色安静"与"tmux 读取故障"——
            # 后者会让所有 pane 驱动的存活判定静默失效。
            LOGGER.warning("pane capture failed: role=%s returncode=%s", role, r.returncode)
            return ""
        pane = r.stdout
    except Exception as e:
        LOGGER.warning("pane capture exception: role=%s error=%s", role, e)
        return ""

    if not pane:
        return ""

    # 内容 hash 去重（线程安全）
    h = hashlib.md5(pane.encode()).hexdigest()
    with _last_hash_lock:
        if _last_hash.get(role) == h:
            LOGGER.debug("pane unchanged: role=%s", role)
            return ""
        _last_hash[role] = h
    return pane


def parse_conversation_signals(pane_text: str, role: str) -> ConversationSignal:
    """从 pane 文本中解析三个信号"""
    _ensure_forbidden_ops()
    sig = ConversationSignal()
    if not pane_text:
        return sig

    # ── 信号 1：步骤完成 ──
    for pat in _COMPLETION_PATTERNS:
        m = re.search(pat, pane_text)
        if m:
            sig.step_completed = True
            sig.completion_evidence = m.group(0)[:150]
            break

    # ── 信号 2：空闲（排除有意义等待）──
    idle_match = None
    for pat in _IDLE_PATTERNS:
        m = re.search(pat, pane_text)
        if m:
            idle_match = m
            break

    if idle_match:
        excluded = any(re.search(p, pane_text) for p in _EXCLUDE_IDLE)
        if not excluded:
            sig.idle_complaint = True
            sig.idle_evidence = idle_match.group(0)[:150]

    # ── 信号 3：越界操作 ──
    # 只扫**已执行命令的回显行**，不扫整段 pane 文本。
    # WHY: pane 里大部分文字是角色的叙述（调研报告、分析结论、引用验证命令），
    # 对全文跑正则会把人话当命令。实测（2026-10-16）5 个样本里 3 个纯文本讨论
    # 全被误判——scout 写"验证命令: systemctl is-active …"、正文讨论 pytest
    # 覆盖率、读含 systemctl 字样的日志，都被判越界。
    # 放大效应：误报 → 引擎反复注入停止指令/cancel_step → 步骤无法推进 →
    # 2.4h 后当僵尸回收。wf_6ff71a38016c 的 s1 累计 violation_count=26，
    # 而其 transcript 里 2911 条 Bash 命令全是只读（cat/ls/sqlite3 SELECT），
    # 真实的越界数为 0。
    # 回显格式（Claude Code 渲染）：`  ⎿  $ <cmd>` 或 `  ⎿  ⏵ $ <cmd>`。
    # 只认带 `$` 的 ⎿ 行；角色正文里的裸命令名不再命中。
    forbidden_ops = _FORBIDDEN_OPS.get(role, _DEFAULT_FORBIDDEN)
    found = []
    for line in pane_text.splitlines():
        if "$" not in line:
            continue
        # 仅命令回显行：⎿ 前缀（含可选 ⏵/⏵⏵ 标记）
        if "⎿" not in line:
            continue
        _cmd = line.split("$", 1)[1]
        for op_type in forbidden_ops:
            for pat in _SHELL_SIGNATURES.get(op_type, []):
                # {_self} 按调用角色展开，使 workspace 禁区豁免"本角色自己"；
                # re.escape 防止角色名含正则元字符时改变语义。
                _pat = pat.replace("{_self}", re.escape(role))
                for m in re.findall(_pat, _cmd, re.IGNORECASE):
                    found.append(f"{op_type}: {m}")
    if found:
        sig.violation_found = True
        sig.violation_commands = found[:5]
        sig.violation_role = role

    # ── 信号 4：内容空转（噪音循环）──
    # 角色 pane 里只有治理/调度 ack 信号、无任何产出性动作证据时，判 content_void。
    # 这是"用内容判断替代流程指标"的关键：pane 有输出变更 ≠ 角色在高质量产出。
    _PRODUCTION_EVIDENCE = [
        r"Goal achieved", r"产出完整|产出齐全", r"step_done_ready",
        r"写[^了]*文件|创建.*\.py|创建.*\.md|写入.*文件",
        r"\bpytest\b.*passed|\bpy_compile\b.*PASS",
        r"commit|git push|git add",
        r"修复|实现|重构|优化|设计|分析",
    ]
    _NOISE_SIGNS = [
        r"调度脉冲", r"超时.*重推.*升级", r"\[notice\]", r"\[blocker\]",
        r"Interrupted", r"monitor_audit", r"test_exit_schema",
        r"无.*待办|保持.*待机|无待办", r"snap 已更新|轮次完成",
    ]
    _has_production = any(re.search(p, pane_text) for p in _PRODUCTION_EVIDENCE)
    _noise_count = sum(1 for p in _NOISE_SIGNS if re.search(p, pane_text))
    # 有治理噪音特征且没有任何产出性证据 → 空转（刷治理噪音而非真实产出）
    if _noise_count >= 2 and not _has_production:
        sig.content_void = True
        sig.void_evidence = f"noise_signs={_noise_count}, no_production_evidence"

    return sig


def get_conversation_signal(role: str) -> ConversationSignal:
    """一站式接口：capture → parse → 返回信号"""
    pane = capture_role_pane(role)
    return parse_conversation_signals(pane, role)


# ── 信号 → 治理提案（闭合"对话驱动问题察觉→治理动作"环的第一步）──
# ponytail: 当前仅"只读提案"写 bus architecture 待审查，不自动执行；
# 安全边界：自动执行先限 ccs.py stop 僵尸回收，角色生长仍人工 merge。
# 后续由 routing_daemon 长驻轮询调用 scan_and_propose() 驱动端到端闭环。
_BUS_CLIENT = Path.home() / ".hermes" / "scripts" / "bus_client.py"


def propose_governance_action(role: str) -> dict:
    """从角色对话信号提取 idle/violation，写 bus architecture 治理提案（只读，不执行）。

    返回 {idle: bool, violation: bool, proposed: bool}。
    任何异常静默降级（写 bus 失败不阻塞监控）。
    """
    try:
        sig = get_conversation_signal(role)
    except Exception as e:
        LOGGER.debug("信号提取失败 role=%s: %s", role, e)
        return {"idle": False, "violation": False, "proposed": False}

    actions = []
    if sig.idle_complaint:
        actions.append(f"idle 空转: {sig.idle_evidence}")
    if sig.violation_found:
        actions.append(f"violation 越界: {'; '.join(sig.violation_commands[:3])}")
    if sig.content_void:
        actions.append(f"content_void: {sig.void_evidence}")

    if not actions:
        return {"idle": sig.idle_complaint, "violation": sig.violation_found,
                "content_void": sig.content_void, "proposed": False}

    title = f"[治理提案] {role}: " + "; ".join(actions)[:80]
    evidence = (f"role={role} idle={sig.idle_complaint} "
                f"violation={sig.violation_found} content_void={sig.content_void} "
                f"void_evidence={sig.void_evidence}")
    try:
        subprocess.run(
            [str(_BUS_CLIENT), "write", "architecture", title,
             "--evidence", evidence, "--src", "conversation_monitor"],
            capture_output=True, timeout=5)
        LOGGER.info("治理提案已写 bus: %s", title)
    except (OSError, subprocess.TimeoutExpired):
        pass  # 写 bus 失败不阻塞
    return {"idle": sig.idle_complaint, "violation": sig.violation_found,
            "content_void": sig.content_void, "proposed": True}


# 非生产测试/垃圾 persona 過濾（CCS 审计 #324584 發現，避免 G3 對不存在角色產出無效提案）
_NON_PRODUCTION_ROLES = frozenset({
    "dead_role", "e2e_test", "t", "t2", "test_role", "workflow_engine",
    "test", "test2",
})


def scan_and_propose(roles: list[str] | None = None) -> list[dict]:
    """对所有（或指定）角色扫描对话信号并写治理提案。供 routing_daemon 轮询调用。

    返回提案列表，每个 {role, idle, violation, content_void, proposed, _action}。
    _action ∈ {"idle","violation","content_void"} 供自动执行判定。
    """
    if roles is None:
        # 文件名 persona_<序号>_<角色名>.json → 角色名。旧写法只剥 "persona_"，
        # 留下序号（persona_14_knowledge_curator → "14_knowledge_curator"），
        # 拼成 ccs-14_knowledge_curator 与实际会话 ccs-knowledge_curator 永不相等
        # → pane capture 100% 失败 → G3 僵尸回收整体失效（实测 6h 内 31 次全带前缀）。
        roles = [re.sub(r"^persona_\d+_", "", p.stem) for p in
                 Path.home().joinpath("hermes-session-roles", "personas", "session-roles").glob("persona_*.json")]
    roles = [r for r in roles if r not in _NON_PRODUCTION_ROLES]
    proposals: list[dict] = []
    for role in roles:
        try:
            r = propose_governance_action(role)
            if r.get("proposed"):
                action = ("idle" if r.get("idle") else
                          "violation" if r.get("violation") else
                          "content_void")
                r2 = dict(r)
                r2["role"] = role
                r2["_action"] = action
                proposals.append(r2)
        except Exception as e:
            LOGGER.debug("scan_and_propose 角色失败 %s: %s", role, e)
    return proposals

