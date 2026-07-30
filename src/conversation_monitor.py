"""
Conversation Monitor — 对话增量监控模块

从 CCS 角色的 tmux pane 中增量读取对话内容，提取三个信号：
  1. 步骤完成（step_completed）
  2. 角色空闲（idle_complaint）
  3. 越界操作（violation）——基于实际执行的 shell 命令检测

所有信号都是 bus-based exit_condition 的补充，不替代现有逻辑。
"""

import hashlib
import logging
import re
import subprocess
import threading
from dataclasses import dataclass, field
# ponytail: import Optional could be added if signature type hints are needed later

LOGGER = logging.getLogger("conversation_monitor")

# ── 角色禁区：运行时从 session-launcher 加载，不硬编码 ──
_FORBIDDEN_OPS: dict[str, list[str]] = {}
_DEFAULT_FORBIDDEN = ["run_tests", "edit_config", "deploy",
                      "start_ccs", "edit_persona_json"]

def _load_forbidden_ops() -> dict:
    """从 session-launcher 的 routing.roles 拉取禁区定义"""
    try:
        from pathlib import Path
        import importlib.util, sys
        launcher_src = str(Path.home() / "session-launcher" / "src")
        spec = importlib.util.spec_from_file_location(
            "launcher_roles", str(Path(launcher_src) / "routing" / "roles.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return dict(mod._FORBIDDEN_MAP)
    except Exception as e:
        LOGGER.warning("failed to load forbidden_ops from roles.py: %s", e)
        return {}  # fallback 为空 dict，调用方走 _DEFAULT_FORBIDDEN

# 懒初始化：首次调用时加载一次，结果缓存（线程安全）
_forbidden_ops_loaded = False
_forbidden_ops_lock = threading.Lock()

def _ensure_forbidden_ops():
    global _forbidden_ops_loaded, _FORBIDDEN_OPS
    if _forbidden_ops_loaded:
        return
    with _forbidden_ops_lock:
        if _forbidden_ops_loaded:  # double-check
            return
        _FORBIDDEN_OPS = _load_forbidden_ops()
        _forbidden_ops_loaded = True


@dataclass
class ConversationSignal:
    """结构化对话信号"""
    step_completed: bool = False
    completion_evidence: str = ""
    idle_complaint: bool = False
    idle_evidence: str = ""
    violation_found: bool = False
    violation_commands: list[str] = field(default_factory=list)
    violation_role: str = ""

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
        r"\bsystemctl\b(?! restart)",
        r"\bsed -i\b.*\.(json|yaml|yml|conf|toml|ini)",
        r"\b(?:vim|vi)\b .*\.(json|yaml|yml|conf|env|toml|ini)",
        r"\bnano .*\.(json|yaml|yml|conf|env)",
    ],
    "edit_persona_json": [r"persona_.*\.json"],
    "write_other_workspace": [r"/home/[^/]+/ccs-workspaces/(?!engineer)[^/]+/"],
}

# ── 信号 1：步骤完成 ──
_COMPLETION_PATTERNS = [
    r"(s\d+ .*完成)",
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
# 排除（有意义的等待 → 不是空闲）
_EXCLUDE_IDLE = [r"waiting for", r"等待.*回复|等待.*确认|等待.*修复|waiting.*coordinator"]

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
            LOGGER.debug("pane capture failed: role=%s returncode=%s", role, r.returncode)
            return ""
        pane = r.stdout
    except Exception as e:
        LOGGER.debug("pane capture exception: role=%s error=%s", role, e)
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
    forbidden_ops = _FORBIDDEN_OPS.get(role, _DEFAULT_FORBIDDEN)
    found = []
    for op_type in forbidden_ops:
        for pat in _SHELL_SIGNATURES.get(op_type, []):
            matches = re.findall(pat, pane_text, re.IGNORECASE)
            for m in matches:
                found.append(f"{op_type}: {m}")
    if found:
        sig.violation_found = True
        sig.violation_commands = found[:5]
        sig.violation_role = role

    return sig


def get_conversation_signal(role: str) -> ConversationSignal:
    """一站式接口：capture → parse → 返回信号"""
    pane = capture_role_pane(role)
    return parse_conversation_signals(pane, role)
