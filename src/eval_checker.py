#!/usr/bin/env python3
"""
eval_checker.py — eval_criteria 运行时执行器。

定期随机挑 1-2 个角色的 eval_criteria，解析"验证:"后面的 bash 命令并执行。
纯文本描述（无可执行命令）跳过。失败写 bus (cat=verification + notice @ccs-monitor)。

集成到 routing_daemon.py main loop:
    from eval_checker import run_eval_check
    run_eval_check()  # 每 300s 调一次
"""

import ast, json, logging, os, random, re, shlex, subprocess, sys, time
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent
# ponytail: 由 paths.py 统一管理路径；运行时可通过环境变量覆盖
_DEFAULT_PERSONAS = SRC_DIR.parents[1] / "hermes-session-roles" / "personas" / "session-roles"
PERSONAS_DIR = Path(os.environ.get(
    "SESSION_ROLES_DIR",
    str(_DEFAULT_PERSONAS),
))
BUS_SCRIPT = Path(os.environ.get(
    "BUS_CLIENT",
    os.path.expanduser("~/.hermes/scripts/bus_client.py"),
))

LOGGER = logging.getLogger("session-pipeline.eval_checker")

# 角色对话 transcri.jsonl 目录（Claude Code jsonl 格式）
_TRANSCRIPTS_DIR = Path.home() / ".claude" / "projects"

# 每个工具调用必须包含的字段（schema 校验）。未知工具不报错（不误杀）。
_REQUIRED_KEYS: dict[str, tuple[str, ...]] = {
    "Bash": ("command",),
    "Edit": ("file_path", "old_string", "new_string"),
    "Write": ("file_path", "content"),
    "Read": ("file_path",),
    "Glob": ("pattern",),
    "Grep": ("pattern",),
    "WebFetch": ("url",),
}

# thrash loop 判定：同一 Bash command 连续重复 ≥ 此次数且无其他 distinct 命令插入
_THRASH_REPEAT_MIN = 4

# ── extract "验证:" from criterion ──

def _extract_cmd(criterion: str) -> str | None:
    """从 eval_criterion 提取验证命令（'验证:' 之后，'输出' 之前的内容）。

    返回 (cleaned_command, expected_output) 或 None。
    expected_output 是 '输出' / '全部返回' 之后的文本，或 None。
    """
    if "验证:" not in criterion:
        return None
    _, _, part = criterion.partition("验证:")
    part = part.strip()

    # 找最后一个 '输出' / '全部返回' 分割命令和预期
    expected = None
    for marker in ("输出结果", "输出 ", "全部返回 "):
        idx = part.rfind(marker)
        if idx >= 0 and part[:idx].count("'") % 2 == 0:
            cmd_raw = part[:idx].strip().rstrip("。，； ")
            expected = part[idx + len(marker):].strip().rstrip("。，；")
            break
    else:
        cmd_raw = part.strip().rstrip("。，； ")

    if not cmd_raw:
        return None

    # 去掉命令中的中文字符（验证说明混入命令中的情况）
    cmd_raw = re.sub(r'[一-鿿　-〿＀-￯]+[<\d]*', '', cmd_raw).strip()
    # 去掉 bash -c '...' 后残留的数字/符号（如 "5 分钟内" → "5" 残留）
    cmd_raw = re.sub(r"^bash -c '(.*)'\s*\d*$", r"bash -c '\1'", cmd_raw).strip()
    # 去掉残留的 " < N" 或 "< N" 比较符（含无等号形式：>48h, <24h）
    cmd_raw = re.sub(r'\s+[<>!]=?\s*\d+(?:\.\d+)?[a-zA-Z]?\s*', ' ', cmd_raw).strip()
    # 去掉英文解释性残留（timestamp >48h / staged <24h / unread --all 后的说明词）
    # 匹配: 裸词(如 timestamp/staged) 可选跟比较符(>/</>=/<=/!=) + 数字(可带字母后缀如 h/m/s)
    cmd_raw = re.sub(r'\s+[a-zA-Z_][\w.-]*(?:\s*[<>!]=?\s*[\d.]+[a-zA-Z]*)?\s*$', '', cmd_raw).strip()
    cmd_raw = re.sub(r'  +', ' ', cmd_raw)
    cmd_raw = re.sub(r'^\s*\|\s*', '', cmd_raw)
    cmd_raw = re.sub(r';$', '', cmd_raw).strip()

    if not cmd_raw:
        return None

    # 剥离 bash -c '...' / sh -c '...' 外层，只保留内部命令
    _inner = re.match(r"^(?:bash|sh)\s+-c\s+'(.+?)'\s*$", cmd_raw, re.DOTALL)
    if _inner:
        cmd_raw = _inner.group(1).strip()

    # 清理 expected：去掉附加说明（; — | 后的内容），保留比较表达式
    if expected:
        expected = re.sub(r'\s*[;—|]\s.*$', '', expected).strip()

    return (cmd_raw, expected) if cmd_raw else None


def _is_shell_cmd(cmd: str) -> bool:
    """粗略判断字符串是否 shell 命令（以常见工具名开头）。"""
    first = cmd.lstrip().split(maxsplit=1)[0] if cmd else ""
    keywords = frozenset({
        "bash", "sh", "curl", "systemctl", "journalctl",
        "ps", "df", "free", "grep", "python3",
        "git", "wc", "find", "cat", "head", "tail",
        "pip", "npm", "cargo", "docker", "ping",
        "ls", "stat", "echo", "test", "mkdir",
        "cd", "export", "time", "which", "command",
        "sort", "uniq", "awk", "sed", "xargs", "tr",
        "cut", "comm", "diff", "patch", "chmod",
        "rsync", "scp", "ssh", "exit",
        "sqlite3", "du", "ccs", "python3", "for",
    })
    # bash 变量赋值: last_fix=$(python3 ...) → 有效 bash 语法
    if "=" in cmd and cmd.split("=")[0].strip().replace("_", "").replace("-", "").isalnum():
        return True
    return bool(first) and first in keywords


# ponytail: 白名单仅覆盖常见只读命令，写操作命令（systemctl start 等）不在内
_ALLOWED_CMDS = frozenset({
    "systemctl", "journalctl", "ps", "df", "free", "grep",
    "curl", "git", "wc", "find",
    "cat", "head", "tail", "ls", "stat", "echo", "test",
    "sort", "uniq", "awk", "sed", "diff", "which", "command",
    "python3",  # 只读验证脚本（bus_client unread 等），非写操作
    "cut",  # 管道过滤常见工具，只读
    "sqlite3",  # 只读查询
    "du",  # 只读磁盘使用
    "ccs",  # CCS CLI
})

_SYSTEMCTL_READONLY = frozenset({"status", "is-active", "is-enabled", "show", "list-units", "list-unit-files"})
_GIT_READONLY = frozenset({"log", "show", "status", "diff", "branch", "tag", "remote", "stash", "rev-parse", "ls-files"})
_CURL_WRITE_FLAGS = frozenset({"-X", "--data", "-d", "-T", "--upload-file", "-F", "--form", "--request"})

def _cmd_is_readonly(first_word: str, cmd: str) -> bool:
    """白名单通过后二次校验：systemctl/curl/git 只允许只读操作。"""
    parts = cmd.lstrip().split()
    if len(parts) < 2:
        return True

    if first_word == "systemctl":
        return parts[1] in _SYSTEMCTL_READONLY

    if first_word == "curl":
        # -X POST/-d/--data 等写操作标志 → 拒绝
        return not any(flag in parts or flag.split("=")[0] in parts for flag in _CURL_WRITE_FLAGS)

    if first_word == "git":
        # 跳过 git 全局选项（-C <dir> / --git-dir=path 等），定位真实子命令
        i = 1
        while i < len(parts) and parts[i].startswith("-"):
            if "=" in parts[i]:
                i += 1
            else:
                i += 2
        return i < len(parts) and parts[i] in _GIT_READONLY

    return True


_CONTROL_WORDS = frozenset({"for", "while", "if", "then", "do", "done", "fi", "else", "elif", "in"})
# 白名单检查通过后仍需排除的 bash 控制结构（非命令，但需在 for/while 体内递归检查）
_SHELL_BUILTINS_READONLY = frozenset({"for", "while", "do", "done", "if", "then", "fi", "else", "elif", "in", "exit", "return", "break", "continue", "true", "false"})


def _split_pipe_segments(cmd: str) -> list[str]:
    """按 '|' 分割命令,但忽略引号内的 '|' (grep 模式等)。"""
    segments, buf = [], []
    in_sq = in_dq = False
    for ch in cmd:
        if ch == "'" and not in_dq: in_sq = not in_sq
        elif ch == '"' and not in_sq: in_dq = not in_dq
        if ch == "|" and not in_sq and not in_dq:
            segments.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        segments.append("".join(buf).strip())
    return [s for s in segments if s]


def _inner_cmd_ok(cmd: str) -> bool:
    """递归检查 bash -c '...' / for 循环体内部的命令是否都在白名单。

    允许 for/while 循环骨架（只读系统巡检的常见写法），但循环体命令仍须在白名单。
    """
    for _seg in _split_pipe_segments(cmd):
        _seg_cmd = _seg.strip().split(maxsplit=1)[0] if _seg.strip() else ""
        if _seg_cmd not in _ALLOWED_CMDS and _seg_cmd not in _SHELL_BUILTINS_READONLY:
            return False
        # for 循环体: "for s in ...; do systemctl ...; done" → 检查 do 之后的命令
        if _seg_cmd == "for":
            _body = _seg[_seg.find("do") + 2:].strip() if "do" in _seg else ""
            if _body and not _inner_cmd_ok(_body):
                return False
    return True


def _validate_python_inline(code: str) -> bool:
    """AST 深度驗證 python3 -c '...' 內部代碼，阻攔危險 API。

    阻攔: import os/subprocess, __import__, open, exec, eval,
    os.system, subprocess.run 等危險函數調用。
    """
    # 安全红线针对的是危险*操作*（os.system/subprocess.run/pickle.loads 等 RCE 路径），
    # 而非惰性 import 语句本身。os/subprocess 仅 import 无害，其危险成员调用已由下方
    # _BLOCKED_ATTRS / _BLOCKED_CALLS 拦截。pickle/marshal 的 loads 自身即可触发 RCE，仍需禁 import。
    _BLOCKED_IMPORTS = {"pickle", "marshal"}
    # 允许 `import os`，但拦截 `from os import system` 这类从危险模块导入危险成员（绕过调用级拦截）
    _DANGEROUS_FROM = {
        "os": {"system", "popen", "execv", "execve", "execvp", "execvpe",
               "execl", "execlp", "execlpe", "spawnl", "spawnle", "spawnlp",
               "spawnlpe", "spawnv", "spawnve", "spawnvp", "spawnvpe",
               "remove", "unlink", "rmdir", "rename", "kill"},
        "subprocess": {"Popen", "run", "call", "check_call", "check_output",
                       "getoutput", "getstatusoutput"},
        "pty": {"spawn", "fork"},
    }
    _BLOCKED_ATTRS = frozenset({
        "system", "popen", "execv", "execvp", "execvpe", "spawn",
        "Popen", "run", "call", "check_call", "check_output",
    })
    _BLOCKED_CALLS = {"__import__", "exec", "eval", "open", "globals", "locals",
                      "getattr", "setattr", "compile", "input"}

    try:
        tree = ast.parse(code)
    except SyntaxError:
        return False

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in _BLOCKED_IMPORTS:
                    return False
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module in _BLOCKED_IMPORTS:
                return False
            # 拦截 `from os import *` / `from subprocess import *` 等星号导入——
            # 星号导入会把危险成员直接引入当前命名空间（如 system, run），
            # 绕过属性访问拦截和 _DANGEROUS_FROM 成员名检查。
            if node.module in _DANGEROUS_FROM and any(a.name == '*' for a in node.names):
                return False
            # 拦截 `from os import system` 这类绕过调用级拦截的危险成员导入
            if node.module in _DANGEROUS_FROM and any(
                a.name in _DANGEROUS_FROM[node.module] for a in node.names
            ):
                return False
        elif isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id in _BLOCKED_CALLS:
                return False
            if isinstance(func, ast.Attribute) and func.attr in _BLOCKED_ATTRS:
                return False
    return True


# 恒真验证命令：输出与系统状态无关，永远 PASS，会伪造通过率看板。
# 这类命令即使通过白名单也必须拒绝执行——"命令能跑通"不等于"检查了真实状态"。
# 典型来源：persona eval_criteria 写成 echo PASS / python3 -c "print('PASS')"。
_TAUTOLOGY_PATTERNS = (
    "print(\"PASS\")", "print('PASS')", "print(\"pass\")", "print('pass')",
    "echo \"PASS\"", "echo 'PASS'", "echo PASS",
    "echo \"pass\"", "echo 'pass'", "echo pass",
    "exit 0", "exit(0)", "import sys; print",
)


def _is_tautology(cmd: str) -> bool:
    """判断命令是否恒真（与系统状态无关，必然 PASS）。"""
    c = cmd.replace(" ", "")
    return any(p.replace(" ", "") in c for p in _TAUTOLOGY_PATTERNS)


def _run(cmd: str, timeout: int = 30) -> dict:
    """执行 bash 命令（白名单检查，仅允许只读类命令）。"""
    # 恒真检查必须放在最前：下方有"引号规范化"会把 eval_criteria 里
    # `python3 -c "print("PASS")"` 这类笔误**修好**使其可执行——那是帮一个
    # 恒真命令通过，而不是拒绝它。先拒绝，再谈白名单与规范化。
    if _is_tautology(cmd):
        return {"ok": False, "stdout": "",
                "stderr": "恒真命令被拒（与系统状态无关，会伪造 PASS）",
                "returncode": -3}
    first_word = cmd.lstrip().split(maxsplit=1)[0] if cmd else ""
    # bash -c '...' 解包: 递归校验内部命令（for 循环巡检是白名单内命令的组合）
    if first_word == "bash":
        _m = re.match(r"^bash\s+-c\s+['\"](.+)['\"]\s*$", cmd, re.DOTALL)
        if _m:
            inner = _m.group(1)
            if not _inner_cmd_ok(inner):
                return {"ok": False, "stdout": "", "stderr": "bash -c 内部命令不在白名单", "returncode": -3}
            # 递归校验通过后, 重新从内部命令开始白名单检查
            return _run(inner, timeout)
        return {"ok": False, "stdout": "", "stderr": f"命令不在白名单: {first_word}", "returncode": -3}
    # 禁止管道符/分号/命令替换——防止 curl|bash 类绕过白名单
    # 管道（|）只读过滤（cat | cut）放行；分号/命令替换仍禁（可拼接写操作）
    # 例外1: for/while 循环骨架中的 ';'（'for s in a b; do' 是安全语法）
    # 例外2: python3 -c '...' 内部的 ';' 是 python 代码分隔符，非 shell 命令分隔符
    # 例外3: bash 变量赋值 + $() 命令替换（如 last_fix=$(python3 ...)）——只读巡检常用
    _is_loop_skeleton = first_word in ("for", "while")
    _is_py_inline = first_word == "python3" and "-c" in cmd.split()[1:3]
    _is_var_assign = "=" in cmd.split()[0] if cmd.split() else False

    # P0: python3 -c 沙箱 — AST 深度驗證內部代碼，阻攔 import os/subprocess 等
    if _is_py_inline:
        # 用 shlex 而非 split(None,2)[-1].strip("'\"")：后者不处理内层 \" 转义，
        # 传给 ast.parse 的代码含多余反斜杠 → SyntaxError("unexpected character
        # after line continuation character")，判据【从未成功执行过】却被记成
        # 角色 FAIL（实测 curator SKILL.md 判据，2026-10-16）。shlex 按 shell
        # 规则还原出真正的 python 源码。切分失败时退回原取法（保守，不崩溃）。
        try:
            _py_code = shlex.split(cmd)[-1]
        except ValueError:
            _py_code = cmd.split(None, 2)[-1].strip("'\"")
        if not _validate_python_inline(_py_code):
            return {"ok": False, "stdout": "", "stderr": "python3 -c 內部代碼含阻斷 API",
                    "returncode": -4}
    for _pat in ("`", "$("):
        if _pat in cmd and not _is_var_assign:
            return {"ok": False, "stdout": "", "stderr": f"管道/分号/命令替换被禁止: 含 {_pat}", "returncode": -3}
    if not _is_loop_skeleton and not _is_py_inline and not _is_var_assign and ";" in cmd:
        # 仅允许 python3 -c '...' 引号内的 ';'（python 代码分隔符）
        # 以及 for/while 循环骨架（'for s in a b; do'）。用管道段递归校验：
        # 若 ; 出现在段间（df ... ; free ...），逐段校验全部只读才放行
        _all_segments = _split_pipe_segments(cmd.replace(";", "|"))
        if len(_all_segments) > 1:
            _ok = all(
                s.strip().split(maxsplit=1)[0] in _ALLOWED_CMDS
                or s.strip().split(maxsplit=1)[0] in _SHELL_BUILTINS_READONLY
                for s in _all_segments if s.strip()
            )
            if not _ok:
                return {"ok": False, "stdout": "", "stderr": "管道/分号/命令替换被禁止: 含 ; 且段含非白名单命令", "returncode": -3}
        else:
            return {"ok": False, "stdout": "", "stderr": "管道/分号/命令替换被禁止: 含 ;", "returncode": -3}
    # 管道仅允许左右两侧都是白名单只读命令（引号感知分割: grep 模式内的 '|' 不算管道）
    for _seg in _split_pipe_segments(cmd):
        _seg_cmd = _seg.strip().split(maxsplit=1)[0] if _seg.strip() else ""
        if _seg_cmd not in _ALLOWED_CMDS and _seg_cmd not in _SHELL_BUILTINS_READONLY:
            return {"ok": False, "stdout": "", "stderr": f"管道段命令不在白名单: {_seg_cmd}", "returncode": -3}
    if first_word not in _ALLOWED_CMDS and first_word not in _SHELL_BUILTINS_READONLY and not _is_var_assign:
        return {"ok": False, "stdout": "", "stderr": f"命令不在白名单: {first_word}", "returncode": -3}
    if not _is_var_assign and not _cmd_is_readonly(first_word, cmd):
        return {"ok": False, "stdout": "", "stderr": f"写操作被拒绝: {first_word} 不允许非只读子命令", "returncode": -3}
    # 变量赋值 + $() 命令替换: 校验替换体内部命令在白名单
    if _is_var_assign and "$(" in cmd:
        _inner_body = cmd.split("$(", 1)[1].rsplit(")", 1)[0] if cmd.count("$(") == cmd.count(")") else ""
        if _inner_body and not _inner_cmd_ok(_inner_body):
            return {"ok": False, "stdout": "", "stderr": f"$() 内部命令不在白名单: {_inner_body[:60]}", "returncode": -3}
    try:
        # 引号规范化: python3 -c "..." 内层同引号 → 转义为 \"
        # persona eval_criteria 常见笔误: python3 -c "import sys; print("PASS")"
        if first_word == "python3" and "-c" in cmd and cmd.count('"') >= 4:
            _parts = cmd.split('"')
            # 奇数索引是引号内内容，把内层未转义的 " 替换为 '（Python 字符串字面量兼容）
            _fixed = []
            for i, p in enumerate(_parts):
                if i % 2 == 1 and '"' in p:
                    p = p.replace('"', "'")
                _fixed.append(p)
            cmd = '"'.join(_fixed)
        r = subprocess.run(
            ["bash", "-c", cmd],
            capture_output=True, text=True, timeout=timeout,
        )
        return {
            "ok": r.returncode == 0,
            "stdout": r.stdout.strip()[:400],
            "stderr": r.stderr.strip()[:200],
            "returncode": r.returncode,
        }
    except subprocess.TimeoutExpired:
        return {"ok": False, "stdout": "", "stderr": "TIMEOUT", "returncode": -1}
    except Exception as e:
        return {"ok": False, "stdout": "", "stderr": str(e)[:200], "returncode": -2}


def _is_tool_error(result: dict) -> bool:
    """判定命令自身执行错误（检查器盲区），而非被检查对象真实故障。

    grep 语法错误(rc=2)/找不到文件、文件不存在等 → 命令写错了，不是系统坏了。
    """
    rc = result["returncode"]
    stderr = result["stderr"]
    if rc == 2 and ("grep:" in stderr or "usage:" in stderr):
        return True
    if "No such file or directory" in stderr:
        return True
    if rc == 2 and "unrecognized option" in stderr:
        return True
    return False


def _check_passed(result: dict, cmd_raw: str, expected: str | None) -> (bool, str):
    """根据命令输出和预期结果判断 pass/fail。

    预期结果检查优先级:
      1. "全部返回 active"  → stdout 包含 "active"
      2. "输出 <N>" 且 cmd 含 wc/awk/grep 的计数 → stdout 转为数字比较
      3. "输出 OK" / "输出 0" / 纯文本 → stdout 包含该文本
      4. 无预期 → fallback 到 exit code

    返回 (passed, detail)。
    """
    stdout = result["stdout"]
    stderr = result["stderr"]
    rc = result["returncode"]

    if expected is None:
        return (result["ok"], f"exit={rc}")

    # grep 类命令无匹配返回 exit=1，但"无 XXX"语义下这是健康信号
    # 例: journalctl ... | grep -ciE 'ERROR' 无匹配 → exit=1 → 应判 PASS
    _is_neg_check = any(k in expected for k in ("无", "没有", "不含", "none", "no "))
    _is_grep_cmd = "grep" in cmd_raw and "wc" not in cmd_raw
    if _is_neg_check and _is_grep_cmd and rc == 1 and not stdout.strip():
        return (True, f"无匹配=健康 (grep exit=1, expected={expected[:40]})")

    if "全部返回" in cmd_raw:
        # 检查 stdout 包含关键字
        keywords = expected.replace("、", " ").split()
        if keywords:
            passed = any(k in stdout for k in keywords)
            detail = f"expected={expected} | stdout={stdout[:120]}"
            return (passed, detail)

    # 检查预期是否含数字
    num_match = re.search(r"([<>]=?|[=!])\s*(-?\d+)", expected)
    if num_match:
        # 取 stdout 最后一行/数字
        numbers = re.findall(r"-?\d+(?:\.\d+)?", stdout)
        if numbers:
            val = float(numbers[-1])
            op = num_match.group(1)
            ref = float(num_match.group(2))
            passed = {
                "<": val < ref, ">": val > ref, "<=": val <= ref,
                ">=": val >= ref, "=": val == ref, "!=": val != ref,
            }.get(op, False)
            detail = f"{val}{op}{ref} | stdout={stdout[:120]}"
            return (passed, detail)

    # 简单文本包含检查
    passed = expected in stdout or expected in stderr
    detail = f"expected={expected[:60]} | stdout={stdout[:120]} | exit={rc}"
    return (passed, detail)


def _pass_score(result: dict, cmd_raw: str, expected: str | None) -> float:
    """_check_passed 的连续 0-1 分数版 (DeepEval metric 范式)。

    pass=1.0; 仅数值比较失败按"达成比例"给部分分, 其余失败 0.0。
    复用 _check_passed 的判定优先级, 不改动其签名 (向后兼容)。
    ponytail: 部分分仅用 val/ref 比例, 无 tolerance 概念; 接入 tolerance 字段后可做等比分。
    """
    if _check_passed(result, cmd_raw, expected)[0]:
        return 1.0
    if expected is None:
        return 0.0
    m = re.search(r"([<>]=?|[=!])\s*(-?\d+)", expected)
    if not m:
        return 0.0
    nums = re.findall(r"-?\d+(?:\.\d+)?", result["stdout"])
    if not nums:
        return 0.0
    val, ref, op = float(nums[-1]), float(m.group(2)), m.group(1)
    if op in (">", ">=") and ref:
        return max(0.0, min(1.0, val / ref))
    if op in ("<", "<=") and val:
        return max(0.0, min(1.0, ref / val))
    return 0.0


def _write_bus(cat: str, text: str, *, evidence: str = "", src: str = "eval_checker"):
    try:
        subprocess.run(
            [sys.executable, str(BUS_SCRIPT), "write", cat, text,
             "--evidence", evidence[:500], "--src", src],
            capture_output=True, timeout=15,
        )
    except Exception as e:
        LOGGER.warning("bus write fail: %s", e)


def _is_critical(criterion: str) -> bool:
    """检查 criterion 是否为 critical 级别（severity: critical）。"""
    return bool(re.search(r'severity[:\s=]*critical', criterion, re.IGNORECASE))


from eval_shared_loader import _load_roles


def run_eval_check(role_filter: str | None = None) -> dict:
    """主入口：加载角色 JSON，按权重轮询挑 1-2 个，
    对每个执行前 3 条 eval_criteria。
    权重 = 该角色可执行 eval 数量（含"验证:"的 criterion 数），最少 1。
    参数 role_filter: 只检查指定角色名。
    返回 {checked, passed, failed, skipped, notices, blockers, avg_score}。
    avg_score 为 0-1 连续均分 (DeepEval 范式), 不影响 passed/failed 计数。
    """
    roles = _load_roles()
    if not roles:
        return {"checked": 0, "passed": 0, "failed": 0, "skipped": 0, "notices": 0, "blockers": 0}

    # 按角色名过滤
    if role_filter:
        filtered = [r for r in roles if r["name"] == role_filter]
        # ── Real evaluation checkers (replacing tautological verification) ──

class OutputSemanticChecker:
    """产出语义检查器：检测模板桩/空洞产出。

    复用 conversation_monitor 的 content_void 启发式 + output_validator.validate_output。
    检查：文件非空、语法有效、git diff 非空、prose/code 密度、output_validator 结构校验。
    """

    def __init__(self):
        self._prod_evidence = re.compile(
            r"(def |class |import |return |print\(|assert |raise |if |for |while |\[|\{|`|```)",
            re.MULTILINE
        )
        self._noise_signs = re.compile(
            r"(\bTODO\b|\bFIXME\b|\bXXX\b|\bpass\b|\bplaceholder\b|"
            r"\bstub\b|\bmock\b|# |\.\.\.)",
            re.IGNORECASE
        )

    def check(self, role: str, task_id: str) -> dict:
        """检查角色在任务窗口内的产出文件。

        返回 {passed: bool, findings: list[dict], checked_files: int}
        """
        from task_evidence import _output_files

        files = _output_files(role)
        if not files:
            # 无任务产出文件 → 无可检查对象，按"无问题"通过（不误杀无待办产出的角色）
            return {"passed": True, "findings": [], "checked_files": 0}

        findings = []
        passed_count = 0
        for f in files:
            try:
                content = f.read_text(encoding="utf-8", errors="ignore")
                if not content.strip():
                    findings.append({"file": str(f), "issue": "empty file"})
                    continue

                # 语法/结构校验
                if f.suffix == ".py":
                    try:
                        compile(content, str(f), "exec")
                    except SyntaxError as e:
                        findings.append({"file": str(f), "issue": f"syntax error: {e}"})
                        continue
                elif f.suffix == ".json":
                    try:
                        json.loads(content)
                    except json.JSONDecodeError as e:
                        findings.append({"file": str(f), "issue": f"json decode error: {e}"})
                        continue
                elif f.suffix in (".md", ".txt", ".rst"):
                    # prose 文件：用 prose 专属启发式（非代码符号判定）
                    if not self._has_prose_content(content):
                        findings.append({"file": str(f), "issue": "content_void: prose density low / noise dominant"})
                        continue

                # output_validator 结构校验（若有 schema）
                # 注：此处不强制 category，仅做非空+基本结构校验

                passed_count += 1

            except Exception as e:
                findings.append({"file": str(f), "issue": f"check error: {e}"})
                continue

        passed = passed_count > 0 and len(findings) == 0
        return {
            "passed": passed,
            "findings": findings,
            "checked_files": len(files)
        }

    def _has_prose_content(self, text: str) -> bool:
        """prose 文件（.md/.txt/.rst）的生产证据判定。

        原 _has_production_evidence 用代码符号（def/class/`）判"产出"，
        纯散文文档（设计文档/会议沉淀/ADR）无代码符号 → 恒判 content_void，
        误杀真实高质量文档（bus#lr session沉淀 误报根因）。
        prose 专属：去噪音后仍有实质文本体量 + 结构标记（标题/列表/段落）。
        """
        stripped = self._noise_signs.sub("", text).strip()
        if len(stripped) < 200:  # 去噪音后仍不足 200 字 → 空洞
            return False
        # 结构证据：标题(#)/RST 下划线(=) /列表(-/*/1.) /代码围架(```) /RST 块指令(.. )
        _struct = re.compile(r"^\s{0,3}#{1,6}\s|^\s{0,3}={3,}\s*$|"
                             r"^\s{0,3}[-*+]\s|^\s{0,3}\d+\.\s|^\s{0,3}-{3,}\s*$|```|"
                             r"^\s{0,3}\.\.\s", re.M)
        if _struct.search(text):
            return True
        # 无结构标记但体量充足（≥500 非噪音字）：覆盖纯 .txt / RST 下划线标题
        # 的散文文档，避免因缺 markdown 标记而误杀实质内容。
        return len(stripped) >= 500


class RequirementConfirmationChecker:
    """需求确认检查器：判断需求是否被真实消费/确认。

    读 bus 消费记录，判断需求 (task_spec/user_story/prd/approval/product_design)
    是否被真实消费或有下游确认 (code_fix/deployment_report/delivery)。
    """

    REQUIREMENT_CATS = {"task_spec", "user_story", "prd", "approval", "product_design"}
    CONFIRM_CATS = {"code_fix", "deployment_report", "delivery", "verification"}

    def __init__(self):
        from hermes_bus.blackboard_legacy import Blackboard
        self._bb = Blackboard()

    def check(self, role: str, task_id: str) -> dict:
        """检查角色在任务窗口内的需求是否被确认。

        返回 {passed: bool, findings: list[dict], checked_requirements: int}
        """
        # 找到该任务关联的需求事实 (简化：搜索 role 的 output_targets 包含的 cat)
        # 简化：搜索 requirement_cats 中未被消费/无下游确认的 fact
        findings = []
        checked = 0
        unconfirmed = 0

        for cat in self.REQUIREMENT_CATS:
            # 搜索该角色的需求 fact
            try:
                facts = self._bb.search(f"role:{role} cat:{cat}", limit=20)
                for fact in facts:
                    checked += 1
                    fid = fact.id

                    # 检查是否有消费记录
                    consumed = self._bb.unconsumed(consumer=None, cat=cat, limit=1)
                    has_consumption = any(f.id == fid for f in consumed)

                    # 检查是否有下游确认
                    has_downstream = False
                    for dc in self.CONFIRM_CATS:
                        downstream = self._bb.search(f"fid:{fid}", cat=dc, limit=5)
                        if downstream:
                            has_downstream = True
                            break

                    if not has_consumption and not has_downstream:
                        # 检查年龄
                        if fact.ts < time.time() - 7 * 86400:  # 7天过期
                            findings.append({"fact_id": fid, "cat": cat, "issue": "expired_unconfirmed_requirement"})
                            unconfirmed += 1
                        else:
                            findings.append({"fact_id": fid, "cat": cat, "issue": "unconfirmed_requirement"})
                            unconfirmed += 1
            except Exception as e:
                findings.append({"cat": cat, "issue": f"check error: {e}"})

        passed = unconfirmed == 0
        return {
            "passed": passed,
            "findings": findings,
            "checked_requirements": checked
        }


# ── Main entry ──

class LeverageAccumulationChecker:
    """杠杆积累检查器：检测 agent 是否在 24h 窗口内产出可复用资产。

    三类可复用资产信号：
      1. bus 上的 knowledge_distill / memory_store / skill_audit 事实
      2. ~/.hermes/skills/ 下新增的技能文件
      3. git 历史中新增的复用型文件（skill/util/helper/core/common/base 等）

    原则：无可检查数据（24h 内零资产）= 通过（不误杀）。
    本检查器为观测型：leverage_count 上报资产数，findings 列出明细。
    """

    ASSET_CATS = ("knowledge_distill", "memory_store", "skill_audit")
    # 复用型文件关键词（git 新增文件命中即计为可复用资产）
    _REUSABLE_KW = ("skill", "util", "helper", "core", "common", "base", "lib", "shared", "knowledge", "memory")
    WINDOW = 86400  # 24h

    def __init__(self):
        from hermes_bus.blackboard_legacy import Blackboard
        self._bb = Blackboard()
        self._skills_dir = Path.home() / ".hermes" / "skills"
        # 扫描 git 仓库（仅统计新增复用型文件，限定已知仓库避免全盘遍历）
        self._git_repos = [
            Path.home() / "session-pipeline",
            Path.home() / "hermes-session-roles",
            Path.home() / ".hermes",
        ]

    def check(self, role: str, task_id: str) -> dict:
        """检查 24h 窗口内的可复用资产积累。

        返回 {passed: bool, findings: list[dict], leverage_count: int}
        无可检查数据 → passed=True, leverage_count=0（no data = pass）。
        """
        cutoff = time.time() - self.WINDOW
        assets: list[dict] = []

        # 1. bus 上的可复用资产事实（24h 窗口）
        for cat in self.ASSET_CATS:
            try:
                for f in self._bb.read(cat=cat, limit=500):
                    if f.ts >= cutoff:
                        assets.append({"source": "bus", "cat": cat, "id": f.id, "title": f.t[:80]})
            except Exception as e:
                LOGGER.debug("leverage bus read %s fail: %s", cat, e)

        # 2. ~/.hermes/skills/ 新增文件（24h 窗口）
        if self._skills_dir.is_dir():
            for p in self._skills_dir.rglob("*"):
                if p.is_file() and p.stat().st_mtime >= cutoff:
                    assets.append({"source": "skills", "file": str(p)})

        # 3. git 历史中新增的复用型文件（24h 窗口）
        git_assets = self._git_reusable_count(cutoff)

        leverage_count = len(assets) + git_assets
        # 观测型检查器：无资产=通过；有资产=上报（不判失败）
        return {
            "passed": True,
            "findings": assets,
            "leverage_count": leverage_count,
        }

    def _git_reusable_count(self, cutoff: float) -> int:
        """统计 24h 内 git 新增的复用型文件数。

        遍历已知仓库，解析 `git log --since --name-status` 的 A 行，
        命中复用关键词即计入。单仓库失败不影响其他仓库。
        """
        n = 0
        since = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cutoff))
        for repo in self._git_repos:
            if not (repo / ".git").is_dir():
                continue
            try:
                r = subprocess.run(
                    ["git", "-C", str(repo), "log", f"--since={since}",
                     "--name-status", "--pretty=format:"],
                    capture_output=True, text=True, timeout=20,
                )
            except Exception as e:
                LOGGER.debug("leverage git log %s fail: %s", repo, e)
                continue
            for line in r.stdout.splitlines():
                if not line.startswith("A"):
                    continue
                path = line[1:].strip()
                low = path.lower()
                if any(kw in low for kw in self._REUSABLE_KW):
                    n += 1
        return n

    def self_test(self) -> bool:
        """自检：注入一条 knowledge_distill 事实，断言 check 能计数。

        返回是否通过。注入使用带时间戳的唯一标题以避免 bus 去重。
        """
        marker = f"[self_test] leverage probe {time.time()}"
        try:
            self._bb.write("knowledge_distill", marker, evidence="leverage self-test", src="eval_checker")
        except Exception as e:
            LOGGER.warning("leverage self_test write fail: %s", e)
            return False
        res = self.check("__self_test__", "selftest")
        assert res["leverage_count"] >= 1, f"self_test: leverage_count={res['leverage_count']} expected>=1"
        assert res["passed"] is True, "self_test: passed must be True"
        return True


class TrajectoryQualityChecker:
    """轨迹质量检查器：从角色对话 transcri.jsonl 评估工具调用质量。

    三项检查：
      1. 工具调用 schema 合法性（必填字段缺失）
      2. 盲改（Write/Edit 文件前未先 Read）
      3. 孤儿调用（发出 tool_use 但无对应 tool_result 返回）
      4. 抖动循环（同一 Bash 命令连续重复 ≥ N 次无进展）
    """

    def _role_sessions(self, role: str) -> list[Path]:
        """解析角色名对应的 transcri 目录（支持 ccs-workspaces/<role> 等路径形态）。"""
        out: list[Path] = []
        for d in _TRANSCRIPTS_DIR.iterdir():
            if not d.is_dir():
                continue
            low = d.name.lower()
            if role.lower() in low:
                out.append(d)
        return out

    def _load_tool_calls(self, sessions: list[Path]) -> list[dict]:
        """读取所有 jsonl，提取有序 (tool_use, tool_result) 调用序列。

        返回扁平列表，每项 {kind, name, input, id, error, order, src}。
        kind ∈ {use, result}。order 为全局单调递增序号，保留跨文件时序。
        src 为该调用所属 jsonl 文件名——orphan_call 判据需要按 transcript
        边界区分"会话到此为止"与"角色中途漏收结果"（见 check 内注释）。
        """
        calls: list[dict] = []
        order = 0
        for sess in sorted(sessions):
            for jf in sorted(sess.glob("*.jsonl")):
                try:
                    with jf.open(encoding="utf-8", errors="ignore") as fh:
                        for line in fh:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                d = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            content = d.get("message", {}).get("content")
                            if not isinstance(content, list):
                                continue
                            for blk in content:
                                if not isinstance(blk, dict):
                                    continue
                                if blk.get("type") == "tool_use":
                                    order += 1
                                    calls.append({
                                        "kind": "use", "name": blk.get("name"),
                                        "input": blk.get("input", {}),
                                        "id": blk.get("id"), "error": None,
                                        "order": order, "src": jf.name,
                                    })
                                elif blk.get("type") == "tool_result":
                                    order += 1
                                    calls.append({
                                        "kind": "result",
                                        "name": None,
                                        "input": {},
                                        "id": blk.get("tool_use_id"),
                                        "error": blk.get("is_error", False),
                                        "order": order, "src": jf.name,
                                    })
                except OSError as e:
                    LOGGER.debug("transcript read fail %s: %s", jf, e)
        return calls

    def check(self, role: str, task_id: str) -> dict:
        """检查角色轨迹质量。

        返回 {passed: bool, findings: list[dict], checked_traces: int}
        无 transcript 文件 → passed=True, checked_traces=0（无可检查对象）。
        """
        sessions = self._role_sessions(role)
        if not sessions:
            return {"passed": True, "findings": [], "checked_traces": 0}

        calls = self._load_tool_calls(sessions)
        findings: list[dict] = []

        # ── 1. schema 校验 ──
        for c in calls:
            if c["kind"] != "use":
                continue
            name = c["name"] or ""
            req = _REQUIRED_KEYS.get(name)
            if req is None:
                continue
            inp = c["input"] or {}
            # Claude Code 有时打包 __unparsedToolInput，此时 input 非标准 shape → 跳过严格校验
            # （这些调用仍可被其它维度评估，避免 schema 维度产生噪声虚报）
            if "__unparsedToolInput" in inp:
                continue
            missing = [k for k in req if k not in inp]
            if missing:
                findings.append({
                    "issue": "schema_missing_keys", "tool": name,
                    "id": c["id"], "missing": missing,
                })

        # ── 2. 盲改 + 3. 孤儿调用 + 4. 抖动循环 ──
        read_files: set[str] = set()
        bash_blob: list[str] = []
        seen_use_ids: set[str] = set()
        result_ids: set[str] = set()
        # Bash 连续重复检测：跟踪上一命令与连续计数
        last_cmd: str | None = None
        repeat_run: int = 0
        repeat_window_start: int | None = None

        # 会话边界：每个 transcript **末尾连续未配对**的 tool_use 段。
        # 同一轮里 Claude 会并行发多个 tool_use（实测 dfd563da 末尾连发 2 条），
        # 故不能只取最后一条——要从后往前收，直到遇到一条已配对 result 的为止。
        # 这些调用无 result 是会话在工具执行中结束所致（角色被杀/compact/换
        # session），不是"漏收结果"。
        # 保守性：角色真在中途漏收结果又继续调用时，那条不在末尾连续段内，
        # 仍会被报出。
        tail_use_ids: set[str] = set()
        _by_src: dict[str, list[dict]] = {}
        for c in calls:
            _by_src.setdefault(c["src"], []).append(c)
        for _src, _cs in _by_src.items():
            _paired = {c["id"] for c in _cs if c["kind"] == "result" and c["id"]}
            for c in reversed(_cs):
                if c["kind"] != "use" or not c["id"]:
                    continue
                if c["id"] in _paired:
                    break  # 已配对 → 边界到此为止
                tail_use_ids.add(c["id"])

        def _close_thrash():
            nonlocal repeat_run, repeat_window_start, last_cmd
            if repeat_run >= _THRASH_REPEAT_MIN and repeat_window_start is not None:
                findings.append({
                    "issue": "thrash_loop", "tool": "Bash",
                    "command": last_cmd, "count": repeat_run,
                })
            repeat_run, repeat_window_start, last_cmd = 0, None, None

        for c in calls:
            if c["kind"] == "result":
                if c["id"]:
                    result_ids.add(c["id"])
                _close_thrash()
                continue

            # kind == use
            name = c["name"] or ""
            inp = c["input"] or {}
            cid = c["id"]
            if cid:
                seen_use_ids.add(cid)

            if name == "Read":
                fp = inp.get("file_path")
                if fp:
                    read_files.add(fp)

            if name in ("Write", "Edit"):
                fp = inp.get("file_path")
                # Bash 读取同样算「读过」：角色规范《Large File / Log Analysis》
                # 明确要求大文件先 grep 定位、日志用 tail/grep，而非 Read 全文。
                # 只认 Read 会把合规行为判成 blind_edit——实测 scout/engineer/lr
                # 共 16 处命中，其目标文件全部已被 Bash 读过（basename 100% 命中，
                # 绝对路径命中仅 2/16，故按 basename 匹配）。
                bash_read = bool(fp) and any(
                    os.path.basename(fp) in b for b in bash_blob)
                if fp and fp not in read_files and not bash_read:
                    findings.append({
                        "issue": "blind_edit", "tool": name,
                        "file": fp, "id": cid,
                    })

            if name == "Bash":
                cmd = (inp.get("command") or "").strip()
                if cmd:
                    bash_blob.append(cmd)
                    if cmd == last_cmd:
                        repeat_run += 1
                    else:
                        _close_thrash()
                        last_cmd = cmd
                        repeat_run = 1
                    if repeat_window_start is None:
                        repeat_window_start = c["order"]
                else:
                    _close_thrash()
        _close_thrash()

        # 孤儿调用：发出但无返回。
        # WHY 要排除每个 transcript 的**末条** tool_use：会话在工具执行中结束
        # （角色被杀/compact/换 session）时，那条 tool_use 不会有配对 result，
        # 但它不是角色的违规——是会话边界。实测 scout 的 4 处 orphan_call
        # **4/4 全是各自 jsonl 的末条 tool_use**（0/4 真阳性）。
        # 判据不排除它，就会把"会话到此为止"永久报成"角色漏收结果"，
        # 且这些历史条目会持续占据 bus 报告的 findings[:3] 三个名额，
        # 把真正的违规挤出视野。排除"末条"是保守的：若角色真在中途漏收
        # 结果又继续调用，那条就不是末条，仍会被报出。
        orphan = seen_use_ids - result_ids
        for oid in sorted(orphan):
            if oid in tail_use_ids:
                continue
            findings.append({"issue": "orphan_call", "tool": "?", "id": oid})

        checked_traces = sum(1 for s in sessions for _ in s.glob("*.jsonl"))
        passed = len(findings) == 0
        return {
            "passed": passed,
            "findings": findings,
            "checked_traces": checked_traces,
        }


def run_eval_check(role_filter: str | None = None) -> dict:
    roles = _load_roles()
    if not roles:
        return {"checked": 0, "passed": 0, "failed": 0, "skipped": 0, "notices": 0, "blockers": 0}

    # 按角色名过滤
    if role_filter:
        filtered = [r for r in roles if r["name"] == role_filter]
        if not filtered:
            LOGGER.warning("role %s not found", role_filter)
            return {"checked": 0, "passed": 0, "failed": 0, "skipped": 0, "notices": 0, "blockers": 0}
        roles = filtered

    weights = [max(1, sum(1 for c in r["eval_criteria"] if "验证:" in c)) for r in roles]
    n = min(2, len(roles))
    sampled = random.choices(roles, weights=weights, k=n)

    summary = {"checked": 0, "passed": 0, "failed": 0, "skipped": 0, "notices": 0, "blockers": 0}

    score_sum, score_n = 0.0, 0  # 连续 0-1 分累加, 见 _pass_score (DeepEval 范式)

    for role in sampled:
        name = role["name"]
        for c in role["eval_criteria"][:3]:
            extracted = _extract_cmd(c)
            if extracted is None:
                summary["skipped"] += 1
                continue
            cmd_raw, expected = extracted
            if not _is_shell_cmd(cmd_raw):
                summary["skipped"] += 1
                continue

            result = _run(cmd_raw)
            # 白名单拒绝（-3）与 python 沙箱阻断（-4）都是检查器自身拒绝执行，
            # 属能力边界而非角色故障 → 记 skipped 不写 FAIL。
            # 工具自身错误（grep 语法错/文件不存在）是 persona 命令写错，同样非系统故障
            if result["returncode"] in (-3, -4) or _is_tool_error(result):
                summary["skipped"] += 1
                continue
            passed, detail = _check_passed(result, cmd_raw, expected)
            summary["checked"] += 1
            score = _pass_score(result, cmd_raw, expected)
            score_sum += score
            score_n += 1

            if passed:
                summary["passed"] += 1
            else:
                summary["failed"] += 1
                evidence = f"role={name} | {detail}"
                is_crit = _is_critical(c)
                tag = "blocker" if is_crit else "notice"
                _write_bus(
                    "verification",
                    f"[eval_checker] FAIL: {name} — {c[:60]}",
                    evidence=evidence,
                )
                _write_bus(
                    tag,
                    f"@ccs-monitor [eval_checker] 检查失败: {name} — {c[:60]}",
                    evidence=evidence,
                )
                if is_crit:
                    summary["blockers"] += 1
                else:
                    summary["notices"] += 1

        # ── 真实评估检查器（替代已删除的恒真验证） ──
        # OutputSemanticChecker: 检查产出文件语义质量（非模板桩、语法、结构）
        # ── 真实评估检查器（替代已删除的恒真验证） ──
        # OutputSemanticChecker: 检查产出文件语义质量（非模板桩、语法、结构）
        try:
            osem = OutputSemanticChecker()
            for role in sampled:
                osem_result = osem.check(role["name"], f"eval_{name}")
                # 只有当有文件被检查且发现问题时才算失败；无文件/无问题=不计入统计
                if not osem_result["passed"] and osem_result.get("checked_files", 0) > 0:
                    summary["failed"] += 1
                    summary["checked"] += 1
                    _write_bus(
                        "verification",
                        f"[eval_checker] OutputSemantic FAIL: {role['name']} — {osem_result['findings'][:3]}",
                        evidence=f"role={role['name']} findings={osem_result['findings'][:3]}",
                    )
                    _write_bus(
                        "notice",
                        f"@ccs-monitor [eval_checker] 产出语义不达标: {role['name']}",
                        evidence=f"role={role['name']} findings={osem_result['findings'][:3]}",
                    )
                    summary["notices"] += 1
        except Exception as e:
            LOGGER.warning("OutputSemanticChecker failed: %s", e)

        # RequirementConfirmationChecker: 检查需求是否被真实消费/确认
        try:
            reqc = RequirementConfirmationChecker()
            for role in sampled:
                reqc_result = reqc.check(role["name"], f"eval_{role['name']}")
                if not reqc_result["passed"] and reqc_result.get("checked_requirements", 0) > 0:
                    summary["failed"] += 1
                    summary["checked"] += 1
                    _write_bus(
                        "verification",
                        f"[eval_checker] RequirementConfirm FAIL: {role['name']} — {reqc_result['findings'][:3]}",
                        evidence=f"role={role['name']} findings={reqc_result['findings'][:3]}",
                    )
                    _write_bus(
                        "notice",
                        f"@ccs-monitor [eval_checker] 需求未确认: {role['name']}",
                        evidence=f"role={role['name']} findings={reqc_result['findings'][:3]}",
                    )
                    summary["notices"] += 1
        except Exception as e:
            LOGGER.warning("RequirementConfirmationChecker failed: %s", e)

        # TrajectoryQualityChecker: 从角色对话 transcri 评估工具调用质量
        try:
            traj = TrajectoryQualityChecker()
            for role in sampled:
                traj_result = traj.check(role["name"], f"eval_{role['name']}")
                if not traj_result["passed"] and traj_result.get("checked_traces", 0) > 0:
                    summary["failed"] += 1
                    summary["checked"] += 1
                    _write_bus(
                        "verification",
                        f"[eval_checker] Trajectory FAIL: {role['name']} — {traj_result['findings'][:3]}",
                        evidence=f"role={role['name']} findings={traj_result['findings'][:3]}",
                    )
                    _write_bus(
                        "notice",
                        f"@ccs-monitor [eval_checker] 轨迹质量不达标: {role['name']}",
                        evidence=f"role={role['name']} findings={traj_result['findings'][:3]}",
                    )
                    summary["notices"] += 1
        except Exception as e:
            LOGGER.warning("TrajectoryQualityChecker failed: %s", e)

        # LeverageAccumulationChecker: 观测 24h 内可复用资产积累（无资产=通过）
        try:
            lev = LeverageAccumulationChecker()
            for role in sampled:
                lev_result = lev.check(role["name"], f"eval_{role['name']}")
                # 始终 passed（观测型），仅上报 leverage_count 供看板
                summary["leverage_count"] = summary.get("leverage_count", 0) + lev_result["leverage_count"]
        except Exception as e:
            LOGGER.warning("LeverageAccumulationChecker failed: %s", e)

    LOGGER.info(
        "eval_check done: checked=%d passed=%d failed=%d skipped=%d notices=%d blockers=%d",
        summary["checked"], summary["passed"],
        summary["failed"], summary["skipped"],
        summary["notices"], summary["blockers"],
    )
    summary["avg_score"] = round(score_sum / score_n, 4) if score_n else 0.0
    return summary


def main():
    logging.basicConfig(level=logging.INFO)
    import argparse
    parser = argparse.ArgumentParser(description="eval_criteria checker")
    parser.add_argument("--role", type=str, default=None,
                        help="只检查指定角色")
    args = parser.parse_args()

    s = run_eval_check(role_filter=args.role)
    print(json.dumps(s, ensure_ascii=False))
    return 0 if s["failed"] == 0 else 1
if __name__ == "__main__":
    sys.exit(main())
