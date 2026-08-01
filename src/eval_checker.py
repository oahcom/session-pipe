#!/usr/bin/env python3
"""
eval_checker.py — eval_criteria 运行时执行器。

定期随机挑 1-2 个角色的 eval_criteria，解析"验证:"后面的 bash 命令并执行。
纯文本描述（无可执行命令）跳过。失败写 bus (cat=verification + notice @ccs-monitor)。

集成到 routing_daemon.py main loop:
    from eval_checker import run_eval_check
    run_eval_check()  # 每 300s 调一次
"""

import json, logging, os, random, re, subprocess, sys, time
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


def _run(cmd: str, timeout: int = 30) -> dict:
    """执行 bash 命令（白名单检查，仅允许只读类命令）。"""
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
                return {"ok": False, "stdout": "", "stderr": f"管道/分号/命令替换被禁止: 含 ; 且段含非白名单命令", "returncode": -3}
        else:
            return {"ok": False, "stdout": "", "stderr": f"管道/分号/命令替换被禁止: 含 ;", "returncode": -3}
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


def _load_roles() -> list | None:
    """加载所有角色 JSON，返回含 eval_criteria 的角色列表。"""
    if not PERSONAS_DIR.is_dir():
        LOGGER.warning("personas dir not found: %s", PERSONAS_DIR)
        return None
    roles = []
    for f in sorted(PERSONAS_DIR.glob("persona_*.json")):
        try:
            d = json.loads(f.read_text())
            ec = d.get("eval_criteria", [])
            if ec:
                roles.append({"name": d.get("name", f.stem), "eval_criteria": ec})
        except Exception as e:
            LOGGER.warning("skip %s: %s", f.name, e)
    return roles


def _find_unverified(roles: list) -> list:
    """返回所有 criterion 均为自然语言（无可执行命令）的角色。"""
    return [r for r in roles if not any("验证:" in c for c in r["eval_criteria"])]


def run_eval_check(role_filter: str | None = None) -> dict:
    """主入口：加载角色 JSON，按权重轮询挑 1-2 个，
    对每个执行前 3 条 eval_criteria。
    权重 = 该角色可执行 eval 数量（含"验证:"的 criterion 数），最少 1。
    参数 role_filter: 只检查指定角色名。
    返回 {checked, passed, failed, skipped, notices, blockers}。
    """
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

    # 权重 = 可执行 eval 数量（含"验证:"的 criterion 数），最少 1
    weights = [max(1, sum(1 for c in r["eval_criteria"] if "验证:" in c)) for r in roles]
    n = min(2, len(roles))
    sampled = random.choices(roles, weights=weights, k=n)

    summary = {"checked": 0, "passed": 0, "failed": 0, "skipped": 0, "notices": 0, "blockers": 0}

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
            # 白名单拒绝（-3）是检查器能力边界，非系统故障 → 记 skipped 不写 FAIL
            # 工具自身错误（grep 语法错/文件不存在）是 persona 命令写错，同样非系统故障
            if result["returncode"] == -3 or _is_tool_error(result):
                summary["skipped"] += 1
                continue
            passed, detail = _check_passed(result, cmd_raw, expected)
            summary["checked"] += 1

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

    LOGGER.info(
        "eval_check done: checked=%d passed=%d failed=%d skipped=%d notices=%d blockers=%d",
        summary["checked"], summary["passed"],
        summary["failed"], summary["skipped"],
        summary["notices"], summary["blockers"],
    )
    return summary


def precheck_all_personas() -> dict:
    """离线预检: 扫描全部 persona 的 eval_criteria，验证命令提取 + 白名单。

    返回 {roles: [{name, criteria: [{criterion, cmd, extracted_ok, will_run_ok, error}]}], summary: {...}}
    """
    roles = _load_roles()
    if not roles:
        return {"roles": [], "summary": {"total": 0}}
    results = []
    for role in roles:
        items = []
        for c in role.get("eval_criteria", []):
            extracted = _extract_cmd(c)
            if extracted is None:
                items.append({"criterion": c[:80], "cmd": None,
                              "extracted_ok": False, "will_run_ok": False,
                              "error": "extract_cmd 返回 None"})
                continue
            cmd_raw, expected = extracted
            is_shell = _is_shell_cmd(cmd_raw)
            is_tool_err = False
            run_ok = False
            error = None
            if not is_shell:
                error = f"首词不在 _is_shell_cmd 关键字列表: {cmd_raw.split()[0] if cmd_raw else '?'}"
            else:
                r = _run(cmd_raw, timeout=5)
                is_tool_err = _is_tool_error(r)
                # 用 _check_passed 做完整判断（grep 无匹配 exit=1 + expected=0 → PASS）
                if expected and not is_tool_err and r["returncode"] in (0, 1):
                    passed, _detail = _check_passed(r, cmd_raw, expected)
                    run_ok = passed
                else:
                    run_ok = r["ok"]
                if not run_ok and not is_tool_err:
                    error = f"exit={r['returncode']} stderr={r['stderr'][:60]}"
            items.append({"criterion": c[:80], "cmd": cmd_raw,
                          "extracted_ok": True, "will_run_ok": run_ok,
                          "error": error})
        results.append({"name": role["name"], "criteria": items})
    bad_count = sum(1 for r in results for c in r["criteria"]
                    if not c["extracted_ok"] or (c["extracted_ok"] and c["error"]))
    return {"roles": results,
            "summary": {"total": len(results), "bad_criteria": bad_count}}


def main():
    logging.basicConfig(level=logging.INFO)
    import argparse
    parser = argparse.ArgumentParser(description="eval_criteria checker")
    parser.add_argument("--report-unverified", action="store_true",
                        help="输出无可执行 criterion 的角色（全自然语言）")
    parser.add_argument("--precheck", action="store_true",
                        help="离线预检全部 persona 的 eval_criteria 命令")
    parser.add_argument("--role", type=str, default=None,
                        help="只检查指定角色")
    args = parser.parse_args()

    if args.precheck:
        result = precheck_all_personas()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["summary"]["bad_criteria"] == 0 else 1

    if args.report_unverified:
        roles = _load_roles()
        if roles is None:
            return 1
        unverified = _find_unverified(roles)
        if unverified:
            print("未验证角色（全自然语言，无可执行 criterion）:")
            for r in unverified:
                print(f"  - {r['name']}")
        else:
            print("所有角色至少有一个可执行 criterion")
        return 0

    s = run_eval_check(role_filter=args.role)
    print(json.dumps(s, ensure_ascii=False))
    return 0 if s["failed"] == 0 else 1
if __name__ == "__main__":
    sys.exit(main())
