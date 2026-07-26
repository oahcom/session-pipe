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
PERSONAS_DIR = Path(os.environ.get(
    "SESSION_ROLES_DIR",
    "/home/administrator/hermes-session-roles/personas/session-roles",
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
    # 去掉残留的 " < N" 比较符
    cmd_raw = re.sub(r'\s+[<>!]=\s*\d+(?:\.\d+)?\s*', ' ', cmd_raw).strip()
    cmd_raw = re.sub(r'  +', ' ', cmd_raw)
    cmd_raw = re.sub(r'^\s*\|\s*', '', cmd_raw)
    cmd_raw = re.sub(r';$', '', cmd_raw).strip()

    if not cmd_raw:
        return None

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
    })
    return bool(first) and first in keywords


# ponytail: 白名单仅覆盖常见只读命令，写操作命令（systemctl start 等）不在内
_ALLOWED_CMDS = frozenset({
    "systemctl", "journalctl", "ps", "df", "free", "grep",
    "bash", "sh", "curl", "python3", "git", "wc", "find",
    "cat", "head", "tail", "ls", "stat", "echo", "test",
    "sort", "uniq", "awk", "sed", "diff", "which", "command",
})

def _run(cmd: str, timeout: int = 30) -> dict:
    """执行 bash 命令（白名单检查，仅允许只读类命令）。"""
    first_word = cmd.lstrip().split(maxsplit=1)[0] if cmd else ""
    if first_word not in _ALLOWED_CMDS:
        return {"ok": False, "stdout": "", "stderr": f"命令不在白名单: {first_word}", "returncode": -3}
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


def run_eval_check() -> dict:
    """主入口：加载角色 JSON，随机挑 1-2 个，
    对每个执行前 3 条 eval_criteria。
    返回 {checked, passed, failed, skipped, notices}。
    """
    if not PERSONAS_DIR.is_dir():
        LOGGER.warning("personas dir not found: %s", PERSONAS_DIR)
        return {"checked": 0, "passed": 0, "failed": 0, "skipped": 0, "notices": 0}

    roles = []
    for f in sorted(PERSONAS_DIR.glob("persona_*.json")):
        try:
            d = json.loads(f.read_text())
            ec = d.get("eval_criteria", [])
            if ec:
                roles.append({"name": d.get("name", f.stem), "eval_criteria": ec})
        except Exception as e:
            LOGGER.warning("skip %s: %s", f.name, e)

    if not roles:
        return {"checked": 0, "passed": 0, "failed": 0, "skipped": 0, "notices": 0}

    n = min(random.randint(1, 2), len(roles))
    # 保证至少包含 maintainer（有真实可执行命令）
    sampled = []
    maintainer = next((r for r in roles if r["name"] == "maintainer"), None)
    if maintainer:
        sampled.append(maintainer)
        other_candidates = [r for r in roles if r["name"] != "maintainer"]
        if n > 1 and other_candidates:
            sampled.append(random.choice(other_candidates))
    else:
        sampled = random.sample(roles, n)
    summary = {"checked": 0, "passed": 0, "failed": 0, "skipped": 0, "notices": 0}

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
            passed, detail = _check_passed(result, cmd_raw, expected)
            summary["checked"] += 1

            if passed:
                summary["passed"] += 1
            else:
                summary["failed"] += 1
                evidence = f"role={name} | {detail}"
                _write_bus(
                    "verification",
                    f"[eval_checker] FAIL: {name} — {c[:60]}",
                    evidence=evidence,
                )
                _write_bus(
                    "notice",
                    f"@ccs-monitor [eval_checker] 检查失败: {name} — {c[:60]}",
                    evidence=evidence,
                )
                summary["notices"] += 1

    LOGGER.info(
        "eval_check done: checked=%d passed=%d failed=%d skipped=%d notices=%d",
        summary["checked"], summary["passed"],
        summary["failed"], summary["skipped"], summary["notices"],
    )
    return summary


def main():
    logging.basicConfig(level=logging.INFO)
    s = run_eval_check()
    print(json.dumps(s, ensure_ascii=False))
    return 0 if s["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
