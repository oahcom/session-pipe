"""REF-GUARD：提交里删掉的 def/class 名若仍被代码引用，就拒绝提交。

只读暂存区/HEAD，由 .githooks/pre-commit 调用；ALLOW_DANGLING_REF=1 可绕过。
背景：2026-10-22 一天内 main 红过 4 次，都是整文件覆盖或删死码时把还在被调用的符号
删掉（ef2a249 抹掉 own_written 实现；74bf13c role_lifecycle 缺 pane_cli_busy，导入即崩）。
"""
from __future__ import annotations

import ast
import re
import subprocess
import sys


def _defs(src: str) -> set[str]:
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return set()
    return {n.name for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}


def _refs(src: str) -> set[str]:
    """代码引用。注释/文档里提到旧名字不算——本仓大量存在，grep 版因此误报。"""
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return set()
    out: set[str] = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Name):
            out.add(n.id)
        elif isinstance(n, ast.Attribute):
            out.add(n.attr)
        elif isinstance(n, ast.alias):
            # from m import gone as g → gone 与 g 都算引用，否则漏报被删的 gone
            out.add(n.name.split(".")[-1])
            if n.asname:
                out.add(n.asname.split(".")[-1])
    return out


def _imported_from(src: str) -> dict[str, str]:
    """本文件里的名字 → 它从哪个模块导入。用于区分「定义搬家」与「符号真丢了」。"""
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return {}
    out: dict[str, str] = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module:
            for a in n.names:
                out.setdefault(a.name, n.module)
                if a.asname:
                    out.setdefault(a.asname, n.module)
    return out


def dangling_refs(old: dict[str, str], new: dict[str, str]) -> dict[str, set[str]]:
    """old/new: 路径 → 源码。返回 {被删符号: {仍引用它的文件}}。

    改签名/加装饰器不算删除；定义搬到别的文件且调用方同步改了 import 也不算。
    """
    gone: set[str] = set()
    for path, src in old.items():
        gone |= _defs(src) - _defs(new.get(path, ""))
    if not gone:
        return {}
    # 定义搬家：同名函数在别的文件里仍有定义，且引用方 import 的正是那个模块。
    # 必须按完整点路径解析——只比 basename 会让 p1/util.py 与 p2/util.py 互相顶替（codex 复审实测）。
    defined_in = {path[:-3].replace("/", "."): _defs(src)
                  for path, src in new.items() if path.endswith(".py")}
    hits: dict[str, set[str]] = {}
    for path, src in new.items():
        imported = _imported_from(src)
        for name in _refs(src) & gone:
            mod = imported.get(name)
            if mod and any(dotted.endswith(mod) and name in defs
                           for dotted, defs in defined_in.items()):
                continue  # 引用方已改 import 指向新定义处
            hits.setdefault(name, set()).add(path)
    return hits


def _git(*args: str) -> str:
    p = subprocess.run(["git", *args], capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or ("git " + args[0] + " 失败"))
    return p.stdout


def _blobs(shas: dict[str, str]) -> dict[str, str]:
    """按 sha 取内容：一次 cat-file --batch 拿全部。按路径取会撞上符号链接/子模块报错。"""
    if not shas:
        return {}
    spec = "".join(sha + "\n" for sha in shas.values()).encode()
    p = subprocess.run(["git", "cat-file", "--batch"], input=spec, capture_output=True)
    out, i, res = p.stdout, 0, {}
    for path, sha in shas.items():
        nl = out.find(b"\n", i)
        if nl < 0:
            break
        head = out[i:nl].split()
        i = nl + 1
        if len(head) != 3 or not head[2].isdigit():
            continue
        size = int(head[2])
        res[path] = out[i:i + size].decode("utf-8", "replace")
        i += size + 1
    return res


def _index_shas() -> dict[str, str]:
    out = _git("ls-files", "-s", "-z", "*.py")
    shas: dict[str, str] = {}
    for rec in out.split("\0"):
        if not rec:
            continue
        meta, _, path = rec.partition("\t")
        parts = meta.split()
        if len(parts) == 3 and parts[2] == "0":
            shas[path] = parts[1]
    return shas


def _head_shas() -> dict[str, str]:
    try:
        out = _git("ls-tree", "-r", "-z", "HEAD")
    except RuntimeError:
        return {}  # 无提交（新仓首个 commit）
    shas: dict[str, str] = {}
    for rec in out.split("\0"):
        if not rec:
            continue
        meta, _, path = rec.partition("\t")
        parts = meta.split()
        if len(parts) == 3 and path.endswith(".py"):
            shas[path] = parts[2]
    return shas


def _staged_sources() -> tuple[dict[str, str], dict[str, str]]:
    """(旧=HEAD 版, 新=暂存区版)。旧版只取「删了 def/class 行」的文件，省一次全量解析。"""
    diff = _git("diff", "--cached", "-U0", "--", "*.py")
    touched: set[str] = set()
    path = ""
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            path = line[6:]
        elif path and re.match(r"-\s*(?:async\s+)?(?:def|class)\s+\w+", line):
            touched.add(path)
    if not touched:
        return {}, {}
    head = _head_shas()
    old = _blobs({p: head[p] for p in touched if p in head})
    new = _blobs(_index_shas())
    bad = [p for p, src in list(old.items()) + list(new.items())
           if not _parses(src)]
    if bad:
        # 解析不了就只能当"没这个文件"：说明守卫对它们失明，必须喊出来而不是静默放行
        print("REF-GUARD 警告: 这些文件语法不完整，本次不参与判定 → " + ", ".join(sorted(bad)),
              file=sys.stderr)
    return old, new


def _parses(src: str) -> bool:
    try:
        ast.parse(src)
    except SyntaxError:
        return False
    return True


def main() -> int:
    try:
        old, new = _staged_sources()
    except RuntimeError as e:
        print("REF-GUARD 无法读取暂存区（fail-closed）: " + str(e), file=sys.stderr)
        return 1
    hits = dangling_refs(old, new)
    if not hits:
        return 0
    print("REF-GUARD 阻止提交: 删掉的符号还有代码引用", file=sys.stderr)
    for name, files in sorted(hits.items()):
        print("  " + name + " ← " + ", ".join(sorted(files)), file=sys.stderr)
    print("  要么连调用点一起删/改，要么别删。确需保留设 ALLOW_DANGLING_REF=1。", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
