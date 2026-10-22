#!/usr/bin/env python3
"""doccheck — 校验/刷新文档里声明的文件行数（wc -l 语义：只数换行符）。

用法：
  python3 scripts/doccheck.py          # 检查，不一致则 exit 1
  python3 scripts/doccheck.py --fix    # 就地把声明行数改成真实行数
  python3 scripts/doccheck.py ../session-launcher/CLAUDE.md

支持两种写法：
  树形   router.py ── 路由表（376 行）
  表格   | **pipeflow/engine.py** | 895 | 说明 |
"""
import argparse
import os
import re
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
TREE = re.compile(r'([\w/]+\.py)(\s*[─-]+.*?（)(\d+)(\s*行)')
# 表格两种写法都认：| **path** | 895 | 与 | **`path`** | 895 |
TABLE = re.compile(r'(\|\s*\*\*)(`?)([\w/]+\.py)\2(\*\*\s*\|\s*)(\d+)(\s*\|)')


def _resolve(name: str, root: Path) -> Path | None:
    """doc 里的相对路径 → 真实文件。先整段匹配，再退到 basename 唯一匹配。

    basename 命中多个时判为歧义返回 None：否则已删的 `pipeflow/db.py` 会被
    `workflow/db.py` 顶替，模块没了也校验通过（实测 2026-10-22）。
    """
    direct = root / "src" / name
    if direct.exists():
        return direct
    cands = list((root / "src").rglob(os.path.basename(name)))
    return cands[0] if len(cands) == 1 else None


CITE = re.compile(r"([\w./-]+\.py):(\d+)")


def _cite_check(text: str, root: Path, path: Path = Path()) -> int:
    """校验 doc 里的 xxx.py:NNN 引用：文件能定位、行号未越界、那一行不是空行。

    只做机器能确定的三件事。不猜"这行是不是讲这个符号"——符号会合法地挪到相邻行，
    靠文本相似度判断只会制造假失败（本条是 R149 审查的教训）。定位不到的文件
    （跨仓引用、已移出本仓的模块）跳过：判据不能要求它做不到的前置条件。
    """
    if path.name in {"ISSUES.md", "CHANGES.md"}:
        return 0  # 这两份是历史快照，行号引用按建单时记，本来就该指不到今天
    bad = 0
    for m in CITE.finditer(text):
        name, ln = m.group(1), int(m.group(2))
        p = root / name
        if not p.exists():
            cands = list((root / "src").rglob(Path(name).name))
            if len(cands) != 1:
                continue
            p = cands[0]
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
        if ln > len(lines):
            print(f"CITE_PAST_EOF {name}:{ln} (file {len(lines)} lines)")
            bad += 1
        elif not lines[ln - 1].strip():
            print(f"CITE_BLANK_LINE {name}:{ln}")
            bad += 1
    return bad


SYM = re.compile(r"([\w./-]+\.py):([A-Za-z_]\w*)")


def _sym_check(text: str, root: Path, path: Path) -> int:
    """校验 xxx.py:符号 引用：该符号在文件里还有 def/class/赋值。

    这是比行号更耐腐的写法（行号会随插入代码整体平移，符号只在改名时才失效，
    而改名本身是可见事件）。文档写行号时用 _cite_check 兜底，写符号时用这里。
    """
    if path.name in {"ISSUES.md", "CHANGES.md"}:
        return 0
    bad = 0
    for m in SYM.finditer(text):
        name, sym = m.group(1), m.group(2)
        p = root / name
        if not p.exists():
            cands = list((root / "src").rglob(Path(name).name))
            if len(cands) != 1:
                continue
            p = cands[0]
        src = p.read_text(encoding="utf-8", errors="ignore")
        pat = rf"^\s*(?:def|class)\s+{re.escape(sym)}\b|^\s*{re.escape(sym)}\s*[:=]"
        if not re.search(pat, src, re.M):
            print(f"SYMBOL_MISSING {name}:{sym}")
            bad += 1
    return bad


def _real_n(p: Path) -> int:
    return open(p, "rb").read().count(b"\n")


def _rows(text: str):
    """产出 (文件名, 数字在文本中的区间) —— 两种写法统一处理。"""
    for m in TREE.finditer(text):
        yield m.group(1), m.start(3), m.end(3)
    for m in TABLE.finditer(text):
        yield m.group(3), m.start(5), m.end(5)


def _check(path: Path, root: Path) -> int:
    text = path.read_text(encoding="utf-8")
    bad = _cite_check(text, root, path) + _sym_check(text, root, path)
    for name, s, e in _rows(text):
        real_p = _resolve(name, root)
        if not real_p:
            print(f"MISSING_FILE {name}")
            bad += 1
            continue
        real = _real_n(real_p)
        if int(text[s:e]) != real:
            print(f"MISMATCH {name} doc={text[s:e]} real={real} ({path})")
            bad += 1
    return bad


def _fix(path: Path, root: Path) -> int:
    text = path.read_text(encoding="utf-8")
    edits = []
    for name, s, e in _rows(text):
        real_p = _resolve(name, root)
        if not real_p:
            continue
        real = str(_real_n(real_p))
        if text[s:e] != real:
            edits.append((s, e, real))
    for s, e, real in sorted(edits, reverse=True):
        text = text[:s] + real + text[e:]
    if edits:
        path.write_text(text, encoding="utf-8")
    return len(edits)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="校验/刷新文档与代码行数的一致性")
    ap.add_argument("targets", nargs="*", help="文档路径，缺省 CLAUDE.md 与 docs/ARCHITECTURE.md")
    ap.add_argument("--fix", action="store_true", help="把文档里的行数改成真实行数")
    ap.add_argument("--root", default=str(BASE), help="被检仓库根；其他仓复用本脚本时用")
    args = ap.parse_args(argv)
    root = Path(args.root).resolve()
    targets = args.targets or sorted(
        p.relative_to(root).as_posix()
        for p in root.rglob("*.md")
        if "/.git/" not in str(p) and "node_modules" not in str(p)
    )
    bad = 0
    for t in targets:
        p = root / t
        if not p.exists():
            print(f"MISSING {t}")
            bad += 1
            continue
        if args.fix:
            print(f"{p}: 已修正 {_fix(p, root)} 处行数声明")
        else:
            bad += _check(p, root)
    print(f"\n{'CLEAN' if bad == 0 else f'{bad} mismatch(es)'}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
