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
TREE = re.compile(r'([\w/]+\.py)(\s*[─-]+.*?（)(\d+)( 行）)')
# 表格两种写法都认：| **path** | 895 | 与 | **`path`** | 895 |
TABLE = re.compile(r'(\|\s*\*\*`?)([\w/]+\.py)(`?\*\*\s*\|\s*)(\d+)(\s*\|)')


def _resolve(name: str) -> Path | None:
    """doc 里的相对路径 → 真实文件。先整段匹配，再退到 basename 唯一匹配。"""
    direct = BASE / "src" / name
    if direct.exists():
        return direct
    for c in (BASE / "src").rglob(os.path.basename(name)):
        return c
    return None


def _real_n(p: Path) -> int:
    return open(p, "rb").read().count(b"\n")


def _rows(text: str):
    """产出 (文件名, 数字在文本中的区间) —— 两种写法统一处理。"""
    for m in TREE.finditer(text):
        yield m.group(1), m.start(3), m.end(3)
    for m in TABLE.finditer(text):
        yield m.group(2), m.start(4), m.end(4)


def _check(path: Path) -> int:
    bad = 0
    text = path.read_text(encoding="utf-8")
    for name, s, e in _rows(text):
        real_p = _resolve(name)
        if not real_p:
            print(f"MISSING_FILE {name}")
            bad += 1
            continue
        real = _real_n(real_p)
        if int(text[s:e]) != real:
            print(f"MISMATCH {name} doc={text[s:e]} real={real} ({path})")
            bad += 1
    return bad


def _fix(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    edits = []
    for name, s, e in _rows(text):
        real_p = _resolve(name)
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
    args = ap.parse_args(argv)
    targets = args.targets or ["CLAUDE.md", "docs/ARCHITECTURE.md"]
    bad = 0
    for t in targets:
        p = BASE / t
        if not p.exists():
            print(f"MISSING {t}")
            bad += 1
            continue
        if args.fix:
            print(f"{p}: 已修正 {_fix(p)} 处行数声明")
        else:
            bad += _check(p)
    print(f"\n{'CLEAN' if bad == 0 else f'{bad} mismatch(es)'}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
