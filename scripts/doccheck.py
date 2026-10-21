#!/usr/bin/env python3
"""doccheck — 校验 CLAUDE.md / ARCHITECTURE.md 中声明的行数与真实 wc -l 是否一致。
沉淀自 "doc-drift-pipeline-launcher" 需求：把文档数字对齐纪律变可复用检查。

用法：
  python3 scripts/doccheck.py               # 检查 pipeline 项目
  python3 scripts/doccheck.py ../session-launcher/CLAUDE.md

输出：每行 "MISMATCH|MISSING|OK <file> doc=<n> real=<n>"，exit 非 0 若有不一致。
"""
import re, sys, os
from pathlib import Path

# doc 中的相对路径 → 真实 path（doc 写的是 src/<basename> 或嵌套）
def _resolve(base: Path, name: str) -> Path | None:
    base = base / "src"
    # 1. 优先完整相对路径匹配（如 pipeflow/db.py → src/pipeflow/db.py）
    #    解决同名文件冲突（pipeflow/db.py=327 vs workflow/db.py=96）
    direct = base / name
    if direct.exists():
        return direct
    # 2. 回退：仅 basename 匹配（向后兼容旧文档写法 src/<basename>）
    for c in base.rglob(os.path.basename(name)):
        return c
    return None

def _collect(base: Path, text: str):
    pat = re.compile(r'((?:[\w/]+\.py))\s*[─-]+\s*(?:.*?（(\d+) 行）)?')
    table = re.compile(r'\|\s*\*\*`(pipeflow/[\w_]+\.py|[\w_]+\.py)`\s*\|\s*(\d+)\s*\|')
    rows = []
    for m in pat.finditer(text):
        f, n = m.group(1), m.group(2)
        if n:
            rows.append((f, int(n)))
    for m in table.finditer(text):
        f, n = m.group(1), int(m.group(2))
        rows.append((f, n))
    return rows

def main():
    base = Path(__file__).resolve().parent.parent  # scripts/ → 项目根
    targets = sys.argv[1:] or [
        str(base / "CLAUDE.md"), str(base / "docs" / "ARCHITECTURE.md"),
    ]
    bad = 0
    for t in targets:
        p = base / t
        if not p.exists():
            print(f"MISSING {t}"); bad += 1; continue
        for f, doc_n in _collect(base, p.read_text()):
            real_p = _resolve(base, f)
            if not real_p:
                print(f"MISSING_FILE {f}"); bad += 1; continue
            # 用 wc -l 语义（仅计数换行符）与文档保持一致，
            # 避免末行无换行时 open() 计数比 wc 多 1 的误判
            raw = open(real_p, "rb").read()
            real_n = raw.count(b"\n")  # wc -l 精确语义：仅计换行符
            if real_n == doc_n:
                pass  # ok
            else:
                print(f"MISMATCH {f} doc={doc_n} real={real_n} ({t})"); bad += 1
    print(f"\n{'CLEAN' if bad == 0 else f'{bad} mismatch(es)'}")
    return 1 if bad else 0

if __name__ == "__main__":
    raise SystemExit(main())
