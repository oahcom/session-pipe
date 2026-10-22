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
import ast
import os
import re
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
SIBLINGS = ("session-launcher", "session-pipeline", "hermes-session-roles")


def _outside(name: str) -> bool:
    """三仓之外的文件（`~/.hermes/...`、绝对路径）不校验也报错。

    这类引用指向的代码不在这三个仓里，判据管不着；但本仓/兄弟仓都找不到的
    裸名或相对路径必须报错 —— 否则拼错的路径永远绿（第二版审查的批评）。
    """
    return name.startswith("~")  # 只有 `~/` 一定在三仓之外；绝对路径先试着按仓内解析
TREE = re.compile(r'([\w/]+\.py)(\s*[─-]+.*?（)(\d+)(\s*行)')
# 表格两种写法都认：| **path** | 895 | 与 | **`path`** | 895 |
TABLE = re.compile(r'(\|\s*\*\*)(`?)([\w/]+\.py)\2(\*\*\s*\|\s*)(\d+)(\s*\|)')


def _resolve(name: str, root: Path) -> Path | None:
    """doc 里的相对路径 → 真实文件。先整段匹配，再退到 basename 唯一匹配。

    basename 命中多个时判为歧义返回 None：否则已删的 `pipeflow/db.py` 会被
    `workflow/db.py` 顶替，模块没了也校验通过（实测 2026-10-22）。
    """
    def inside(base: Path, repo: Path) -> bool:
        # `../x.py:1` 这种引用能逃出仓库目录 —— 落在仓库外的文件不校验也不算命中
        try:
            return base.resolve().is_relative_to(repo.resolve())
        except (OSError, ValueError):
            return False

    for base in (root / name, root / "src" / name):
        if base.exists() and inside(base, root):
            return base
    head, _, rest = name.partition("/")
    if head in SIBLINGS:  # 显式跨仓路径 session-pipeline/src/x.py 是可校验的，别当跨仓丢掉
        repo = root.parent / head
        for base in (root.parent / name, repo / "src" / rest):
            if base.exists() and inside(base, repo):
                return base
        return None
    if name.startswith("/"):  # 绝对路径：落在三仓里的按仓内相对路径处理
        for repo in (root,) + tuple(root.parent / n for n in SIBLINGS if n != root.name):
            try:
                rel = Path(name).relative_to(repo)
            except ValueError:
                continue
            for base in (repo / rel, repo / "src" / rel):
                if base.exists():
                    return base
        return None
    if "/" in name:
        return None  # 带目录的引用要么本仓相对路径，要么跨仓；都不许退到 basename 猜
    cands = [q for q in root.rglob(os.path.basename(name)) if ".git" not in q.parts]
    if len(cands) > 1:
        return None  # 本仓就有多个同名 → 歧义，不许拿兄弟仓消歧（第七轮审查的批评）
    if len(cands) == 1:
        return cands[0]
    # 跨仓裸名：launcher 的 PUBLIC_API 会写 pipeline 的 `manager.py:414`。只认兄弟仓里
    # 唯一的同名文件 —— 否则 "本仓烂引用" 与 "别人的文件" 分不开，门禁会天天假红。
    # 只认三仓本身，不扫 worktree：/home/administrator 下还有几十个 wt-*/ 工作树，
    # 扫进去同名文件必然多个 → 全判歧义 → 门禁又变成假红。
    sibs = [q for n in SIBLINGS if n != root.name and (root.parent / n / "src").is_dir()
            for q in (root.parent / n / "src").rglob(os.path.basename(name))]
    return sibs[0] if len(sibs) == 1 else None


FENCE = "```"


def _strip_fences(text: str) -> str:
    """去掉围栏代码块：里面的 file.py:123 是示例（命令、粘贴的日志），不是引用。

    不这么做，一段示例代码就能让校验永久变红，或者逼着人把示例改歪。
    """
    out, fence = [], False
    for line in text.splitlines():
        if line.lstrip().startswith(FENCE):
            fence = not fence
            out.append("")
            continue
        out.append("" if fence else line)
    return "\n".join(out)


def _symbols(path: Path) -> tuple[set[str], set[str]] | None:
    """文件里的名字，分两层：

    public —— 模块级 def/class/赋值/import，以及类里的方法与类属性（文档引用的都是这层）；
    every  —— 全部绑定，含函数内的局部变量。

    为什么要分两层：局部变量同名会掩盖"模块级符号已被删"。文档引用局部变量几乎总是写错了，
    所以只出现在 every 里时要单独报出来（SYMBOL_LOCAL_ONLY），不能当通过。

    用 AST 而不是正则：正则会把注释、字符串当成定义。解析失败返回 None，调用方跳过
    （不拿正则兜底，否则又回到误判）。
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
    except SyntaxError:
        return None
    public: set[str] = set()
    every: set[str] = set()

    def binds(target) -> set[str]:
        return {n.id for n in ast.walk(target)
                if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)}

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            every.add(node.name)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            every.add(node.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            every.update((a.asname or a.name.split(".")[0]) for a in node.names)

    for node in tree.body:  # 模块级
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            public.add(node.name)
        elif isinstance(node, ast.Assign):
            public.update(t.id for t in node.targets if isinstance(t, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            public.add(node.target.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            public.update((a.asname or a.name.split(".")[0]) for a in node.names)
        elif isinstance(node, (ast.For, ast.With)):
            public.update(binds(node.target))

    for node in ast.walk(tree):  # 类体里的方法与类属性
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    public.add(item.name)
                elif isinstance(item, ast.Assign):
                    public.update(t.id for t in item.targets if isinstance(t, ast.Name))
                elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                    public.add(item.target.id)
    return public, every


CITE = re.compile(r"([~\w./-]+\.py):(\d+)")


def _cite_check(text: str, root: Path, path: Path = Path()) -> int:
    """校验 doc 里的 xxx.py:NNN 引用：文件能定位、行号未越界、那一行不是空行。

    只做机器能确定的三件事。不猜"这行是不是讲这个符号"——符号会合法地挪到相邻行，
    靠文本相似度判断只会制造假失败（本条是 R149 审查的教训）。定位不到的文件
    （跨仓引用、已移出本仓的模块）跳过：判据不能要求它做不到的前置条件。
    """
    if path.name in {"ISSUES.md", "CHANGES.md"}:
        return 0  # 这两份是历史快照，行号引用按建单时记，本来就该指不到今天
    bad = 0
    for m in CITE.finditer(_strip_fences(text)):
        name, ln = m.group(1), int(m.group(2))
        p = _resolve(name, root)
        if p is None:
            if not _outside(name):
                print(f"CITE_MISSING_FILE {name}")  # 三仓都找不到 = 烂引用
                bad += 1
            continue
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
        if ln > len(lines):
            print(f"CITE_PAST_EOF {name}:{ln} (file {len(lines)} lines)")
            bad += 1
        elif not lines[ln - 1].strip():
            print(f"CITE_BLANK_LINE {name}:{ln}")
            bad += 1
    return bad


SYM = re.compile(r"([~\w./-]+\.py):([A-Za-z_]\w*)")


def _sym_check(text: str, root: Path, path: Path) -> int:
    """校验 xxx.py:符号 引用：该符号在文件里还有 def/class/赋值。

    这是比行号更耐腐的写法（行号会随插入代码整体平移，符号只在改名时才失效，
    而改名本身是可见事件）。文档写行号时用 _cite_check 兜底，写符号时用这里。
    """
    if path.name in {"ISSUES.md", "CHANGES.md"}:
        return 0
    bad = 0
    for m in SYM.finditer(_strip_fences(text)):
        name, sym = m.group(1), m.group(2)
        p = _resolve(name, root)
        if p is None:
            if not _outside(name):
                print(f"SYMBOL_MISSING_FILE {name}")
                bad += 1
            continue
        syms = _symbols(p)
        if syms is None:
            continue  # 解析不了（非 py 语法）就不下结论
        public, every = syms
        if sym in public:
            continue
        if sym in every:
            print(f"SYMBOL_LOCAL_ONLY {name}:{sym}")  # 只在函数内绑定过：文档引用错了
            bad += 1
            continue
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
            # 修完行数仍要跑一遍引用检查：否则 --fix 会对着越界的 xxx.py:NNN 说 CLEAN，
            # 把"刷新"变成"刷新并宣布没问题"（第二版审查的 P1）。
            print(f"{p}: 已修正 {_fix(p, root)} 处行数声明")
        bad += _check(p, root)
    print(f"\n{'CLEAN' if bad == 0 else f'{bad} mismatch(es)'}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
