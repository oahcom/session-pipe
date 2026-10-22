"""doccheck 的 xxx.py:NNN / xxx.py:符号 引用校验（R150 工具化）。

覆盖的是"机器能确定"的判据：越界、空行、文件不存在、符号是否存在（按 AST，
不是正则 —— 正则会把注释和字符串当成定义）。以及必须跳过的两类：历史快照
（ISSUES.md/CHANGES.md）与跨仓引用（带目录但本仓没有）。
"""
import importlib.util
from pathlib import Path


def _load():
    p = Path(__file__).resolve().parent.parent / "scripts" / "doccheck.py"
    spec = importlib.util.spec_from_file_location("doccheck", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


dc = _load()


def _repo(tmp_path: Path, body: str = "x = 1\n") -> Path:
    src = tmp_path / "src"
    src.mkdir()
    (src / "mod.py").write_text(body, encoding="utf-8")
    return tmp_path


def test_cite_past_eof_is_flagged(tmp_path, capsys):
    root = _repo(tmp_path)  # 只有 1 行
    assert dc._cite_check("见 mod.py:9", root, Path("X.md")) == 1
    assert "CITE_PAST_EOF" in capsys.readouterr().out


def test_cite_blank_line_is_flagged(tmp_path, capsys):
    root = _repo(tmp_path, "x = 1\n\ny = 2\n")
    assert dc._cite_check("见 mod.py:2", root, Path("X.md")) == 1
    assert "CITE_BLANK_LINE" in capsys.readouterr().out


def test_cite_valid_line_passes(tmp_path):
    root = _repo(tmp_path, "x = 1\ny = 2\n")
    assert dc._cite_check("见 mod.py:2", root, Path("X.md")) == 0


def test_fenced_examples_are_ignored(tmp_path):
    """代码块里的 file.py:NNN 是示例，不是引用 —— 否则示例本身把门禁顶红。"""
    root = _repo(tmp_path)
    text = "示例：\n```sh\npython3 -c \"import mod\"  # mod.py:99\n```\n"
    assert dc._cite_check(text, root, Path("X.md")) == 0
    assert dc._sym_check(text, root, Path("X.md")) == 0


def test_bare_unknown_file_is_flagged_but_cross_repo_is_skipped(tmp_path, capsys):
    root = _repo(tmp_path)
    assert dc._cite_check("见 nowhere.py:99", root, Path("X.md")) == 1
    assert "CITE_MISSING_FILE" in capsys.readouterr().out
    assert dc._cite_check("见 other/repo/mod.py:99", root, Path("X.md")) == 0


def test_history_files_are_skipped(tmp_path):
    root = _repo(tmp_path)
    assert dc._cite_check("见 mod.py:99", root, Path("ISSUES.md")) == 0
    assert dc._sym_check("见 mod.py:nothing", root, Path("CHANGES.md")) == 0


def test_sym_uses_ast_not_regex(tmp_path, capsys):
    """注释/字符串里的名字不算定义；re-export 的导入名算定义。"""
    body = "# delta = 1\n\"delta = 2\"\n\nfrom os import path as delta\n\nclass Beta:\n    pass\n\nGAMMA = 1\n"
    root = _repo(tmp_path, body)
    assert dc._sym_check("mod.py:Beta mod.py:GAMMA mod.py:delta", root, Path("X.md")) == 0
    assert dc._sym_check("mod.py:epsilon", root, Path("X.md")) == 1
    assert "SYMBOL_MISSING mod.py:epsilon" in capsys.readouterr().out
