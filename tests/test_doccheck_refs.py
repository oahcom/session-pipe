"""doccheck 的 xxx.py:NNN / xxx.py:符号 引用校验（R150 工具化）。

只测三条机器能确定的判据：越界、空行、符号不存在；以及必须跳过的情况
（历史快照 / 定位不到的文件），后者乱报会让整条门禁被关掉。
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


def test_unknown_and_history_files_are_skipped(tmp_path):
    root = _repo(tmp_path)
    assert dc._cite_check("见 nowhere.py:99", root, Path("X.md")) == 0
    assert dc._cite_check("见 mod.py:99", root, Path("ISSUES.md")) == 0
    assert dc._sym_check("见 mod.py:nothing", root, Path("CHANGES.md")) == 0


def test_sym_checks_def_class_and_assignment(tmp_path, capsys):
    root = _repo(tmp_path, "def alpha():\n    pass\n\nclass Beta:\n    pass\n\nGAMMA = 1\n")
    ok = "mod.py:alpha mod.py:Beta mod.py:GAMMA"
    assert dc._sym_check(ok, root, Path("X.md")) == 0
    assert dc._sym_check("mod.py:delta", root, Path("X.md")) == 1
    assert "SYMBOL_MISSING mod.py:delta" in capsys.readouterr().out
