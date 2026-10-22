"""doccheck 的 xxx.py:NNN / xxx.py:符号 引用校验（R150 工具化）。

判据只做机器能确定的事：行号越界、目标行空行、文件在三仓里都找不到、符号不存在（AST，
不是正则）。跳过只有两类：历史快照（ISSUES.md/CHANGES.md）与三仓之外的文件（`~/`、绝对路径）。
显式跨仓路径（`session-pipeline/src/x.py`）要真去兄弟仓里找 —— 不然"跨仓"就成了免检通道。
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
    root = tmp_path / "session-launcher"
    (root / "src").mkdir(parents=True)
    (root / "src" / "mod.py").write_text(body, encoding="utf-8")
    return root


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


def test_missing_file_is_flagged_unless_outside_the_three_repos(tmp_path, capsys):
    root = _repo(tmp_path)
    assert dc._cite_check("见 nowhere.py:99", root, Path("X.md")) == 1
    assert "CITE_MISSING_FILE" in capsys.readouterr().out
    assert dc._cite_check("见 other/repo/mod.py:99", root, Path("X.md")) == 1  # 拼错的相对路径
    assert dc._cite_check("见 ~/.hermes/scripts/x.py:9", root, Path("X.md")) == 0  # 仓外，管不着


def test_explicit_sibling_repo_path_is_resolved(tmp_path):
    root = _repo(tmp_path)
    sib = tmp_path / "session-pipeline" / "src" / "lifecycle"
    sib.mkdir(parents=True)
    (sib / "manager.py").write_text("def confirm_step():\n    pass\n", encoding="utf-8")
    ref = "见 session-pipeline/src/lifecycle/manager.py:confirm_step"
    assert dc._sym_check(ref, root, Path("X.md")) == 0
    assert dc._sym_check("见 session-pipeline/src/lifecycle/manager.py:nope", root, Path("X.md")) == 1


def test_history_files_are_skipped(tmp_path):
    root = _repo(tmp_path)
    assert dc._cite_check("见 mod.py:99", root, Path("ISSUES.md")) == 0
    assert dc._sym_check("见 mod.py:nothing", root, Path("CHANGES.md")) == 0


def test_sym_uses_ast_not_regex(tmp_path, capsys):
    """注释/字符串里的名字不算定义；解包绑定、re-export 的导入名算定义。"""
    body = ("# delta = 1\n\"delta = 2\"\n\nfrom os import path as delta\n\n"
            "class Beta:\n    pass\n\nGAMMA = 1\n\nfor epsilon, zeta in []:\n    pass\n")
    root = _repo(tmp_path, body)
    assert dc._sym_check("mod.py:Beta mod.py:GAMMA mod.py:delta mod.py:epsilon", root, Path("X.md")) == 0
    assert dc._sym_check("mod.py:eta", root, Path("X.md")) == 1
    assert "SYMBOL_MISSING mod.py:eta" in capsys.readouterr().out


def test_local_only_binding_is_flagged(tmp_path, capsys):
    """同名局部变量不能掩盖"模块级符号已被删" —— 文档引用局部变量几乎总是写错。"""
    body = "def outer():\n    config = 1\n    return config\n\nCONFIG = 2\n"
    root = _repo(tmp_path, body)
    assert dc._sym_check("mod.py:CONFIG", root, Path("X.md")) == 0
    assert dc._sym_check("mod.py:config", root, Path("X.md")) == 1
    assert "SYMBOL_LOCAL_ONLY mod.py:config" in capsys.readouterr().out


def test_class_methods_count_as_public(tmp_path):
    body = "class LifecycleManager:\n    def confirm_step(self):\n        pass\n"
    root = _repo(tmp_path, body)
    assert dc._sym_check("mod.py:confirm_step", root, Path("X.md")) == 0


def test_ambiguous_bare_name_is_not_resolved_by_sibling(tmp_path):
    """本仓有两个同名文件时判歧义，不许拿兄弟仓的唯一命中来消歧。"""
    root = tmp_path / "session-launcher"
    for d in ("a", "b"):
        (root / d).mkdir(parents=True)
        (root / d / "mod.py").write_text("x = 1\n", encoding="utf-8")
    assert dc._resolve("mod.py", root) is None  # 两个同名 → 歧义


def test_absolute_path_inside_a_repo_is_checked(tmp_path):
    root = _repo(tmp_path, "x = 1\ny = 2\n")
    ref = f"见 {root}/src/mod.py:2"
    assert dc._cite_check(ref, root, Path("X.md")) == 0
    assert dc._cite_check(f"见 {root}/src/mod.py:99", root, Path("X.md")) == 1


def test_cli_subcommand_must_be_declared(tmp_path, capsys):
    """文档里的 `python3 x.py 子命令` 必须是 add_parser 的真实字面量（R157）。

    反向用例：README 曾写 `ccs.py socket start`，而真子命令里没有 socket —— 那类错
    读者会照着敲，静态判据能零误报地抓住它。
    """
    body = ("import argparse\np = argparse.ArgumentParser()\n"
            "s = p.add_subparsers()\ns.add_parser('status')\ns.add_parser('new', aliases=['create'])\n")
    root = _repo(tmp_path, body)
    assert dc._cli_check("```sh\npython3 src/mod.py status\n```", root, Path("X.md")) == 0
    assert dc._cli_check("```sh\npython3 src/mod.py create\n```", root, Path("X.md")) == 0
    assert dc._cli_check("```sh\npython3 src/mod.py socket start\n```", root, Path("X.md")) == 1
    assert "CLI_BAD_SUBCOMMAND" in capsys.readouterr().out


def test_cli_check_stays_silent_when_parser_is_not_static(tmp_path):
    """判不了就闭嘴：没有 add_parser（手写 argv 分发）、父解析器另有位置参数、路径解析不到。"""
    root = _repo(tmp_path)
    assert dc._cli_check("python3 src/mod.py anything", root, Path("X.md")) == 0
    assert dc._cli_check("python3 nowhere.py socket", root, Path("X.md")) == 0
    assert dc._cli_check("python3 src/mod.py socket", root, Path("ISSUES.md")) == 0


def test_path_escaping_the_repo_is_not_resolved(tmp_path, capsys):
    """`../outside/x.py:1` 逃出仓库目录 → 不算命中，按缺失处理（第十二轮审查）。"""
    root = _repo(tmp_path)
    (tmp_path / "outside").mkdir()
    (tmp_path / "outside" / "x.py").write_text("a = 1\n", encoding="utf-8")
    assert dc._resolve("../outside/x.py", root) is None
    assert dc._cite_check("见 ../outside/x.py:1", root, Path("X.md")) == 1
