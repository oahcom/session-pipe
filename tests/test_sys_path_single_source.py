"""守卫：测试只认一份 sys.path 来源（conftest 的 _src）。

历史写法把 ~/session-pipeline/src 插到 sys.path[0]。在 git worktree 里跑测试时，
**运行时** import（测试函数体内的 from pipeflow.engine import …）会命中主工作树，
于是同一模块出现两份实例（常量、patch 目标各一套）——worktree 的红绿结论不可信。
2026-10-22 实测：worktree 已含新符号，测试仍 ImportError 到主树；按"跟随 __file__"
局部修 2 个文件后全量红从 7 涨到 14，再修 3 个涨到 37（半树半树）。

本守卫只锁一件事：别再把源码路径写死。跨树跑测试依赖 conftest 的 _src。
"""
import pathlib
import re

_TESTS = pathlib.Path(__file__).resolve().parent
_BAD = re.compile(r"sys\.path\.insert\(.*(?:home\(\)|expanduser|/home/administrator)")


def test_no_test_file_hardcodes_pipeline_path():
    bad = [f"{p.name}:{i}"
           for p in _TESTS.glob("test_*.py")
           for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1)
           if _BAD.search(line)]
    assert not bad, ("这些测试把源码路径写死，会让 worktree 的测试读到主工作树: "
                     + ", ".join(bad))


def test_conftest_pins_its_own_src():
    """conftest 必须按本文件位置定位 src —— 这是全仓唯一的 sys.path 来源。"""
    txt = (_TESTS / "conftest.py").read_text(encoding="utf-8")
    assert 'Path(__file__).resolve().parent.parent / "src"' in txt, \
        "conftest 未按 __file__ 定位 src"
