"""ref-guard（scripts/check_dangling_refs.py）的纯 Python 表征测试。

只断言函数级行为，不触及 git。每条只验证一个契约。

跑法：python3 tests/test_dangling_refs.py（也可 pytest）
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.check_dangling_refs import dangling_refs

NEW = {
    "a/b.py": "x = 1\ndef keep():\n    return x\n",
    "a/c.py": "from a.b import keep\nprint(keep())\n",
}

# 1. 没删除任何符号 → {}
assert dangling_refs(dict(NEW), dict(NEW)) == {}

# 2. 删掉 keep，c.py 还在引用 → 命中
assert dangling_refs(NEW, {"a/b.py": "x = 1\n", "a/c.py": NEW["a/c.py"]}) == {
    "keep": {"a/c.py"}
}

# 3. 定义搬迁：b 删掉 keep，c 也删掉引用 → {}
assert dangling_refs(NEW, {"a/b.py": "x = 1\n", "a/c.py": "x = 1\n"}) == {}

# 4. 只改签名（同名仍定义） → 不算删除
assert dangling_refs(
    {"a/b.py": "def keep(a: int):\n    return a\n", "a/c.py": "from a.b import keep\nkeep(1)\n"},
    {"a/b.py": "def keep(a: int, b: int):\n    return a + b\n", "a/c.py": "from a.b import keep\nkeep(1, 2)\n"},
) == {}

# 5. 别名导入 from m import gone as g → gone 记为引用，应被拦
assert dangling_refs(
    {"a/m.py": "def gone():\n    return 1\n", "a/n.py": "from a.m import gone as g\nprint(g())\n"},
    {"a/m.py": "", "a/n.py": "from a.m import gone as g\nprint(g())\n"},
) == {"gone": {"a/n.py"}}

# 6. 动态属性 getattr(m, "gone") 不是静态引用 → 不命中（已知盲区）
assert dangling_refs(
    {"a/m.py": "def gone():\n    return 1\n", "a/n.py": 'print(getattr(__import__("a.m"), "gone"))\n'},
    {"a/m.py": "", "a/n.py": 'print(getattr(__import__("a.m"), "gone"))\n'},
) == {}

# 7. 注释里提到旧名字不算引用
assert dangling_refs(
    {"a/m.py": "def gone():\n    return 1\n", "a/n.py": "# gone 已不存在，见上一版\npass\n"},
    {"a/m.py": "", "a/n.py": "# gone 已不存在，见上一版\npass\n"},
) == {}

# 8. 定义搬家 + 引用方同步改 import → {}（不是丢失）
assert dangling_refs(
    {"a/m.py": "def keep():\n    return 1\n", "a/n.py": "y = 2\n", "a/u.py": "from a.m import keep\nkeep()\n"},
    {"a/m.py": "", "a/n.py": "def keep():\n    return 1\n", "a/u.py": "from a.n import keep\nkeep()\n"},
) == {}

# 9. 定义搬家但引用方仍从旧模块 import → 命中（导入路径已断）
assert dangling_refs(
    {"a/m.py": "def keep():\n    return 1\n", "a/n.py": "y = 2\n", "a/u.py": "from a.m import keep\nkeep()\n"},
    {"a/m.py": "", "a/n.py": "def keep():\n    return 1\n", "a/u.py": "from a.m import keep\nkeep()\n"},
) == {"keep": {"a/u.py"}}


# 10. 同名 basename 不同目录：p1/util.py 掉了 keep，p2/util.py 有 keep，不能互相顶替（codex 复审发现）
assert dangling_refs(
    {"p1/util.py": "def keep():\n    return 1\n", "p2/util.py": "x = 1\n", "u.py": "from p1.util import keep\nkeep()\n"},
    {"p1/util.py": "", "p2/util.py": "def keep():\n    return 1\n", "u.py": "from p1.util import keep\nkeep()\n"},
) == {"keep": {"u.py"}}

# 11. 搬家 + 别名导入：from n import keep as k，引用方已改指向 → {}（codex 复审发现）
assert dangling_refs(
    {"m.py": "def keep():\n    return 1\n", "n.py": "y = 2\n", "u.py": "from m import keep as k\nk()\n"},
    {"m.py": "", "n.py": "def keep():\n    return 1\n", "u.py": "from n import keep as k\nk()\n"},
) == {}

# 12. 模块级别名转发不是引用：_x = mod.x 的赋值左端是 Store，不算「还在用 _x」
assert dangling_refs(
    {"m.py": "def _x():\n    return 1\n", "n.py": "import mod\n"},
    {"m.py": "import mod\n_x = mod.x\n", "n.py": "import mod\n"},
) == {}

# 13. 别名转发后，外部通过属性访问 routing_daemon._x 仍算引用 → 必须命中
assert dangling_refs(
    {"m.py": "def _x():\n    return 1\n", "n.py": "import m\n"},
    {"m.py": "import mod\n_x = mod.x\n", "n.py": "import m\nm._x()\n"},
) == {"_x": {"n.py"}}

print("表征测试全部通过")
