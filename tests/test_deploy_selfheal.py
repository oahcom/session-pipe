"""部署树落后 main 时必须能被发现（daemon 自杀重启 → systemd ExecStartPre 快进）。

没有这条判据时的实测代价：2026-10-26 生产树落后 main **191 个提交**，角色投递/闸门/
证据判据整整一代没上线，而所有修复看起来都"做完"了。
"""
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def _repo():
    d = tempfile.mkdtemp()
    def g(*a):
        subprocess.run(["git", "-C", d] + list(a), check=True, capture_output=True)
    g("init", "-q")
    g("config", "user.email", "t@t")
    g("config", "user.name", "t")
    with open(os.path.join(d, "a"), "w") as fh:
        fh.write("1")
    g("add", "-A")
    g("commit", "-qm", "c1")
    g("branch", "-M", "main")
    c1 = subprocess.run(["git", "-C", d, "rev-parse", "HEAD"],
                        capture_output=True, text=True).stdout.strip()
    return d, g, c1


def test_detached_behind_main_counts_commits():
    from routing_daemon import _tree_behind_main
    d, g, c1 = _repo()
    with open(os.path.join(d, "b"), "w") as fh:
        fh.write("2")
    g("add", "-A")
    g("commit", "-qm", "c2")
    g("checkout", "-q", "--detach", c1)
    assert _tree_behind_main(d) == 1


def test_up_to_date_is_zero():
    from routing_daemon import _tree_behind_main
    d, _g, _c1 = _repo()
    assert _tree_behind_main(d) == 0


def test_unreadable_tree_is_zero_not_crash():
    """取不到就返回 0：判据算不出来时不许把 daemon 弄崩（也不许误判成"落后"）。"""
    from routing_daemon import _tree_behind_main
    assert _tree_behind_main("/nonexistent/definitely-not-a-repo") == 0
