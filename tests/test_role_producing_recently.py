#!/usr/bin/env python3
"""回归守卫：auto-cancel 判死不得只看 pane 静默。

现场（2026-10-19 21:12:49）：引擎 auto-cancel 掉 wf_sub_8de33cd1559e，
理由 "running 状态 s1 无推进记录 步骤停滞 0.2h" —— 但 reviewer 当时正在
写报告（会话转录 mtime 21:06:17），只是 pane 静默 465s > PANE_ACTIVE_WINDOW(300s)。
Claude 深度推理时不产生终端输出，而 review 类任务本身就是长推理。

template=reviewer_pr_review 历史 cancelled 占 22%（5/23）。

行为契约：
  1. 转录近期有写入  → True（还在干活，不得判死）
  2. 转录长期无写入  → False（真僵尸，照常判死）
  3. 探测不可用      → False（退化为原 pane 判据，不放过真僵尸）
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.wf_lifecycle import _role_producing_recently


def _mk_transcript_dir(base: Path, role: str, age_sec: float) -> None:
    """在 base 下造角色转录目录（含 .claude/projects 两层），内有 age_sec 秒前写入的 jsonl。"""
    d = base / ".claude" / "projects" / f"-home-administrator-ccs-workspaces-{role}"
    d.mkdir(parents=True, exist_ok=True)
    f = d / "session.jsonl"
    f.write_text("{}\n", encoding="utf-8")
    t = time.time() - age_sec
    os.utime(f, (t, t))


def test_fresh_transcript_means_producing(tmp_path, monkeypatch):
    """转录 10 秒前写入 → 判定为在产出（不得判死）。"""
    _mk_transcript_dir(tmp_path, "reviewer", age_sec=10)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    assert _role_producing_recently("reviewer") is True, "刚写过转录的角色不应被判死"


def test_stale_transcript_means_not_producing(tmp_path, monkeypatch):
    """转录 20 分钟前写入（> 300s 窗）→ 判定为未产出（允许判死）。"""
    _mk_transcript_dir(tmp_path, "reviewer", age_sec=1200)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    assert _role_producing_recently("reviewer") is False, "转录久未更新应判为未产出"


def test_missing_transcript_dir_returns_false(tmp_path, monkeypatch):
    """探测不可用（目录不存在）→ False，退化为原 pane 判据。"""
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    assert _role_producing_recently("nosuchrole") is False, "无转录目录不得当成产出"


def test_window_boundary_is_configurable(tmp_path, monkeypatch):
    """window 参数生效：90 秒前的转录在 60s 窗内为假、在 300s 窗内为真。"""
    _mk_transcript_dir(tmp_path, "reviewer", age_sec=90)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    assert _role_producing_recently("reviewer", window=60) is False, "超出自定义窗"
    assert _role_producing_recently("reviewer", window=300) is True, "在自定义窗内"


if __name__ == "__main__":
    import traceback

    class _MP:
        """minimal monkeypatch：仅支持 setattr(Path, 'home', ...)。"""
        def __init__(self):
            self._undo = []

        def setattr(self, obj, name, val):
            self._undo.append((obj, name, getattr(obj, name)))
            setattr(obj, name, val)

        def undo(self):
            for obj, name, old in reversed(self._undo):
                setattr(obj, name, old)

    import tempfile
    failed = 0
    for fn in (test_fresh_transcript_means_producing,
               test_stale_transcript_means_not_producing,
               test_missing_transcript_dir_returns_false,
               test_window_boundary_is_configurable):
        mp = _MP()
        with tempfile.TemporaryDirectory() as td:
            try:
                fn(Path(td), mp)
                print(f"  PASS  {fn.__name__}")
            except Exception:
                failed += 1
                print(f"  FAIL  {fn.__name__}")
                traceback.print_exc()
            finally:
                mp.undo()
    print("FAILED" if failed else "ALL PASS")
    sys.exit(1 if failed else 0)
