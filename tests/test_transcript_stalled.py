"""生成中 ≠ 有进展：spinner 挂着但 transcript 不落行 = 卡死。

现场（2026-10-21 05:22Z）：coordinator 末事件是一条没有 tool_result 的 tool_use，
之后 31 分钟 transcript 零行，pane 一直显示 `Compacting… 55% · esc to interrupt`。
launcher 的 `_role_is_busy` 见状态栏即拒投（"角色思考中"），pipeline 的 L4 见
step status 为空串即判空闲——两边结论相反，结果既不投递也不回收：
  3 个 WL-02 实例 queued=387/291，ts 冻结在 04:32，等到 08:32 预算耗尽才被回收。

本契约：`_transcript_stalled` 用落盘事实区分「在跑」与「卡死」，
且取不到 transcript 时一律返回 False（宁可漏判，不打断正在干活的角色）。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_transcript_stalled.py -q
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow import role_lifecycle as rl  # noqa: E402


def _write_transcript(root: Path, role: str, age_sec: float) -> Path:
    """造一个 transcript，mtime 回拨到 age_sec 前。"""
    d = root / f"-home-administrator-ccs-workspaces-{role.replace('_', '-')}"
    d.mkdir(parents=True, exist_ok=True)
    f = d / "sess.jsonl"
    f.write_text('{"type":"assistant","message":{"content":[]}}\n', encoding="utf-8")
    old = time.time() - age_sec
    os.utime(f, (old, old))
    return f


def test_quiet_transcript_is_stalled(tmp_path, monkeypatch):
    """300s 无落行 → 卡死。"""
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_transcript(tmp_path, "coordinator", age_sec=400)
    assert rl._transcript_stalled("coordinator") is True


def test_recent_transcript_is_not_stalled(tmp_path, monkeypatch):
    """刚有事件 → 正常在跑，不得判卡死（否则会 C-u 打断当前轮）。"""
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_transcript(tmp_path, "engineer", age_sec=30)
    assert rl._transcript_stalled("engineer") is False


def test_missing_transcript_is_not_stalled(tmp_path, monkeypatch):
    """取不到 transcript → False。保守方向：不打断。"""
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    assert rl._transcript_stalled("nonexistent-role") is False


def test_picks_newest_transcript(tmp_path, monkeypatch):
    """会话轮转后旧文件停写，判据必须看最新的那个，不能被旧文件误判卡死。"""
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_transcript(tmp_path, "qa", age_sec=7200)
    d = tmp_path / "-home-administrator-ccs-workspaces-qa"
    newer = d / "new.jsonl"
    newer.write_text("{}\n", encoding="utf-8")
    assert rl._transcript_stalled("qa") is False


if __name__ == "__main__":
    import tempfile
    for name, fn in [
        ("静默 400s → 卡死", test_quiet_transcript_is_stalled),
        ("刚有事件 → 在跑", test_recent_transcript_is_not_stalled),
        ("无 transcript → 不判死", test_missing_transcript_is_not_stalled),
        ("轮转后取最新文件", test_picks_newest_transcript),
    ]:
        with tempfile.TemporaryDirectory() as td:
            class _MP:
                def setattr(self, obj, n, v):
                    setattr(obj, n, v)
            fn(Path(td), _MP())
        print(f"  OK  {name}")
    print("全部通过")
