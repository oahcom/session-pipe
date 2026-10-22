#!/usr/bin/env python3
"""ccs start 失败必须可观测 — 防"启动静默失败被当成 tmux 未就绪"。

根因（2026-10-19 生产实测）：
  writer 角色的 `ccs start` 返回 {"success": false, "error": "swap 仅剩 0MB..."}，
  但 _wait_for_role_ready 丢弃整个返回值 → 表层只看到"tmux 没起来"，
  日志还错报 "tmux alive but no claude process"（tmux 实际不存在）
  → 空转 90s → 排队 14 次 → 子链被回收 cancelled → 父链 s5 空等断链。

契约（两条，缺一则资源类阻断重回静默）：
  1. ccs start 返回码 != 0 或 stdout 含 "success": false → ERROR 日志含真实原因
  2. 同一事件必须写 blocker，使阻断可被外部（人/其他 session）观测

运行: cd /home/administrator/session-pipeline && python3 tests/test_role_start_failure_observable.py
"""
import re
import sys
from pathlib import Path

_SRC = (Path(__file__).resolve().parent.parent / "src")
_SRC_FILE = Path(_SRC) / "pipeflow" / "role_lifecycle.py"


def test_start_result_is_captured():
    """ccs start 的 subprocess 返回值必须被接住（原为裸调用丢弃）。

    锚点已从 _wait_for_role_ready 的内联副本移到唯一拉起路径
    _check_and_start_role（2026-10-22 去重）：两处各写一份时，只有一份接住返回值、
    也只有一份写 blocker，另一份在 90s 内对同一角色连发 6 次必败启动。
    """
    src = _SRC_FILE.read_text(encoding="utf-8")
    m = re.search(r'def _check_and_start_role.*?(?=\n    def )', src, re.S)
    assert m, "未找到 _check_and_start_role"
    body = m.group(0)
    assert re.search(r'_st = _engine_mod\._sp\.run\(\s*\n\s*\["python3", str\(_CCS_CLI\), "start"',
                     body), "ccs start 返回值未被接住（仍为裸调用，失败不可观测）"
    # 拉起只允许**一条**路径：_wait_for_role_ready 必须委托，不许再内联一份
    # （内联副本没有锁/冷却/审计，实测 pg 90s 内被连启 6 次必败）。
    w = re.search(r'def _wait_for_role_ready.*?(?=\n    def )', src, re.S)
    assert w, "未找到 _wait_for_role_ready"
    assert "self._check_and_start_role(role, deadline)" in w.group(0), \
        "_wait_for_role_ready 不再委托共用拉起路径（副本回归）"
    # tmux 探活仍会用 _sp.run，所以只禁"拉起角色的那一次调用"的指纹
    assert '"start", role' not in w.group(0) and "--drive" not in w.group(0), \
        "_wait_for_role_ready 里又出现了直接 ccs start（第二份副本回归）"


def test_failure_writes_blocker():
    """失败路径必须写 blocker（外部可观测），不能只 LOGGER。"""
    src = _SRC_FILE.read_text(encoding="utf-8")
    m = re.search(r'def _check_and_start_role.*?(?=\n    def )', src, re.S)
    assert m, "未找到 _check_and_start_role"
    body = m.group(0)
    assert "启动失败 (rc=" in body and "工作流无法投递" in body, \
        "拉起路径里缺少 blocker 文案（应含返回码）"
    # 文案在不算数：必须真的写 blocker（只留字符串时，删掉写库也能全绿）
    assert 'self._bb.write("blocker"' in body, "拉起失败没有写 blocker（外部不可观测）"
    assert "ccs start 失败 (rc=%s)" in body, "缺少含返回码的 ERROR 日志"


def test_no_false_tmux_alive_claim():
    """日志不得在 tmux 不存在时声称 'tmux alive'（假成功文案）。"""
    src = _SRC_FILE.read_text(encoding="utf-8")
    assert "tmux alive but no claude process" not in src, \
        "仍存在误导性日志「tmux alive but no claude process」（tmux 可能根本不存在）"


if __name__ == "__main__":
    _fns = (test_start_result_is_captured, test_failure_writes_blocker,
            test_no_false_tmux_alive_claim)
    _fails = 0
    for _fn in _fns:
        try:
            _fn()
            print(f"  OK  {_fn.__name__}")
        except AssertionError as _e:
            _fails += 1
            print(f"  FAIL {_fn.__name__}: {_e}")
    print(f"\n结果: {len(_fns) - _fails} 通过, {_fails} 失败")
    sys.exit(1 if _fails else 0)
