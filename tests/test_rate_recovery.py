#!/usr/bin/env python3
"""_recover_rate_limited_roles 回归守卫。

生产事故：curator / maintainer 的 pane 卡在
「Please run /login · API Error: 403 FreeTierError」数小时无人恢复。
根因是恢复函数只从 workflow_instances 的 assignee 取角色，而这两个角色
没有在跑的工作流 → 永远不在候选集里。tmux 兜底必须覆盖到它们。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_rate_recovery.py -q
"""
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import WorkflowEngine, _WORKFLOWS_DIR


def _engine():
    eng = WorkflowEngine(workflows_dir=_WORKFLOWS_DIR)
    eng._lm = MagicMock()
    eng._lm.query.return_value = []          # DB 里没有活跃工作流
    eng._last_rate_recover = {}
    # conftest 已把 Blackboard 重定向到临时 DB；这里再 mock 是为了让断言面
    # 只落在 tmux 调用上，不依赖 bus 是否可用（本轮曾把 daemon 写的
    # "[rate_limit] knowledge_curator" 误归因给测试，记录在此避免重犯）。
    eng._bb = MagicMock()
    return eng


def test_no_workflow_role_recovered_via_tmux():
    """不在任何工作流里、但 tmux 会话存在的角色，403 时也要被恢复。"""
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-curator\nccs-maintainer\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0,
                             stdout="● Please run /login · API Error: 403 FreeTierError")
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    resumed = {c[c.index("-t") + 1][4:].split(":")[0]
               for c in sent if "继续" in " ".join(c)}
    assert resumed == {"curator", "maintainer"}, f"应恢复两个孤儿角色, got {sent}"


def test_upstream_connection_lost_is_recovered():
    """pane 停在 "API Error: upstream connection lost" 也要恢复。

    实测 curator 12:39 起 21min 无产出：进程活着、pane 停在错误，
    恢复词表里没有这一种 → 引擎视而不见。
    """
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-curator\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0,
                             stdout="● API Error: upstream connection lost\n✻ Sautéed for 48s")
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert any("继续" in " ".join(c) for c in sent), f"upstream 中断应发继续, got {sent}"


def test_recovered_role_not_pinged_again_on_scrollback():
    """错误滚到 -S -60 的旧内容里、角色已恢复 → 不得再发「继续」。

    实测 6h 内 35 条 `[rate_limit] 已发继续`：按全文匹配，旧错误每档退避
    都命中一次，每次换来一段无进展的自检长文。
    """
    eng = _engine()
    sent = []
    pane = ("● API Error: 403 FreeTierError\n" + "  " + "\n".join(["working…"] * 40)
            + "\n✻ Cogitated for 2m\n❯ ")

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-curator\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout=pane)
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert not any("继续" in " ".join(c) for c in sent), f"已恢复角色不应被重发继续, got {sent}"


def test_healthy_role_not_touched():
    """pane 没有额度错误文案 → 不发「继续」。"""
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="✻ Cogitated for 3m\n❯ ")
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert sent == [], f"健康角色不应被打扰, got {sent}"


def test_generating_role_not_interrupted_by_stale_error_text(tmp_path):
    """pane 里残留旧 403 文案但角色正在生成 → 不发 C-u/「继续」。

    -S -60 会带上滚出视口的旧错误行；只按"含错误文案"判会把正在干活的角色
    打断（C-u 清输入行）。
    """
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout=(
                "● Please run /login · API Error: 403 FreeTierError\n"
                "✻ Gesticulating… (13s · ↓ 492 tokens)\n"
                "────────\n❯ \n────────\n"
                "  bypass permissions on (shift+tab to cycle) · esc to interrupt"))
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    # 判据已从"是否生成中"改为"生成中且 transcript 无进展"：本条要验的是
    # "有进展就不打断"，必须给出新鲜 transcript，否则会落进卡死分支。
    import os, time
    from pipeflow import role_lifecycle as _rl
    _d = tmp_path / "-home-administrator-ccs-workspaces-lr"
    _d.mkdir(parents=True, exist_ok=True)
    _f = _d / "sess.jsonl"
    _f.write_text("{}\n", encoding="utf-8")
    os.utime(_f, (time.time(), time.time()))
    with patch.object(_rl, "_TRANSCRIPT_ROOT", tmp_path), \
         patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert sent == [], f"生成中的角色不应被打断, got {sent}"


def test_client_retry_in_progress_not_interrupted(tmp_path):
    """429 客户端重试进行中（`Retrying in 5s · attempt 9/10`）→ 不发「继续」。

    2026-10-21 实测：devops/closer/qa 收到「继续」后 attempt 从 4 回升到 9。
    CLI 自己在退避，插入新消息会把计数重置，把快恢复的角色重新推回 10/10。
    """
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-devops\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout=(
                "❯ 继续\n"
                '✻ 429 [429]: {"type":"error"} · Retrying in 5s · attempt 9/10\n'
                "────────\n❯ \n────────\n"
                "  bypass permissions on · esc to interrupt"))
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    # transcript 已停滞（否则会落到另一条 continue）——要验的正是 429 分支先拦住
    from pipeflow import role_lifecycle as _rl
    _d = tmp_path / "-home-administrator-ccs-workspaces-devops"
    _d.mkdir(parents=True, exist_ok=True)
    _f = _d / "sess.jsonl"
    _f.write_text("{}\n", encoding="utf-8")
    import os, time
    old = time.time() - 900
    os.utime(_f, (old, old))
    with patch.object(_rl, "_TRANSCRIPT_ROOT", tmp_path), \
         patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert sent == [], f"429 客户端重试中不应被打断, got {sent}"


def test_recovery_resets_backoff_after_healthy_tick():
    """403 → 恢复 → 再次 403：第二次仍从 attempt=1 起步，而不是累加到 2。

    不清退避时档位只增不减，反复的间歇性 403 会把等待推到 1800s 封顶，
    恢复一次要等半小时。
    """
    eng = _engine()
    state = {"mode": "403"}

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            if state["mode"] == "403":
                return MagicMock(returncode=0, stdout="API Error: 403 FreeTierError")
            return MagicMock(returncode=0, stdout="✻ Cogitated for 3m\n❯ ")
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()            # 首次 403 → attempt=1
        assert eng._last_rate_recover["lr"][1] == 1
        state["mode"] = "ok"
        eng._recover_rate_limited_roles()            # 恢复 → 清退避
        assert "lr" not in eng._last_rate_recover, "恢复后应清掉退避状态"
        state["mode"] = "403"
        eng._recover_rate_limited_roles()            # 再次 403

    assert eng._last_rate_recover["lr"][1] == 1, "退避应重新从第 1 档起步"


def test_backoff_blocks_immediate_retry():
    """第二次调用落在退避窗口内 → 不再发。"""
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="API Error: 429 Rate limit exceeded")
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()
        eng._recover_rate_limited_roles()

    resumed = [c for c in sent if "继续" in " ".join(c)]
    assert len(resumed) == 1, f"退避窗口内应只发 1 次, got {len(resumed)}"


def test_audit_title_carries_time_so_rows_dont_merge():
    """审计行标题必须带时刻：bus 按标题 UPSERT 且保留首次 ts。

    attempt 随退避重置反复从 1 起算，同一标题会把今天的注入覆盖到昨天那行上——
    实测今日 8 次「已发继续」在 facts 里查不到，最后可见行停在 2026-10-20 23:46。
    """
    eng = _engine()
    titles = []
    eng._bb.write.side_effect = lambda cat, t, **k: titles.append(t)

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="API Error: 403 FreeTierError")
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert titles, "注入后必须有审计行"
    assert re.search(r"已发继续（第 1 次 \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}）",
                     titles[0]), f"标题缺完整日期 → 跨天 UPSERT 会合并, got {titles[0]}"


def test_send_failure_writes_no_audit_and_does_not_raise():
    """发送失败时不得写审计（没注入就别谎报），也不得抛异常打断其它角色。"""
    eng = _engine()

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="API Error: 403 FreeTierError")
        if "send-keys" in cmd:
            raise OSError("tmux gone")
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()   # 不应抛

    assert not eng._bb.write.called, "注入失败却写了审计"


def test_send_nonzero_rc_writes_no_audit():
    """tmux 返回 rc≠0（会话已死）也必须算发送失败。

    subprocess.run 不会因 rc≠0 抛异常；只看异常会把"没发出去"记成"已发继续"。
    """
    eng = _engine()

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="API Error: 403 FreeTierError")
        if "send-keys" in cmd:
            return MagicMock(returncode=1, stdout="", stderr="no such session")
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert not eng._bb.write.called, "tmux rc=1 却写了「已发继续」"

def test_send_negative_rc_or_none_is_failure():
    """负数 rc（-9 SIGKILL / -15 SIGTERM）与 None 都不能算成功。

    max(0, -9) == 0 → 会写出「已发继续」假审计，正是本提交要消灭的那一类。
    """
    for _r1, _r2 in ((-9, 0), (0, -15), (None, 0)):
        eng = _engine()

        def fake_run(cmd, *a, _a=_r1, _b=_r2, **k):
            if "list-sessions" in cmd:
                return MagicMock(returncode=0, stdout="ccs-lr\n")
            if "capture-pane" in cmd:
                return MagicMock(returncode=0, stdout="API Error: 403 FreeTierError")
            if "send-keys" in cmd:
                return MagicMock(returncode=_a if "C-u" in cmd else _b, stdout="")
            return MagicMock(returncode=0, stdout="")

        with patch("pipeflow.engine._sp.run", side_effect=fake_run):
            eng._recover_rate_limited_roles()
        assert not eng._bb.write.called, f"rc=({_r1},{_r2}) 却写了「已发继续」"


def test_request_rejected_429_is_recovered():
    """9router 现行 429 文案 `Request rejected (429)` 必须命中恢复词表。

    2026-10-21 实测 closer/maintainer 的 pane 停在这一行，旧词表
    ("API Error: 429" / "Rate limit exceeded") 一条不中 → 恢复分支整条不可达，
    两角色各空等 16min，当日 [rate_limit] 审计 0 条（#432582）。
    """
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-closer\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout=(
                "● API Error: Request rejected (429) · [ollama/gpt-oss:120b] [429]:\n"
                '{"error":"You reached the Free usage limit."} (reset after 4m 45s)\n'
                "✻ Cogitated for 16m 23s\n❯ "))
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert any("继续" in " ".join(c) for c in sent), f"Request rejected (429) 应发继续, got {sent}"


if __name__ == "__main__":
    for fn in (test_no_workflow_role_recovered_via_tmux,
               test_healthy_role_not_touched,
               test_backoff_blocks_immediate_retry,
               test_request_rejected_429_is_recovered):
        fn()
        print(f"OK {fn.__name__}")


# ── R122：pane 只有视口，错误判据必须回到转录 ──
# 实测 24h 全角色 1,342 条 API 错误轮（output_tokens 恒为 0），pane 只认出 9 次
# = 0.67%；maintainer 216→0 / product-architect 119→0 / qa 107→0。15/15 ccs 会话
# alternate_on=1，capture-pane -S -60 拿到的就是视口，错误文案几分钟即滚出。

def _write_turn(tmp_path, role, age_sec, err=True, text="Please run /login · API Error: 403"):
    import json as _json
    from datetime import datetime, timezone, timedelta
    d = tmp_path / f"-home-administrator-ccs-workspaces-{role.replace('_', '-')}"
    d.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc) - timedelta(seconds=age_sec)
    rec = {"type": "assistant", "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
           "message": {"role": "assistant", "content": [{"type": "text", "text": text}]}}
    if err:
        rec["isApiErrorMessage"] = True
    with open(d / "sess.jsonl", "a", encoding="utf-8") as fh:
        fh.write(_json.dumps(rec) + "\n")


def _run_recovery(eng, panes):
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="".join(f"ccs-{r}\n" for r in panes))
        if "capture-pane" in cmd:
            r = cmd[cmd.index("-t") + 1][4:].split(":")[0]
            return MagicMock(returncode=0, stdout=panes[r])
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()
    return sent


def test_pane_blind_recent_api_error_still_recovered(tmp_path, monkeypatch):
    """pane 干净、转录里最后一条是 30s 前的 API 错误 → 必须恢复。"""
    from pipeflow import role_lifecycle as rl
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_turn(tmp_path, "curator", age_sec=30)
    sent = _run_recovery(_engine(), {"curator": "❯ \n"})
    assert any("继续" in " ".join(c) for c in sent), f"转录有近期错误却没恢复: {sent}"


def test_recovered_after_error_is_not_nudged(tmp_path, monkeypatch):
    """错误之后已有一条正常轮 → 自愈了，再发就是打断。"""
    from pipeflow import role_lifecycle as rl
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_turn(tmp_path, "curator", age_sec=60)
    _write_turn(tmp_path, "curator", age_sec=10, err=False, text="已修复并提交")
    sent = _run_recovery(_engine(), {"curator": "❯ \n"})
    assert not any("继续" in " ".join(c) for c in sent), f"已自愈角色被重发: {sent}"


def test_nudge_carries_what_to_retry(tmp_path, monkeypatch):
    """注入的不是裸「继续」：要指明重发被中断的那次请求。

    实测 24h 1,247 次「继续」里 611 次的后续轮零工具调用（另 520 次立刻又撞错）。
    """
    from pipeflow import role_lifecycle as rl
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_turn(tmp_path, "qa", age_sec=30)
    sent = _run_recovery(_engine(), {"qa": "❯ \n"})
    body = [c for c in sent if "继续" in " ".join(c)][0][-2]
    assert "重发" in body and "不要重新规划" in body, body
