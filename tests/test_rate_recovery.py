#!/usr/bin/env python3
"""_recover_rate_limited_roles 回归守卫。

生产事故：curator / maintainer 的 pane 卡在
「Please run /login · API Error: 403 FreeTierError」数小时无人恢复。
根因是恢复函数只从 workflow_instances 的 assignee 取角色，而这两个角色
没有在跑的工作流 → 永远不在候选集里。tmux 兜底必须覆盖到它们。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_rate_recovery.py -q
"""
import re
import time
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


def test_no_workflow_role_recovered_via_tmux(tmp_path):
    """不在任何工作流里、但 tmux 会话存在的角色，403 时也要被恢复。

    触发判据已收窄为转录错误轮（pane 只用于分型）：错误轮写进转录。
    """
    eng = _engine()
    sent = []
    _write_turn(tmp_path, "curator", age_sec=30)
    _write_turn(tmp_path, "maintainer", age_sec=30)

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


def test_upstream_connection_lost_is_recovered(tmp_path):
    """pane 停在 "API Error: upstream connection lost" 也要恢复。

    实测 curator 12:39 起 21min 无产出：进程活着、pane 停在错误，
    恢复词表里没有这一种 → 引擎视而不见。
    触发判据已收窄为转录错误轮：该类错误轮常无 isApiErrorMessage 标记，
    以未标记纯错误轮写入（正文整体以 API Error: 开头）。
    """
    eng = _engine()
    sent = []
    _write_turn(tmp_path, "curator", age_sec=30, err=False,
                text="API Error: upstream connection lost")

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


def test_recovery_resets_backoff_after_healthy_tick(tmp_path):
    """403 → 恢复 → 再次 403：第二次仍从 attempt=1 起步，而不是累加到 2。

    不清退避时档位只增不减，反复的间歇性 403 会把等待推到 1800s 封顶，
    恢复一次要等半小时。触发判据已收窄为转录错误轮：转录随三相演化
    （错误轮 → tool_use 进展轮 → 新错误轮），恢复=错误后出现真进展。
    """
    import json as _json
    from datetime import datetime, timezone, timedelta
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

    def _write_tool_turn(role, age_sec):
        d = tmp_path / f"-home-administrator-ccs-workspaces-{role}"
        d.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc) - timedelta(seconds=age_sec)
        rec = {"type": "assistant",
               "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
               "message": {"role": "assistant", "content": [
                   {"type": "tool_use", "id": "t1", "name": "Bash", "input": {}}]}}
        with open(d / "sess.jsonl", "a", encoding="utf-8") as fh:
            fh.write(_json.dumps(rec) + "\n")

    _write_turn(tmp_path, "lr", age_sec=120)         # 首次 403：错误轮
    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()            # 首次 403 → attempt=1
        assert eng._last_rate_recover["lr"][1] == 1
        _write_tool_turn("lr", age_sec=90)           # 恢复：错误后出现 tool_use 进展
        state["mode"] = "ok"
        eng._recover_rate_limited_roles()            # 恢复 → 清退避
        assert "lr" not in eng._last_rate_recover, "恢复后应清掉退避状态"
        _write_turn(tmp_path, "lr", age_sec=1)       # 再次 403：新的错误轮
        state["mode"] = "403"
        eng._recover_rate_limited_roles()            # 再次 403

    assert eng._last_rate_recover["lr"][1] == 1, "退避应重新从第 1 档起步"


def test_backoff_blocks_immediate_retry(tmp_path):
    """第二次调用落在退避窗口内 → 不再发。"""
    eng = _engine()
    sent = []
    _write_turn(tmp_path, "lr", age_sec=30)

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


def test_audit_title_carries_time_so_rows_dont_merge(tmp_path):
    """审计行标题必须带时刻：bus 按标题 UPSERT 且保留首次 ts。

    attempt 随退避重置反复从 1 起算，同一标题会把今天的注入覆盖到昨天那行上——
    实测今日 8 次「已发继续」在 facts 里查不到，最后可见行停在 2026-10-20 23:46。
    """
    eng = _engine()
    titles = []
    eng._bb.write.side_effect = lambda cat, t, **k: titles.append(t)
    _write_turn(tmp_path, "lr", age_sec=30)

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="API Error: 403 FreeTierError")
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert titles, "注入后必须有审计行"
    # FreeTierError 属硬限（IP/额度，重发不能恢复）→ 标题是 [quota] 且只到分钟；
    # 两种标题都必须带日期，否则跨天 UPSERT 会把今天的注入并进昨天那行。
    assert re.search(r"（第 1 次 \d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?）",
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


def test_request_rejected_429_is_recovered(tmp_path):
    """9router 现行 429 文案 `Request rejected (429)` 必须命中恢复词表。

    2026-10-21 实测 closer/maintainer 的 pane 停在这一行，旧词表
    ("API Error: 429" / "Rate limit exceeded") 一条不中 → 恢复分支整条不可达，
    两角色各空等 16min，当日 [rate_limit] 审计 0 条（#432582）。
    触发判据已收窄为转录错误轮：该错误轮写进转录。
    """
    eng = _engine()
    sent = []
    _write_turn(tmp_path, "closer", age_sec=30,
                text="API Error: Request rejected (429) · [ollama/gpt-oss:120b]")

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
    # 文本现在是独立的 -l 调用，所以是那一调用的最后一个参数（不再与 Enter 同调用）
    body = [c for c in sent if "继续" in " ".join(c)][0][-1]
    assert "重发" in body and "不要重新规划" in body, body
    # 反向断言：旧文案断言"0 产出"，而 6h 内 11 次中断里 10 次被杀前已调 7~92 次工具
    # → 角色据此重做已做过的动作（本轮实测）。文案不许再声称零产出。
    assert "0 产出" not in body, body
    # 反向断言：旧文案把证据一起禁了（"不要写自检报告"），而这条注入是角色失忆后
    # 唯一的上下文锚 → 必须索要一行可验证据，不许只喊继续。
    assert "可验证据" in body and "不要写自检报告" not in body, body


def test_reset_predicate_ignores_error_turn_age():
    """恢复判据必须是「进展」不是「新鲜度」：错误轮再老也算没恢复。

    旧写法用默认 120s 时效：角色卡在错误轮、pane 里文案已滚出视口（实测 pane 只
    认出 0.67% 的错误轮）时，120s 后谓词翻 False → 清掉退避状态 → attempt 恒 1，
    退避曲线失效（24h 614 次注入、间隔 median 248s = 重置后再等 120s）。
    """
    eng = _engine()
    eng._last_rate_recover = {"curator": (time.time() - 30, 3)}   # 已在第 4 档退避
    seen = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-curator\n")
        if "capture-pane" in cmd:
            # pane 不含错误文案（已滚出视口）——正是复查要覆盖的那 99%
            return MagicMock(returncode=0, stdout="✻ Thinking…\n❯ ")
        return MagicMock(returncode=0, stdout="")

    def fake_last_turn(role, max_age=None):
        seen.append(max_age)
        return True   # 转录：最后一条 assistant 就是错误轮（不论多老）

    with patch("pipeflow.engine._sp.run", side_effect=fake_run), \
         patch("pipeflow.role_lifecycle._transcript_last_turn_is_api_error",
               side_effect=fake_last_turn):
        eng._recover_rate_limited_roles()

    assert seen, "恢复路径没有查转录（pane 只有视口，认不出 99% 的错误）"
    assert all(_a == float("inf") for _a in seen), \
        f"重置判据仍在用时效窗口 {seen}：错误轮过期会被当成已恢复"
    assert "curator" in eng._last_rate_recover, "错误轮未变老就清掉了退避状态"
    assert eng._last_rate_recover["curator"][1] >= 3, "attempt 被重置，退避曲线失效"


# ── #463020：410 ModelDeprecated 是终态，不是瞬时限流 ──
# 实测 pm 被当瞬时限流走了 5 档退避、第 5 档后永久 1800s 重发，输入框积 10 份
# nudge / 13.1h 零产出。终态错误的处置是换模型重启会话，不是发「继续」。

_DEPRECATED = ('API Error: 410 [410]: {"type":"error","error":{"type":"ModelDeprecated",'
               '"message":"Model longcat-2.5-preview-free has been deprecated."}}')


def test_deprecated_model_not_nudged(tmp_path, monkeypatch):
    """末轮是 410 ModelDeprecated → 一次都不发「继续」，只留一条 [terminal] 审计。"""
    from pipeflow import role_lifecycle as rl
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_turn(tmp_path, "pm", age_sec=30, text=_DEPRECATED)
    eng = _engine()
    titles = []
    eng._bb.write.side_effect = lambda cat, t, **k: titles.append(t)

    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-pm\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="❯ \n")
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        for _ in range(3):
            eng._recover_rate_limited_roles()

    assert sent == [], f"终态错误不得重发 nudge, got {sent}"
    assert not any("被额度拒绝" in t for t in titles), \
        f"模型下线不能说成额度拒绝: {titles}"
    assert len([t for t in titles if t.startswith("[terminal] pm ")]) == 1, \
        f"3 个 tick 应只留 1 条终态审计, got {titles}"


def test_deprecated_model_audit_carries_real_error(tmp_path, monkeypatch):
    """审计证据要带错误原文（ModelDeprecated），不能只留 UI 壳。"""
    from pipeflow import role_lifecycle as rl
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_turn(tmp_path, "pm", age_sec=30, text=_DEPRECATED)
    eng = _engine()
    ev = {}
    eng._bb.write.side_effect = lambda cat, t, **k: ev.setdefault("e", k.get("evidence"))

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-pm\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="❯ \n")
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert "ModelDeprecated" in (ev.get("e") or ""), ev


def test_transient_error_still_nudged(tmp_path, monkeypatch):
    """反向守卫：普通 429 仍必须发「继续」——终态分流不能把可恢复的也停掉。"""
    from pipeflow import role_lifecycle as rl
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_turn(tmp_path, "qa", age_sec=30, text="API Error: 429 Rate limit exceeded")
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-qa\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="❯ \n")
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert any("继续" in " ".join(c) for c in sent), f"可恢复的 429 被误停: {sent}"


# ── #459931：nudge 必须「先文本、后独立 Enter」两次调用 ──
# 同一次 send-keys 里带 Enter 时，Enter 落在多行文本中间，实测干净 pane
# 落地 0/3；分调用 3/3（真值取转录 user 轮，不靠 pane 目测）。

def test_nudge_sends_text_and_enter_separately(tmp_path, monkeypatch):
    """nudge 的 send-keys 序列：C-u → -l 文本 → 独立 Enter，Enter 不与文本同调用。"""
    from pipeflow import role_lifecycle as rl
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_turn(tmp_path, "qa", age_sec=30)
    eng = _engine()
    seq = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-qa\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="❯ \n")
        if "send-keys" in cmd:
            seq.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert len(seq) == 3, f"应为 3 次 send-keys（C-u/文本/Enter）, got {seq}"
    assert seq[0][-1] == "C-u", seq[0]
    assert "-l" in seq[1] and "--" in seq[1], f"文本必须按字面量写入: {seq[1]}"
    from pipeflow.role_lifecycle import _RATE_LIMIT_NUDGE
    assert seq[1][-1] == _RATE_LIMIT_NUDGE, f"文本调用不得带 Enter: {seq[1]}"
    assert seq[2][-1] == "Enter" and len(seq[2]) <= 6, f"Enter 必须单独一次: {seq[2]}"


def test_compact_sends_enter_separately(tmp_path, monkeypatch):
    """/compact 同款缺陷：文本与 Enter 不得同一次调用。"""
    from pipeflow.engine import _WORKFLOWS_DIR
    eng = WorkflowEngine(workflows_dir=_WORKFLOWS_DIR)
    eng._lm = MagicMock()
    eng._lm.query.return_value = []
    eng._last_compact = {}
    seq = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="Context limit reached")
        if "send-keys" in cmd:
            seq.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._check_context_overflow()

    assert any(c[-1] == "/compact" for c in seq), f"文本调用缺失: {seq}"
    assert any(c[-1] == "Enter" for c in seq), f"Enter 必须单独一次: {seq}"
    assert not any("/compact" in c and "Enter" in c for c in seq), \
        f"文本与 Enter 同调用 → 落地 0/3: {seq}"


def test_recent_tool_use_suppresses_nudge(tmp_path, monkeypatch):
    """角色正在跑工具时不注入：C-u 会与工具输出撞车，文本被吞但 tmux rc=0 →
    写出「已发继续」假审计（6h 12 次注入中 1 次转录里没有落地）。"""
    import json as _json
    from datetime import datetime, timezone, timedelta
    from pipeflow import role_lifecycle as rl
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    d = tmp_path / "-home-administrator-ccs-workspaces-qa"
    d.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)

    def _rec(kind, age, content, err=False):
        rec = {"type": "assistant",
               "timestamp": (now - timedelta(seconds=age)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
               "message": {"role": "assistant", "content": content}}
        if err:
            rec["isApiErrorMessage"] = True
        return rec

    with open(d / "sess.jsonl", "a", encoding="utf-8") as fh:
        fh.write(_json.dumps(_rec("text", 30, [{"type": "text", "text": "API Error: 429"}], err=True)) + "\n")
        fh.write(_json.dumps(_rec("tool", 5, [{"type": "tool_use", "id": "t1",
                                               "name": "Bash", "input": {}}])) + "\n")
    sent = _run_recovery(_engine(), {"qa": "❯ \nPlease run /login · API Error: 429\n"})
    assert not any("继续" in " ".join(c) for c in sent), f"角色正在跑工具却被注入: {sent}"


# ── 引文误判 / 未标记错误轮：触发只认转录，pane 不再单独触发 ──
# 实测 pg 会话：任务 17:23:45 自然收尾（产出一份分析报告），报告里引用了
# 历史失败「今天那 15 次失败全是 opencode 线的 429 FreeUsageLimitError」。
# pane 末 20 行命中错误词表 → _pane_err=True 绕过转录判据，17:24:05 与
# 17:25:37 两次向已收尾的会话发 nudge，角色被迫重做已完成的报告。
# 错误轮的权威判据是转录（24h 实测 1,342 条错误轮 pane 只认出 9 次 = 0.67%），
# pane 只用于分型（配额/瞬时/终态）。

def test_pane_quoted_error_completed_turn_no_nudge(tmp_path, monkeypatch):
    """pane 含错误文案（报告引用）但转录末轮是正常完成轮 → 不发 nudge。

    报告/日志摘录**引用**错误文案不是活限流；旧代码（pane OR 转录）此例必红。
    """
    from pipeflow import role_lifecycle as rl
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_turn(tmp_path, "pg", age_sec=30, err=False,
                text="分析报告：今天那 15 次失败全是 opencode 线的 429 FreeUsageLimitError，"
                     "均为出口 IP 限流，处理结论见上文。")
    sent = _run_recovery(_engine(), {"pg": "❯ 今天那 15 次失败全是 429 FreeUsageLimitError\n"})
    assert not any("继续" in " ".join(c) for c in sent), \
        f"报告引用错误文案不是活限流，自然收尾的会话不得被唤醒重做: {sent}"


def test_unflagged_error_turn_still_nudges(tmp_path, monkeypatch):
    """无 isApiErrorMessage 标记、正文整体就是错误文本（流中断形态）→ 发 nudge。

    「上游流中断」类错误轮没有 isApiErrorMessage 标记（pane 模式 upstream
    connection lost 正因此存在）。旧 judge 只认标记 → 旧代码此例应红。
    """
    from pipeflow import role_lifecycle as rl
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_turn(tmp_path, "curator", age_sec=30, err=False,
                text="API Error: upstream connection lost")
    sent = _run_recovery(_engine(), {"curator": "❯ \n"})
    assert any("继续" in " ".join(c) for c in sent), \
        f"未标记的纯错误轮（流中断）必须触发恢复: {sent}"


def test_flagged_error_turn_nudges_unchanged(tmp_path, monkeypatch):
    """isApiErrorMessage:true 的错误轮 → 发 nudge（既有行为回归锚）。"""
    from pipeflow import role_lifecycle as rl
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_turn(tmp_path, "qa", age_sec=30)   # 默认即标记错误轮
    sent = _run_recovery(_engine(), {"qa": "❯ \n"})
    assert any("继续" in " ".join(c) for c in sent), \
        f"标记错误轮必须照旧触发恢复: {sent}"
