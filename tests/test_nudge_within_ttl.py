#!/usr/bin/env python3
"""TTL 内必须提醒审批人 —— 只在过期后 nudge 等于把闸门交给自动放行。

现场（bus #427435/R04）：architect_full_design 已完成的 13 个 s4 中 8 个
approval_consumed_at 恰为 ttl+30.0~30.2min，即全部由 approval_timeout_fallback
自动通过，人一次都没参与。原因是 _nudge_assigner 只在 `now > ttl` 时触发：
整个 1 小时有效窗口里审批人收不到任何提醒，TTL 一过只剩 30min grace。

契约：
  1. 签发后 2min 起，按 2/5/10/20/40min 退避各推一次，档位不重复
  2. 推提醒不改 token、不消费审批（否则一提醒就把钥匙作废）
  3. nudges_sent 写回 DB：daemon 重启/新 run 对象不会退回第 0 档
  4. TTL 外仍走原来的 _nudge_assigner（过期后重推语义不变）

运行: cd ~/session-pipeline && python3 tests/test_nudge_within_ttl.py
"""
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

_SRC = Path(__file__).resolve().parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from lifecycle.manager import LifecycleManager      # noqa: E402
from pipeflow.step_handler import StepHandlerMixin  # noqa: E402


class _Engine(StepHandlerMixin):
    def __init__(self, lm):
        self._lifecycle = lm


class _Env:
    def __enter__(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.path = self.tmp.name
        self.tmp.close()
        self.lm = LifecycleManager("engineer", db_path=self.path)
        self.lm.upsert_template(
            "tpl", "模板", "描述",
            [{"step_id": "s1", "title": "产出", "target_role": "engineer",
              "prompt_template": "do", "exit_condition": {"bus_category": "task_spec"}},
             {"step_id": "s2", "title": "门禁", "target_role": "qa", "type": "review",
              "prompt_template": "gate", "exit_condition": {"bus_category": "test_report"}}],
            trigger_scene=["t"], allowed_initiators=["engineer"],
            allowed_executors=["engineer", "qa"])
        self.lm.start_wf("wf_n", "s1", "tpl", initiator="engineer")
        with patch("subprocess.run") as sp:
            sp.return_value = SimpleNamespace(returncode=1, stderr=b"busy")
            self.lm.complete_step("wf_n", "s1")
            self.lm.complete_step("wf_n", "s2")
        for _ in range(2):
            pass
        self.eng = _Engine(self.lm)
        self.run = SimpleNamespace(id="wf_n", workflow_name="tpl", step_results={})
        self.step = SimpleNamespace(id="s2", title="门禁", type="review",
                                    target_role="qa")
        return self

    def __exit__(self, *a):
        self.lm.close()
        os.unlink(self.path)
        return False

    def ttl(self):
        wf = self.lm._get_wf_unsafe("wf_n")
        return self.lm._parse_results(wf)["s2"]["approval_token_ttl"]

    def age_to(self, seconds):
        """把签发时刻回拨，模拟「已等 N 秒」。"""
        wf = self.lm._get_wf_unsafe("wf_n")
        sr = self.lm._parse_results(wf)
        now = time.time()
        sr["s2"]["approval_token_ttl"] = now - seconds + 3600
        sr["s2"]["approval_issued_at"] = now - seconds
        self.lm.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(sr), "wf_n"))
        self.lm.commit()

    def sent(self):
        wf = self.lm._get_wf_unsafe("wf_n")
        return self.lm._parse_results(wf)["s2"].get("nudges_sent", 0)


def test_no_nudge_before_two_minutes():
    """签发 1 分钟内不提醒：首发投递刚发过，立即重推只是噪音。"""
    with _Env() as e:
        e.age_to(30)
        with patch.object(e.eng, "_nudge_assigner") as n:
            e.eng._nudge_within_ttl(e.run, e.step, "tok", e.ttl())
        assert not n.called, "30 秒内就重推：与首次投递撞车"
        assert e.sent() == 0


def test_nudge_fires_after_two_minutes():
    """2 分钟后提醒一次（这是原实现完全缺失的窗口）。"""
    with _Env() as e:
        e.age_to(130)
        with patch.object(e.eng, "_nudge_assigner") as n:
            e.eng._nudge_within_ttl(e.run, e.step, "tok", e.ttl())
        assert n.call_count == 1, f"TTL 内未提醒，调用 {n.call_count} 次"
        assert e.sent() == 1


def test_backoff_does_not_repeat_same_slot():
    """同一档位只推一次：否则每 tick 一次，复刻 44 分钟 33 次刷屏。"""
    with _Env() as e:
        e.age_to(130)
        with patch.object(e.eng, "_nudge_assigner") as n:
            for _ in range(10):
                e.eng._nudge_within_ttl(e.run, e.step, "tok", e.ttl())
        assert n.call_count == 1, f"同档位重复推送 {n.call_count} 次"


def test_counter_survives_new_run_object():
    """计数写回 DB：新 run 对象（daemon 重启）不得退回第 0 档。"""
    with _Env() as e:
        e.age_to(130)
        with patch.object(e.eng, "_nudge_assigner"):
            e.eng._nudge_within_ttl(e.run, e.step, "tok", e.ttl())
        fresh_run = SimpleNamespace(id="wf_n", workflow_name="tpl", step_results={})
        with patch.object(e.eng, "_nudge_assigner") as n2:
            e.eng._nudge_within_ttl(fresh_run, e.step, "tok", e.ttl())
        assert not n2.called, "新 run 对象把计数清零 → 同档位再次推送"


def test_nudge_does_not_consume_token():
    """提醒不能动 token：否则一次提醒就作废钥匙。"""
    with _Env() as e:
        before = e.lm.get_approval_token("wf_n", "s2", requester="engineer")
        e.age_to(130)
        with patch.object(e.eng, "_nudge_assigner"):
            e.eng._nudge_within_ttl(e.run, e.step, "tok", e.ttl())
        after = e.lm.get_approval_token("wf_n", "s2", requester="engineer")
        assert before == after and after, "提醒消费/改写了 token"


def test_backoff_has_five_slots():
    """档位覆盖整个 TTL 窗口（2/5/10/20/40min），不能只有头两档。"""
    assert StepHandlerMixin._NUDGE_BACKOFF_SEC == (120, 300, 600, 1200, 2400), \
        f"退避排程异常: {StepHandlerMixin._NUDGE_BACKOFF_SEC}"


if __name__ == "__main__":
    tests = [
        ("2 分钟内不提醒", test_no_nudge_before_two_minutes),
        ("2 分钟后提醒一次", test_nudge_fires_after_two_minutes),
        ("同档位不重复", test_backoff_does_not_repeat_same_slot),
        ("计数跨 run 对象保留", test_counter_survives_new_run_object),
        ("提醒不消费 token", test_nudge_does_not_consume_token),
        ("退避档位完整", test_backoff_has_five_slots),
    ]
    passed = failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print(f"  OK  {name}")
        except AssertionError as ex:
            failed += 1
            print(f"  X   {name}: {ex}")
        except Exception as ex:
            failed += 1
            print(f"  E   {name}: {type(ex).__name__}: {ex}")
    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
