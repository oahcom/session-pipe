#!/usr/bin/env python3
"""审批密钥必须有一条「审批人自己拉取」的通道 —— 推送失败不等于凭证丢失。

现场（bus #427318/#427380/#427435，2026-10-21 02:35 TTL 到期）：
  wf_sub_f93f81010342/s4 type=review，assigner=engineer。
  _issue_approval_token 用 `ccs send` 推明文 token → 角色正在思考，tmux 拒绝注入
  → rc!=0 → 走 _bb_write_fallback 写 bus，token 被替换成 ***。
  审批人既读不到该事实（标题无 @engineer，且 cat=workflow 不在其订阅内），
  也拿不到密钥，唯一出路是 TTL+30min 后 daemon 自动 approve —— 质量闸门被绕过。
  全库 426 个已发放 token，approval_consumed_at 非空 = 0。

契约（缺一则重回「闸门形式化」）：
  1. 拉取接口只对审批人返回明文；其他角色一律 PermissionError
  2. 非 step_done_ready / 已消费 / 已过 TTL 的 token 不得被拉取
  3. 拉取不得消费 token（旁路读取），否则一次查看就作废
  4. 拉取返回剩余 TTL，审批人能看到还能等多久
  5. 降级 bus 事实的标题必须含 @<assigner>，否则 _addressed_to 恒 False

运行: cd ~/session-pipeline && python3 tests/test_approval_pull_lane.py
"""
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

_SRC = Path(__file__).resolve().parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from lifecycle.manager import LifecycleManager  # noqa: E402


_STEPS = [
    {"step_id": "s1", "title": "产出", "target_role": "engineer",
     "prompt_template": "do", "exit_condition": {"bus_category": "task_spec"}},
    # 第 2 步是 review → 审批人 = 第 1 步的 target_role 之外的 assigner。
    # 模板里 s2 的 target_role 必须不同于 assigner，否则 _get_assigner_for_step
    # 会按「自审」返回空，本用例的 token 根本不会签发。
    {"step_id": "s2", "title": "门禁", "target_role": "qa", "type": "review",
     "prompt_template": "gate", "exit_condition": {"bus_category": "test_report"}},
]


def _lm(path):
    lm = LifecycleManager("product_architect", db_path=path)
    lm.upsert_template("tpl", "模板", "描述", _STEPS,
                       trigger_scene=["t"], allowed_initiators=["engineer"],
                       allowed_executors=["engineer", "qa"])
    lm.start_wf("wf_pull", "s1", "tpl", initiator="engineer")
    return lm


class _Env:
    """临时库 + 已签发 token 的 s2；ccs send 恒失败（复现角色忙）。"""

    def __enter__(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.path = self.tmp.name
        self.tmp.close()
        self.lm = _lm(self.path)
        # s1(single) 完成 → 推进到 s2(review)；再完成 s2 才会签发 token。
        # 全程让 ccs send 失败，复现「角色忙 → 走 bus 降级」的生产路径。
        with patch("subprocess.run") as _sp:
            _sp.return_value = MagicMock(returncode=1, stderr=b"tmux send skipped")
            self.lm.complete_step("wf_pull", "s1")
            self.lm.complete_step("wf_pull", "s2")
        return self

    def __exit__(self, *a):
        self.lm.close()
        os.unlink(self.path)
        return False


def test_assigner_can_pull_token():
    """审批人可拉取明文 token —— 推送失败时的唯一可用路径。"""
    with _Env() as e:
        r = e.lm.get_approval_token("wf_pull", "s2", requester="engineer")
        assert r and len(r) >= 32, f"审批人拿不到 token: {r!r}"


def test_other_roles_are_denied():
    """非审批人拉取必须拒绝，否则 token 等于公开。"""
    with _Env() as e:
        for who in ("qa", "product_architect", "curator", ""):
            try:
                e.lm.get_approval_token("wf_pull", "s2", requester=who)
            except PermissionError:
                continue
            raise AssertionError(f"非审批人 {who!r} 竟然拿到了 token")


def test_pull_does_not_consume():
    """旁路读取：拉取后 token 仍在，审批人仍能 confirm。"""
    with _Env() as e:
        t1 = e.lm.get_approval_token("wf_pull", "s2", requester="engineer")
        t2 = e.lm.get_approval_token("wf_pull", "s2", requester="engineer")
        assert t1 == t2, "拉取消费了 token：查看一次就作废"
        assert e.lm.confirm_step("wf_pull", "s2", token=t1, approved=True, reason="ok")


def test_expired_token_is_not_returned():
    """TTL 已过 → 不得再返回明文（否则签名过期形同虚设）。"""
    with _Env() as e:
        sr = e.lm._parse_results(e.lm._get_wf_unsafe("wf_pull"))
        sr["s2"]["approval_token_ttl"] = time.time() - 1
        e.lm.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                     (json.dumps(sr), "wf_pull"))
        e.lm.commit()
        assert e.lm.get_approval_token("wf_pull", "s2", requester="engineer") is None, \
            "过期 token 仍被拉出：审批人会用死钥匙反复 confirm"


def test_consumed_token_is_not_returned():
    """已消费 → 不得返回。"""
    with _Env() as e:
        t = e.lm.get_approval_token("wf_pull", "s2", requester="engineer")
        e.lm.confirm_step("wf_pull", "s2", token=t, approved=True, reason="ok")
        assert e.lm.get_approval_token("wf_pull", "s2", requester="engineer") is None


def test_pull_reports_remaining_ttl():
    """审批人要能看到剩余时间，否则不知道是否还来得及。"""
    with _Env() as e:
        left = e.lm.approval_ttl_remaining("wf_pull", "s2")
        assert 3000 < left <= 3600, f"剩余 TTL 异常: {left}"


def test_fallback_title_is_addressable():
    """降级事实标题必须含 @<assigner>，否则 bus_client._addressed_to 恒 False。"""
    with _Env() as e:
        bb = MagicMock()
        with patch("hermes_bus.blackboard.Blackboard", return_value=bb):
            # 直接调用降级写入（production 路径：ccs send rc!=0 时）
            e.lm._bb_write_fallback("wf_pull", "s2", "tok_xyz", "prompt", "engineer")
        assert bb.write.called, "降级路径未写 bus"
        title = bb.write.call_args[0][1]
        assert "@engineer" in title, f"降级标题无人可寻址: {title!r}"


if __name__ == "__main__":
    tests = [
        ("审批人可拉取 token", test_assigner_can_pull_token),
        ("非审批人拒绝", test_other_roles_are_denied),
        ("拉取不消费", test_pull_does_not_consume),
        ("过期不返回", test_expired_token_is_not_returned),
        ("已消费不返回", test_consumed_token_is_not_returned),
        ("返回剩余 TTL", test_pull_reports_remaining_ttl),
        ("降级标题含 @审批人", test_fallback_title_is_addressable),
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
