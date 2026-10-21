#!/usr/bin/env python3
"""价值门禁结论回归守卫：审计 FAIL 不得被收尾成 completed。

事故：wf_5f5eebc881（WL-food_chain_loop）于 05:29 收尾 status=completed，
而同期 lr/VALUE_AUDIT.md 首行是 FAIL（"无实质收益；s5 测另一条工作流的产物"）。
失败分支只 return 不落库，代码继续走子链扫描并以 completed 关闭 —— 父链状态
与审计结论直接矛盾，下游据此认为收益已达成。

判据只认「文件存在 + 结构化 FAIL」；文件缺失走原有语义（exit_schema required 兜底）。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_value_gate_verdict.py -q
"""
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.wf_lifecycle import _value_audit_failed, _resolve_value_gate_paths


def test_verdict_first_line_fail():
    assert _value_audit_failed("FAIL\n\n# 价值审计\n无实质收益")


def test_verdict_field_reject():
    assert _value_audit_failed("# 审计\nverdict: REJECT\n理由…")


def test_template_prose_not_verdict():
    """模板说明里提到 FAIL 不算结论（旧实现 "FAIL" in upper() 的正相反例）。"""
    assert not _value_audit_failed("# 价值审计模板\n- 判定 PASS 或 FAIL\n- verdict: PASS|FAIL")


def test_paths_split_by_comma():
    """value_gate.file 实际是逗号分隔多文件名（food_chain_loop 旧配置）。"""
    got = _resolve_value_gate_paths("A.md, B.md", "lr")
    assert [p[1] for p in got] == ["A.md", "B.md"]
    assert _resolve_value_gate_paths("", "lr") == []


def _drive_advance(audit_text: str | None) -> list:
    """真正调用 _advance_production_wf 的末步分支，返回 close_wf 收到的 status。"""
    from pipeflow.engine import WorkflowEngine, WorkflowDef, Step

    tmp = Path(tempfile.mkdtemp())
    if audit_text is not None:
        (tmp / "VALUE_AUDIT.md").write_text(audit_text, encoding="utf-8")

    eng = WorkflowEngine(workflows_dir=tmp)  # 空目录，不用生产模板
    wf = WorkflowDef(
        name="loop_tpl", title="t", description="d",
        steps=[Step(id="s7", title="价值审计", target_role="lr",
                    prompt_template="x", exit_condition={})],
        loop={"enabled": True, "max_iterations": 3,
              "value_gate": {"file": "VALUE_AUDIT.md", "workspace": str(tmp)}})
    eng._workflows = {"loop_tpl": wf}

    closed = []

    # 最小 lm：_conn_tx 返回可执行 SQL 的连接；close_wf 落在 lm 上
    # （_advance_production_wf 用的是 self._lifecycle.close_wf）
    class _Lm:
        _conn = MagicMock()
        def __enter__(self): return self
        def close_wf(self, wfid, status="completed", **_kw): closed.append(status)
        def _cascade_cancel_unsafe(self, wfid, reason=""): self.cascaded = True
        def _conn_tx(self):
            lm = self
            class _Ctx:
                def __enter__(self):
                    c = MagicMock()
                    c.execute.return_value.fetchone.return_value = {
                        "status": "running", "step_results": json.dumps({"s7": {"status": "step_done_ready"}})}
                    c.in_transaction = False
                    lm._c = c
                    return c
                def __exit__(self, *a): return False
            return _Ctx()

    eng._lm = _Lm()   # _lifecycle 是只读 property，底层字段是 _lm
    eng._advance_production_wf("wf_t", eng._lifecycle, "loop_tpl", "s7",
                               {"s7": {"status": "step_done_ready"}})
    return closed


def test_fail_verdict_closes_failed():
    assert _drive_advance("FAIL\n\n无实质收益\n") == ["failed"], "审计 FAIL 应以 failed 收尾"


def test_pass_verdict_closes_completed():
    assert _drive_advance("PASS\n\n收益已达成\n") == ["completed"], "审计 PASS 应正常完成"


if __name__ == "__main__":
    for fn in (test_verdict_first_line_fail, test_verdict_field_reject,
               test_template_prose_not_verdict, test_paths_split_by_comma,
               test_fail_verdict_closes_failed, test_pass_verdict_closes_completed):
        fn()
        print(f"OK {fn.__name__}")
