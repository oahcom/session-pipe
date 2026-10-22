"""返工任务的问题清单必须能拿到审查报告里的逐条明细。

事故（2026-10-22）：完成消息只写「CHANGES_REQUESTED 0P0/3P1/5P2/4P3」，
返工角色拿到的问题清单就是这一行 → 无法定位 → 整篇重做 → 复查按同样数量
再驳回（reject→rework→reject 死循环）。明细在 REVIEW-<实例>.md 的
「### 【业务问题】」/「### 【系统问题】」两段里，本用例锁住把它转成引擎认的分类段。
"""
import types

import pytest

import pipeflow.wf_lifecycle as wl

REVIEW = """# 审查报告

### 【业务问题】（返工流向：engineer）
**P1-1 | ADR-2 把 CLI 的 48.1% 归因为 new-input 口径，实测 16.4% | DESIGN.md:247**
**P2-1 | 并发读可能读到半文件，被源码证伪 | DESIGN.md:142**
### 【系统问题】（返工流向：maintainer）
**S-P1-1 | 规范名被兄弟链覆盖 | product_architect/GATE_REPORT.md:21**
"""


class _Engine(wl.WorkflowLifecycleMixin):
    def __init__(self):
        pass


@pytest.fixture()
def workspaces(tmp_path, monkeypatch):
    d = tmp_path / "reviewer"
    d.mkdir()
    (d / "REVIEW-wf_sub_abc123456789.md").write_text(REVIEW, encoding="utf-8")
    # 共享的最新报告：正文提到该实例，但文件名不带 id → 不得当作本链报告
    (d / "REVIEW.md").write_text("# 别的链\nwf_sub_abc123456789\n【业务问题】\n- P1 错的东西\n",
                                 encoding="utf-8")
    monkeypatch.setattr(wl, "_CCS_WORKSPACES", tmp_path)
    return tmp_path


def test_block_extracts_both_sections(workspaces):
    blk = _Engine()._review_issues_block("wf_sub_abc123456789")
    sys_items, biz_items, _, _ = wl.WorkflowLifecycleMixin._parse_review_issues(blk)
    assert len(biz_items) == 2, blk
    assert len(sys_items) == 1, blk
    assert "48.1%" in biz_items[0]


def test_unknown_instance_yields_nothing(workspaces):
    assert _Engine()._review_issues_block("wf_sub_nope") == ""


def test_shared_report_not_used(workspaces):
    """REVIEW.md 正文提到实例但文件名不带 id → 不能拿它当本链的明细。"""
    blk = _Engine()._review_issues_block("wf_sub_abc123456789")
    assert "错的东西" not in blk
