"""服务名里的 @ 不是定向提及（pm #476416）。

`ccs@consumer` / `ccs@pm.service` 被当成 @consumer 后：route_all 用它收窄接收者
（订阅该分类的 pm 被丢掉），route_to_ccs 用它把非提及角色判成"不是给我的"。
实测 #476153 deployment_report 因此只有 consumer 收到，订阅者 pm 一条都没见过。

Run: python3 -m pytest tests/test_mention_boundary.py -q
"""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from routing.routes import MENTION_RE, mentions  # noqa: E402

# pm #476416 的原样标题（title 截断 100 字符，与 polling 同口径）
_DEPLOY_TITLE = ("DEPLOYMENT_REPORT: ccs@consumer MemoryMax 6G→8G 已 live 生效零进程丢失；"
                 "停/重启该 unit 仍是 NO-GO（12 角色全在其 cgroup）")


def test_service_name_is_not_a_mention():
    """ccs@consumer 里的 @ 前是标识符字符 s → 不算提及。"""
    assert mentions(_DEPLOY_TITLE) == [], f"服务名被当成提及: {mentions(_DEPLOY_TITLE)}"


def test_service_name_with_dot_suffix_is_not_a_mention():
    """ccs@pm.service：右边点号是服务名的一部分，整个词不拆出 @pm。"""
    assert mentions("ccs@pm.service 顶满 memory.max 6144MB") == []


def test_real_mention_at_line_start_and_after_space():
    assert mentions("@devops P1: unit 停不得") == ["devops"]
    assert mentions("升级: 见 @engineer 的回复") == ["engineer"]
    assert mentions("[@devops @ccs-monitor] 派单") == ["devops", "ccs-monitor"]


def test_mention_re_is_the_only_definition():
    """同一条正则只能有一份：两处实现 = 改一处漏一处。"""
    src = Path(__file__).resolve().parent.parent / "src"
    hits = [p for p in src.rglob("*.py")
            if "findall(r\"@([a-zA-Z_]" in p.read_text(encoding="utf-8", errors="ignore")]
    assert hits == [], f"还有裸 @ 正则未走 mentions(): {hits}"
    assert MENTION_RE.findall("@engineer 看这条") == ["engineer"]


def _route_all(title, consumers):
    from routing.routes import route_all
    router = MagicMock()
    router.routing = {"pm": {"consume": ["deployment_report"]},
                      "consumer": {"consume": ["*"]},
                      "devops": {"consume": ["code_fix"]}}
    msgs = [{"id": 476153, "category": "deployment_report", "text": title,
             "priority": 3, "consumers": consumers, "src": "optimizer"}]
    with patch("routing.routes.HEARTBEAT"), patch("routing.routes.METRICS"), \
         patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod.get_router", return_value=router), \
         patch("bus_protocol.Blackboard"):
        return route_all(consumer="test", dry_run=True)


def test_subscriber_kept_when_body_only_names_a_service():
    """#476153 回归：标题只有 ccs@consumer，不得把订阅者 pm 丢掉。"""
    got = _route_all(_DEPLOY_TITLE, ["pm", "consumer"])["details"][0]
    assert got["assigned"] == ["pm", "consumer"], f"订阅者被服务名挤掉: {got}"


def test_real_tag_still_narrows():
    """真提及仍要收窄，否则本修复退化成广播。"""
    got = _route_all("@devops P1: ccs@consumer 停不得", ["pm", "consumer", "devops"])["details"][0]
    assert "pm" not in got["assigned"], f"@devops 未收窄: {got}"
    assert "devops" in got["assigned"], f"定向接收者被丢弃: {got}"


def test_route_to_ccs_does_not_treat_service_name_as_directed():
    """route_to_ccs 的 _is_for_me：服务名不该让未订阅该分类的角色收到消息。

    修复前 directed=True 绕过分类过滤 → consumer 收到 deployment_report（它不订阅），
    pm 那条事实的 log 里正好只有 pipeline 与 consumer 两条 consume。
    """
    from routing.routes import route_to_ccs
    router = MagicMock()
    router.routing = {"consumer": {"consume": []}}
    msgs = [{"id": 476153, "category": "deployment_report", "text": _DEPLOY_TITLE,
             "priority": 3, "consumers": ["consumer"], "src": "devops"}]
    with patch("routing.routes.HEARTBEAT"), patch("routing.routes.METRICS"), \
         patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod.get_router", return_value=router):
        result = route_to_ccs("consumer", dry_run=True)
    assert result["total"] == 0, f"未订阅的角色仍因服务名收到消息: {result}"
