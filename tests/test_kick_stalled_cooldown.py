"""kick-stalled 冷却实证：4 用例 + 投递真伪。"""
import sys, json, time
sys.path.insert(0, '/home/administrator/session-pipeline/src')
from unittest.mock import MagicMock, patch
from pipeflow.engine import WorkflowEngine, Step, WorkflowDef

with patch.object(WorkflowEngine, "__init__", lambda s: None):
    eng = WorkflowEngine()
eng._lm = MagicMock(); eng.__dict__["_lifecycle"] = eng._lm; eng._bb = MagicMock()
now = time.time()
step = Step(id="s4", title="t", target_role="engineer", prompt_template="do it",
            exit_condition={}, type="handoff", max_retries=2)
eng._workflows = {"food_chain_loop": WorkflowDef(
    name="food_chain_loop", title="t", description="", steps=[step])}
eng._recently_producing = lambda r: False
eng._is_role_busy = lambda r, **k: False
sent, synced = [], []
eng._send_to_role = lambda role, msg, **kw: sent.append(kw.get("step_id")) or True
eng._sync_step_results = lambda wfid, sr: synced.append(json.loads(json.dumps(sr)))

def run(sr):
    sent.clear(); synced.clear()
    eng._lm.query = MagicMock(return_value=[{
        "instance_id": "wf_t", "template_id": "food_chain_loop",
        "current_step_id": "s4", "assignee": "engineer",
        "title": "T", "description": "D",
        "step_results": json.dumps(sr)}])
    eng._kick_stalled_roles()

cases = [
    ("无冷却戳（首次停滞）", {"status": "notified", "notified_at": now - 400}, 1),
    ("冷却戳=现在（刚推过）", {"status": "notified", "notified_at": now - 400,
                              "kick_stalled_at": now}, 0),
    ("冷却戳=599s前（冷却内）", {"status": "notified", "notified_at": now - 900,
                                "kick_stalled_at": now - 599}, 0),
    ("冷却戳=700s前（已过期）", {"status": "notified", "notified_at": now - 900,
                                "kick_stalled_at": now - 700}, 1),
    ("停滞不足180s", {"status": "notified", "notified_at": now - 100}, 0),
    ("status=completed（非 notified）", {"status": "completed", "notified_at": now - 900}, 0),
]
ok = True
for label, s, expect in cases:
    run({"s4": s})
    mark = "OK " if len(sent) == expect else "FAIL"
    if len(sent) != expect: ok = False
    print(f"  [{mark}] {label:28} 重推={len(sent)} 期望={expect}")
    if expect == 1 and synced:
        assert synced[-1]["s4"].get("kick_stalled_at"), "重推后必须落冷却戳"
print("\n落冷却戳验证: 通过（重推路径写 kick_stalled_at）" if ok else "\n存在失败用例")
