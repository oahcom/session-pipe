# read-only analysis of workflows.db (temporary, delete after use)
import sqlite3, json, time
from collections import Counter, defaultdict

c = sqlite3.connect('file:/home/administrator/.hermes/data/state/workflows.db?mode=ro', uri=True)
c.row_factory = sqlite3.Row
now = time.time()
cut = now - 86400
def fmt(ts): return time.strftime('%m-%d %H:%M:%S', time.localtime(ts)) if ts else '-'

print("NOW", fmt(now))
print("[R1] all logs of wf_e50bbe5664 and wf_3f71a2a78f76 (no action filter)")
for r in c.execute("""select workflow_instance_id,action,actor,substr(coalesce(detail,''),1,80) d,ts from workflow_logs
 where workflow_instance_id in ('wf_e50bbe5664','wf_3f71a2a78f76') order by ts"""):
    print("   %s|%s|%s|%s" % (fmt(r['ts']), r['action'], r['actor'], r['d']))

print()
print("[R2] reviewer_pr_review: every instance ever, with close count and age")
for r in c.execute("""select instance_id,status,parent_wf_id,created_at,completed_at,
 (select count(*) from workflow_logs l where l.workflow_instance_id=i.instance_id and l.action='wf_closed') closes
 from workflow_instances i where template_id='reviewer_pr_review' order by created_at desc limit 30"""):
    print("   %s st=%-16s parent=%-18s created=%s closes=%d" % (r['instance_id'], r['status'], r['parent_wf_id'], fmt(r['created_at']), r['closes']))

print()
print("[R3] total wf_closed logs by detail, last 24h")
for r in c.execute("""select substr(coalesce(detail,''),1,40) d, count(*) k from workflow_logs
 where action='wf_closed' and ts>=? group by 1 order by k desc""", (cut,)):
    print("   %d | %s" % (r['k'], r['d']))

print()
print("[R4] blocker bus writes referencing sub/delivery (engine side, via workflow_logs cross_role_send to maintainer?) -- skip")
print("[R5] recovered-then-reblocked signature: instances with recover_count>0")
tot = 0
for iid, st, sr in c.execute("select instance_id,status,step_results from workflow_instances where step_results like '%recover_count%'"):
    d = json.loads(sr or '{}')
    for sid, sv in d.items():
        if isinstance(sv, dict) and sv.get('recover_count'):
            tot += 1
            print("   %s st=%s step=%s recover_count=%s delivery_fail_count=%s status=%r" %
                  (iid, st, sid, sv.get('recover_count'), sv.get('delivery_fail_count'), sv.get('status')))
print("   total steps with recover_count>0 =", tot)

print()
print("[R6] CURRENT live loop check (last 5 min of logs)")
for r in c.execute("select workflow_instance_id,action,actor,substr(coalesce(detail,''),1,70) d,ts from workflow_logs where ts>=? order by ts desc limit 25", (now - 300,)):
    print("   %s|%s|%s|%s|%s" % (fmt(r['ts']), r['workflow_instance_id'], r['action'], r['actor'], r['d']))
