#!/usr/bin/env python3
"""
Workflow Daemon — 监控 workflow 表，推送 prompt 给运行中的 CCS。

通过 feed socket 推送 prompt，利用 CCS 的 CLAUDE.md 中定义的 loop 行为
让 CCS 自动执行工作流步骤。

用法：
  python3 workflow_daemon.py --interval 30
"""

import json
import socket
import sys
import time
from pathlib import Path

# 路径设置
from paths import ensure_paths
ensure_paths()

from pipeflow.db import WorkflowDB

FEED_SOCKET = "/tmp/sister_bus_feed.sock"
INTERVAL = 30  # 秒


def connect_feed() -> socket.socket:
    """连接 feed socket。"""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect(FEED_SOCKET)
    s.sendall(b'{"cmd":"SUBSCRIBE","agent":"feed"}\n')
    return s


def push_prompt_to_ccs(role: str, prompt: str):
    """通过 feed socket 推送 prompt 给运行中的 CCS。"""
    msg = json.dumps({
        "cmd": "PUBLISH",
        "to": role,
        "msg": {
            "event": "workflow_prompt",
            "cat": "workflow",
            "title": f"[workflow] {role} 执行工作流步骤",
            "src": "workflow_daemon",
            "prompt": prompt,
        }
    }) + "\n"
    try:
        s = connect_feed()
        s.sendall(msg.encode())
        s.close()
        print(f"[daemon] ✅ 推送 workflow prompt 给 {role}")
        return True
    except Exception as e:
        print(f"[daemon] ❌ 推送给 {role} 失败: {e}")
        return False


def check_and_push():
    """检查待处理工作流并推送 prompt。"""
    db = WorkflowDB()
    try:
        pending = db.list_workflows(status="pending", limit=10)
        print(f"[daemon] 检查到 {len(pending)} 个待处理工作流")
        for wf in pending:
            assignee = wf['assignee']
            instance_id = wf['instance_id']
            template_id = wf.get('template_id')

            # 从模板获取步骤
            if template_id:
                template = db.get_template(template_id)
                if template:
                    raw = json.loads(template['steps_json'] or '{}')
                    if isinstance(raw, list):
                        raw = {"steps": raw}
                    if not raw.get('steps'):
                        continue
                    first_step = raw['steps'][0]
                    prompt = first_step.get('prompt_template', '')

                    # 替换变量
                    context = json.loads(wf.get('context') or '{}')
                    for k, v in context.items():
                        prompt = prompt.replace(f'{{{k}}}', str(v))

                    # 推送 prompt
                    if push_prompt_to_ccs(assignee, prompt):
                        # 更新状态为 running
                        db.update_workflow(instance_id, status='running', current_step_id=first_step.get('id'))
                        print(f"[daemon] 📤 workflow {instance_id} → {assignee}")
    finally:
        db.close()


import os
import threading
import signal

_PID_FILE = Path("/tmp/workflow-daemon.pid")

def _acquire_pid_lock() -> bool:
    """获取 PID 锁，防止双进程竞争。"""
    if _PID_FILE.exists():
        try:
            old_pid = int(_PID_FILE.read_text())
            os.kill(old_pid, 0)
            print(f"[daemon] ⚠️ 已有实例 PID={old_pid} 在运行")
            return False
        except (OSError, ValueError):
            _PID_FILE.unlink(missing_ok=True)
    _PID_FILE.write_text(str(os.getpid()))
    return True

def daemon_loop(interval: int):
    """守护进程主循环。"""
    if not _acquire_pid_lock():
        sys.exit(1)
    
    shutdown = False
    def _handler(s, f):
        nonlocal shutdown; shutdown = True
    
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)
    
    print(f"[daemon] 🚀 Workflow Daemon 启动 PID={os.getpid()}, 间隔 {interval}s")

    try:
        while not shutdown:
            try:
                check_and_push()
            except Exception as e:
                print(f"[daemon] ⚠️ 轮询异常: {e}")
            time.sleep(interval)
    finally:
        _PID_FILE.unlink(missing_ok=True)
        print("[daemon] 已停止")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Workflow Daemon")
    parser.add_argument("--interval", type=int, default=INTERVAL, help="轮询间隔秒数")
    args = parser.parse_args()

    daemon_loop(args.interval)
