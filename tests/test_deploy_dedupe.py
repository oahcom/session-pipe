#!/usr/bin/env python3
"""部署通知去重：字节未变则不复制、不广播 [deploy]。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_deploy_dedupe.py -x -q
"""
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pipeflow.wf_lifecycle import WorkflowLifecycleMixin


def _engine(tmp, src_files, dest_files):
    eng_ws = tmp / "workflows" / "engineer" / "cli"
    deploy = tmp / ".hermes" / "bin"
    eng_ws.mkdir(parents=True)
    deploy.mkdir(parents=True)
    for name, body in src_files.items():
        (eng_ws / name).write_text(body)
    for name, body in dest_files.items():
        (deploy / name).write_text(body)
    obj = SimpleNamespace(_bb=MagicMock())
    return obj, eng_ws, deploy


def test_unchanged_files_produce_no_deploy_notification():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        obj, eng_ws, deploy = _engine(tmp, {"a.py": "X=1\n"}, {"a.py": "X=1\n"})
        with patch("pipeflow.wf_lifecycle._CCS_WORKSPACES", tmp / "workflows"), \
             patch.object(Path, "home", return_value=tmp):
            WorkflowLifecycleMixin._deploy_production_outputs(obj, "wf_test")
        obj._bb.write.assert_not_called()


def test_changed_file_deploys_and_notifies():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        obj, eng_ws, deploy = _engine(tmp, {"a.py": "X=2\n"}, {"a.py": "X=1\n"})
        with patch("pipeflow.wf_lifecycle._CCS_WORKSPACES", tmp / "workflows"), \
             patch.object(Path, "home", return_value=tmp):
            WorkflowLifecycleMixin._deploy_production_outputs(obj, "wf_test")
        assert (deploy / "a.py").read_text() == "X=2\n"
        obj._bb.write.assert_called_once()
        assert "a.py" in obj._bb.write.call_args[0][1]


if __name__ == "__main__":
    test_unchanged_files_produce_no_deploy_notification()
    test_changed_file_deploys_and_notifies()
    print("SELF-CHECK PASS")
