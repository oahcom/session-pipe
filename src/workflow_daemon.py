#!/usr/bin/env python3
"""thin wrapper — re-export from pipeflow.daemon"""
import sys
from pathlib import Path
from pipeflow.daemon import daemon_loop, connect_feed, push_prompt_to_ccs, check_and_push
__all__ = ['daemon_loop', 'connect_feed', 'push_prompt_to_ccs', 'check_and_push']

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Workflow Daemon")
    parser.add_argument("--interval", type=int, default=30, help="轮询间隔秒数")
    args = parser.parse_args()
    daemon_loop(args.interval)
