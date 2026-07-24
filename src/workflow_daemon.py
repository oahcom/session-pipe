#!/usr/bin/env python3
"""thin wrapper — re-export from pipeflow.daemon"""
from pipeflow.daemon import daemon_loop, INTERVAL
__all__ = ['daemon_loop']

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Workflow Daemon")
    parser.add_argument("--interval", type=int, default=INTERVAL, help="轮询间隔秒数")
    args = parser.parse_args()
    daemon_loop(args.interval)
