#!/usr/bin/env python3
"""thin wrapper — re-export from pipeflow.daemon"""
from pipeflow.daemon import daemon_loop, connect_feed, push_prompt_to_ccs, check_and_push
__all__ = ['daemon_loop', 'connect_feed', 'push_prompt_to_ccs', 'check_and_push']
