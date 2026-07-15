#!/usr/bin/env python3
"""thin wrapper — re-export from routing.auto"""
from routing.auto import route_all, route_to_ccs, route_all_to_ccs, dispatch_investigator, poll_unconsumed, status

__all__ = ['route_all', 'route_to_ccs', 'route_all_to_ccs', 'dispatch_investigator', 'poll_unconsumed', 'status']
