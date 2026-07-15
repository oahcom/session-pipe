#!/usr/bin/env python3
"""thin wrapper — re-export from routing.router"""
from routing.router import Router, get_router, _parse_produce_categories, _parse_consume_categories, priority, CATEGORY_PRIORITY, CATEGORY_DESC

__all__ = ['Router', 'get_router', '_parse_produce_categories', '_parse_consume_categories', 'priority', 'CATEGORY_PRIORITY', 'CATEGORY_DESC']
