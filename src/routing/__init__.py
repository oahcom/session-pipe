"""routing 子包 — 路由逻辑。"""

from routing.router import Router, get_router, get_producers, get_consumers, priority, CATEGORY_DESC
from routing.routes import route_all, route_to_ccs, route_all_to_ccs, dispatch_investigator
from routing.rdb import RoutingDB, load_routing, save_routing, init_db

__all__ = [
    "Router", "get_router", "get_producers", "get_consumers", "priority", "CATEGORY_DESC",
    "route_all", "route_to_ccs", "route_all_to_ccs", "dispatch_investigator",
    "RoutingDB", "load_routing", "save_routing", "init_db",
]
