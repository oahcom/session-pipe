"""人造 persona 目录 —— 把 contract_updater 的外部依赖从判据里摘出去。

真 personas 在 ~/hermes-session-roles/personas/session-roles，是**仓库外部**依赖。
干净导出树（CI / /tmp 复核）里没有它 → 别名表只剩 _ROLE_DRIFT_PATCH → 英文
"engineer" 解析成空 → 5 条 contract_updater 用例假红（2026-10-22 实测：同一份代码
在 ~/session-pipeline 12 passed、在 /tmp 导出树 5 failed，差异只来自目录布局）。

判据不该依赖「仓库放在哪」：需要 persona 的用例显式换成 fixture 目录。
"""
import contextlib
import json
import shutil
import tempfile
from pathlib import Path

ROLES = {"engineer": "开发者", "coordinator": "协调者", "maintainer": "运维者",
         "qa": "测试者", "lr": "技术决策者"}


@contextlib.contextmanager
def persona_dir():
    """临时 persona 目录；退出时把模块级缓存还原。"""
    import contract_updater as cu
    d = Path(tempfile.mkdtemp(prefix="personas_"))
    for name, title in ROLES.items():
        (d / f"persona_{name}.json").write_text(
            json.dumps({"name": name, "title": title}, ensure_ascii=False), encoding="utf-8")
    old_dir, old_alias_dir = cu.PERSONAS_DIR, cu._ALIAS_DIR
    cu.PERSONAS_DIR, cu._ALIAS_DIR = d, None
    cu._ROLE_ALIAS.clear()
    try:
        yield d
    finally:
        cu.PERSONAS_DIR, cu._ALIAS_DIR = old_dir, old_alias_dir
        cu._ROLE_ALIAS.clear()
        shutil.rmtree(d, ignore_errors=True)
