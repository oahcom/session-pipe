#!/usr/bin/env python3
"""survival_monitor.py — L1/L2/L3 三层存活检测。

L1: 进程存活（tmux has-session + 哨兵文件）
L2: 思考存活（pane 最后活动时间 + 9Router token 消耗）
L3: 产出存活（按角色 output_targets 分类查 bus 最近产出）

集成方式：routing_daemon 每 120s 调 tick()。
独立于 CCS 侧代码，纯外部观测。
"""
import json, logging, os, subprocess, sys, time, http.client, urllib.parse
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("session-pipeline.survival_monitor")

try:
    from paths import CCS_SENTINEL_DIR as SENTINEL_DIR, SESSION_ROLES_ROOT
    _ROLES_ROOT = SESSION_ROLES_ROOT
except ImportError:
    SENTINEL_DIR = Path("~/.hermes/run/ccs-sentinels")
    _ROLES_ROOT = Path.home() / "hermes-session-roles"
PERSONAS_DIR = _ROLES_ROOT / "personas" / "session-roles"
BUS_SCRIPT = Path(os.environ.get(
    "BUS_CLIENT",
    str(Path.home() / ".hermes" / "scripts" / "bus_client.py"),
))
ROUTER_URL = os.environ.get("ROUTER_URL", "localhost:20128")

# ── 角色 output_targets 缓存 ──
_ROLE_TARGETS_CACHE: dict[str, list[str]] = {}
_ROLE_TARGETS_TS: float = 0

def _load_role_targets() -> dict[str, list[str]]:
    """从 persona JSON 加载每个角色的产出分类列表。"""
    global _ROLE_TARGETS_CACHE, _ROLE_TARGETS_TS
    now = time.time()
    if now - _ROLE_TARGETS_TS < 300 and _ROLE_TARGETS_CACHE:
        return _ROLE_TARGETS_CACHE
    cache: dict[str, list[str]] = {}
    if PERSONAS_DIR.is_dir():
        for f in sorted(PERSONAS_DIR.glob("persona_*.json")):
            try:
                d = json.loads(f.read_text())
                name = d.get("name", f.stem)
                cats = set()
                for t in d.get("output_targets", []):
                    m = re.search(r'cat=(\w+)', t)
                    if m:
                        cats.add(m.group(1))
                if cats:
                    cache[name] = sorted(cats)
            except Exception as e:
                LOGGER.debug("skip malformed persona %s: %s", f.name, e)
    _ROLE_TARGETS_CACHE = cache
    _ROLE_TARGETS_TS = now
    return cache


class SurvivalMonitor:
    """三层存活检测器。"""

    def __init__(self):
        self._cache: dict[str, dict[str, Any]] = {}
        self._role_targets: dict[str, list[str]] = {}

    # ── L1: 进程存活 ──

    def _l1_check(self, role: str) -> dict:
        tmux_name = f"ccs-{role}"
        alive = False
        try:
            r = subprocess.run(
                ["tmux", "has-session", "-t", tmux_name],
                capture_output=True, timeout=3,
            )
            alive = r.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            LOGGER.debug("tmux has-session check failed for %s", role)
        sentinel = SENTINEL_DIR / f"{role}.json"
        has_sentinel = sentinel.exists()
        if alive and has_sentinel:
            status = "ALIVE"
        elif has_sentinel and not alive:
            status = "ORPHAN"
        elif not has_sentinel:
            status = "UNKNOWN"
        else:
            status = "DEAD"
        return {"status": status, "tmux_alive": alive, "has_sentinel": has_sentinel}

    # ── L2: 思考存活 ──

    def _l2_check(self, role: str) -> dict:
        """L2: pane 最后活动时间 + 9Router token 消耗推断是否在推理。"""
        tmux_name = f"ccs-{role}"
        now = time.time()

        # 信号 1: tmux pane 最后活动时间
        # pane_activity = 0 表示 pane 从未活跃（tmux 返回 0），等同于无数据
        last_activity = 0.0
        try:
            r = subprocess.run(
                ["tmux", "list-panes", "-t", tmux_name, "-F", "#{pane_activity}"],
                capture_output=True, text=True, timeout=3,
            )
            if r.returncode == 0 and r.stdout.strip():
                val = r.stdout.strip().split("\n")[0]
                if val and val != "0":
                    last_activity = float(val)
        except (FileNotFoundError, ValueError, subprocess.TimeoutExpired):
            LOGGER.debug("tmux pane activity check failed for %s", role)
        # last_activity == 0 表示无数据（非 epoch 0），标记为 unknown(-1)
        pane_age = now - last_activity if last_activity > 0 else -1

        # 信号 2: 9Router token 消耗（最近 2 分钟）
        tokens_2m = 0
        try:
            conn = http.client.HTTPConnection(ROUTER_URL, timeout=3)
            conn.request("GET", f"/v1/token-usage?role={urllib.parse.quote(role)}&since_s=120")
            resp = conn.getresponse()
            if resp.status == 200:
                data = json.loads(resp.read().decode())
                tokens_2m = int(data.get("total_tokens", 0))
            conn.close()
        except Exception as e:
            LOGGER.debug("L2 token check failed for %s: %s", role, e)

        # 综合判断
        # pane_age == -1 时仅依赖 token；两项均未知时退化为 unknown 而非 stale
        pane_active = pane_age >= 0 and pane_age < 300
        has_token = tokens_2m > 0
        if pane_age < 0 and not has_token:
            thinking = None  # 两项均未知，让调用方用 L3 fallback
        else:
            thinking = has_token or pane_active

        return {
            "last_activity_epoch": last_activity,
            "pane_age_seconds": pane_age,
            "tokens_2m": tokens_2m,
            "thinking": thinking,
            "detail": f"pane_age={pane_age:.0f}s tokens_2m={tokens_2m}",
        }

    # ── L3: 产出存活 ──

    def _l3_check(self, role: str) -> dict:
        """L3: 按角色 output_targets 分类查 bus 最近产出。

        对每个 output_targets 中提取的 bus 分类，
        查最近 10 分钟是否有该分类的产出消息（src=该角色）。
        """
        self._role_targets = _load_role_targets()
        targets = self._role_targets.get(role, None)
        now = time.time()

        # 不指定分类逐个查
        try:
            # 查该角色最近 5 条产出消息
            r = subprocess.run(
                [sys.executable, str(BUS_SCRIPT), "read", "--src", role,
                 "--limit", "5", "--json"],
                capture_output=True, text=True, timeout=10,
            )
            if r.returncode == 0 and r.stdout.strip():
                facts = json.loads(r.stdout)
                if isinstance(facts, list) and facts:
                    recent_ts = max(
                        float(f.get("created_at", 0) or f.get("timestamp", 0))
                        for f in facts
                    )
                    age = now - recent_ts
                    producing = age < 600

                    # 分类覆盖度
                    found_cats = set(f.get("cat", "") for f in facts)
                    if targets:
                        covered = found_cats & set(targets)
                        coverage = f"{len(covered)}/{len(targets)}"
                    else:
                        coverage = f"{len(found_cats)}/?"

                    return {
                        "producing": producing,
                        "age_seconds": age,
                        "recent_count": len(facts),
                        "categories_found": sorted(found_cats),
                        "targets_coverage": coverage,
                        "detail": f"最近产出 {age:.0f}s 前, {len(facts)} 条, 覆盖 {coverage}",
                    }
            return {
                "producing": False,
                "age_seconds": None,
                "recent_count": 0,
                "categories_found": [],
                "targets_coverage": "0/?",
                "detail": "bus 无匹配产出",
            }
        except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError,
                OSError, IndexError, ValueError) as e:
            return {
                "producing": None,
                "age_seconds": None,
                "recent_count": 0,
                "categories_found": [],
                "targets_coverage": "?",
                "detail": f"L3 异常: {e}",
            }

    # ── 汇总 ──

    def _overall(self, l1: dict, l2: dict, l3: dict) -> str:
        if l1.get("status") == "DEAD":
            return "dead"
        if l1.get("status") == "ORPHAN":
            return "orphan"
        if l1.get("status") != "ALIVE":
            return "unknown"
        # thinking=None（两项信号均未知）→ 跳过 L2，让 L3 bus 产出定结论
        if l2.get("thinking") is False:
            return "stale"
        # L2 不确定时看 L3——如果最近有 bus 产出，仍算 healthy
        if l2.get("thinking") is None:
            if l3.get("producing") is True:
                return "healthy"
            if l3.get("producing") is False:
                return "idle"
            return "unknown"  # 两项信号均无数据
        if l3.get("producing") is False:
            return "idle"
        return "healthy"

    # ── 写 bus（接轨 eval_checker 模式）──

    def _write_bus(self, cat: str, text: str, *, evidence: str = ""):
        try:
            subprocess.run(
                [sys.executable, str(BUS_SCRIPT), "write", cat, text,
                 "--evidence", evidence[:500], "--src", "survival_monitor"],
                capture_output=True, timeout=10,
            )
        except Exception as e:
            LOGGER.warning("bus write fail: %s", e)

    # ── 写哨兵 health（接轨 watchdog/tracker 模式）──

    def _write_health(self, role: str, **kwargs):
        try:
            health_dir = Path.home() / ".hermes" / "run" / "ccs-health"
            health_dir.mkdir(parents=True, exist_ok=True)
            path = health_dir / f"{role}.json"
            health = {}
            if path.exists():
                health.update(json.loads(path.read_text()))
            health.update(kwargs)
            path.write_text(json.dumps(health))
        except Exception as e:
            LOGGER.warning("health write fail: %s", e)

    # ── 僵尸清理 ──

    def _cleanup_dead(self, role: str, checks_since: int) -> None:
        """连续 N 次 DEAD → 删除过期哨兵文件，释放状态。"""
        if checks_since < 2:
            return
        sentinel = SENTINEL_DIR / f"{role}.json"
        try:
            if sentinel.exists():
                sentinel.unlink()
                LOGGER.info("[survival] 清理 DEAD 哨兵: %s (连续 %d 次检测)", role, checks_since)
                self._write_bus("architecture",
                    f"[survival:cleanup] {role} 哨兵已删除 (连续 {checks_since} 次 DEAD)",
                    evidence=f"survival_dead={checks_since}次")
        except OSError as e:
            LOGGER.warning("[survival] 清理哨兵失败 %s: %s", role, e)

    def _cleanup_orphan(self, role: str) -> None:
        """ORPHAN 状态: 哨兵存在但 tmux 无响应 → kill session + 删除哨兵。"""
        tmux_name = f"ccs-{role}"
        sentinel = SENTINEL_DIR / f"{role}.json"
        try:
            subprocess.run(
                ["tmux", "kill-session", "-t", tmux_name],
                capture_output=True, timeout=5,
            )
            LOGGER.debug("[survival] 清理 ORPHAN session: %s", role)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            LOGGER.debug("[survival] ORPHAN session 不存在: %s", tmux_name)
        try:
            if sentinel.exists():
                sentinel.unlink()
        except OSError:
            pass
        self._write_bus("architecture",
            f"[survival:cleanup] {role} ORPHAN session + 哨兵已清理",
            evidence="orphan_cleanup")

    # ── 入口 ──

    def tick(self) -> dict[str, dict]:
        if not SENTINEL_DIR.is_dir():
            return {}
        roles = []
        for f in SENTINEL_DIR.glob("*.json"):
            try:
                d = json.loads(f.read_text())
                roles.append(d.get("role", f.stem))
            except (json.JSONDecodeError, OSError):
                continue
        now = time.time()
        results = {}
        for role in roles:
            prev = self._cache.get(role, {})
            l1 = self._l1_check(role)
            l2 = (self._l2_check(role) if l1["status"] == "ALIVE"
                  and now - prev.get("l2_ts", 0) > 120 else prev.get("l2", {}))
            l3 = (self._l3_check(role) if l2.get("thinking") is not False
                  and now - prev.get("l3_ts", 0) > 300 else prev.get("l3", {}))
            overall = self._overall(l1, l2, l3)
            result = {
                "l1": l1, "l1_ts": now,
                "l2": l2, "l2_ts": now if "thinking" in l2 else prev.get("l2_ts", 0),
                "l3": l3, "l3_ts": now if "producing" in l3 else prev.get("l3_ts", 0),
                "overall": overall,
            }
            self._cache[role] = result
            results[role] = result

            # 写健康数据到 /tmp/ccs-health/（接轨 sentinel health 系统）
            self._write_health(role,
                survival_overall=overall,
                survival_l2_thinking=l2.get("thinking"),
                survival_l3_producing=l3.get("producing"),
                survival_ts=now,
            )

            # 状态变更或异常时写 bus（接轨 eval_checker notice 模式）
            prev_overall = prev.get("overall", "unknown")
            if overall != prev_overall:
                self._write_bus("architecture",
                    f"[survival:{overall}] {role} L1={l1['status']} L2={l2.get('thinking')} L3={l3.get('producing')}",
                    evidence=f"prev={prev_overall} | {l2.get('detail','')} | {l3.get('detail','')}")
            elif overall in ("stale", "idle"):
                self._write_bus("notice",
                    f"@ccs-monitor [survival] {role} 状态异常: {overall}",
                    evidence=f"{l2.get('detail','')} | {l3.get('detail','')}")

            # 僵尸清理
            prev_dead_checks = prev.get("dead_checks_since", 0)
            if overall == "dead" and l1.get("status") != "ORPHAN":
                dead_checks = prev_dead_checks + 1
                result["dead_checks_since"] = dead_checks
                self._cleanup_dead(role, dead_checks)
            elif l1.get("status") == "ORPHAN":
                self._cleanup_orphan(role)
            else:
                result["dead_checks_since"] = 0

        stalled = [r for r, h in results.items() if h["overall"] in ("stale", "dead")]
        orphaned = [r for r, h in results.items() if h.get("overall") == "orphan"]
        if stalled:
            LOGGER.warning("存活告警 stale/dead: %s", stalled)
        if orphaned and len(orphaned) <= 3:
            LOGGER.debug("orphan sessions (已清理): %s", orphaned)
        LOGGER.debug("survival tick: %d roles, %d stale/dead, %d orphan",
                     len(results), len(stalled), len(orphaned))
        return results
