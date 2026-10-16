#!/usr/bin/env python3
"""survival_monitor.py — L1/L2/L3 三层存活检测。

L1: 进程存活（tmux has-session + 哨兵文件）
L2: 思考存活（pane 最后活动时间 + 9Router token 消耗）
L3: 产出存活（按角色 output_targets 分类查 bus 最近产出）

集成方式：routing_daemon 每 120s 调 tick()。
独立于 CCS 侧代码，纯外部观测。
"""
import json, logging, os, re, subprocess, sys, time, http.client, urllib.parse
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("session-pipeline.survival_monitor")

try:
    from paths import CCS_SENTINEL_DIR as SENTINEL_DIR, SESSION_ROLES_ROOT
    _ROLES_ROOT = SESSION_ROLES_ROOT
except ImportError:
    SENTINEL_DIR = Path("~/.hermes/run/ccs-sentinels")
    _ROLES_ROOT = Path.home() / "hermes-session-roles"

# 与 liveness.HEALTH_DIR 同一口子：测试/隔离场景可经 CCS_HEALTH_DIR 重定向。
# 原先 _write_health 硬编码真实目录，导致单测把 survival_* 写进生产
# ~/.hermes/run/ccs-health/{role}.json（实测 2026-10-16）。
HEALTH_DIR = Path(os.environ.get(
    "CCS_HEALTH_DIR", str(Path.home() / ".hermes" / "run" / "ccs-health")))
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


_ROLE_LIFECYCLES_CACHE: dict[str, str] = {}
_ROLE_LIFECYCLES_TS: float = 0


def _load_role_lifecycles() -> dict[str, str]:
    """加载各角色 lifecycle（infinite/ondemand），供 idle 回收判断。"""
    global _ROLE_LIFECYCLES_CACHE, _ROLE_LIFECYCLES_TS
    now = time.time()
    if now - _ROLE_LIFECYCLES_TS < 300 and _ROLE_LIFECYCLES_CACHE:
        return _ROLE_LIFECYCLES_CACHE
    cache: dict[str, str] = {}
    if PERSONAS_DIR.is_dir():
        for f in sorted(PERSONAS_DIR.glob("persona_*.json")):
            try:
                d = json.loads(f.read_text())
                name = d.get("name", f.stem)
                cache[name] = d.get("lifecycle", d.get("drive", "ondemand")) or "ondemand"
            except Exception:
                continue
    _ROLE_LIFECYCLES_CACHE = cache
    _ROLE_LIFECYCLES_TS = now
    return cache


class SurvivalMonitor:
    """三层存活检测器。"""

    def __init__(self, engine: Any = None):
        self._cache: dict[str, dict[str, Any]] = {}
        self._role_targets: dict[str, list[str]] = {}
        # 修复: _state 原未定义，R3 idle 回收每次抛 AttributeError 被外层吞掉（从未生效）
        self._state: dict[str, dict[str, Any]] = {}
        # 复用 engine._ensure_role_alive（实现 B：tmux 双验证 + 僵尸重启 + --drive infinite）
        # 而非内联 subprocess；无引擎则惰性构造单例（monitor 每 10min 才用到一次）
        self._engine = engine
        # L3 走进程内 bus 查询（read 子命令无 --src，见 _l3_check 注释）
        try:
            from hermes_bus.blackboard import Blackboard
            self._bb = Blackboard()
        except Exception as _e:
            LOGGER.warning("SurvivalMonitor: Blackboard 不可用，L3 产出检测降级: %s", _e)
            self._bb = None

    # ── L1: 进程存活 ──

    def _l1_check(self, role: str) -> dict:
        """L1: 进程存活。红线3 — 单次 tmux 探测失败(超时/socket抖动)不立即翻转状态,
        连续3次失败才降级判活/不活, 抑制 ccs_health 0↔9 全天噪声(bus#302580/#323191)。"""
        tmux_name = f"ccs-{role}"
        alive = False
        probe_ok = True
        try:
            r = subprocess.run(
                ["tmux", "has-session", "-t", tmux_name],
                capture_output=True, timeout=3,
            )
            alive = r.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            probe_ok = False
        prev = self._cache.get(role, {})
        prev_fail = prev.get("l1_fail_streak", 0)
        if not probe_ok:
            fail_streak = prev_fail + 1
            # socket 瞬时抖动/隔离: 探测命令本身异常(非returncode判定), 保留前态不翻转
            if fail_streak < 3:
                if prev.get("tmux_alive") is True:
                    # 前态存活: 沿用 ALIVE 结论, 不把9角色整体判死
                    has_sentinel = (SENTINEL_DIR / f"{role}.json").exists()
                    status = "ALIVE" if has_sentinel else prev.get("status", "UNKNOWN")
                    return {"status": status, "tmux_alive": True,
                            "has_sentinel": has_sentinel, "l1_fail_streak": fail_streak}
                if prev.get("status"):
                    # 前态非存活但有记录: 沿用前态(异常=无法探测, 非确认死亡), 抑制 ORPHAN 噪声
                    return {"status": prev["status"], "tmux_alive": None,
                            "has_sentinel": prev.get("has_sentinel", False),
                            "l1_fail_streak": fail_streak}
            alive = False  # 连续3次失败或无前态: 正常判活/不活
        else:
            fail_streak = 0
        sentinel = SENTINEL_DIR / f"{role}.json"
        has_sentinel = sentinel.exists()
        if alive and has_sentinel:
            status = "ALIVE"
        elif has_sentinel and not alive:
            # 单次 tmux 检查失败不立即判 ORPHAN（3 次连续失败才 kill，红线）
            status = "ORPHAN"
        elif alive and not has_sentinel:
            # tmux 会话在但哨兵缺失: 状态丢失, 需重建（DEAD 分支, 走 _cleanup_dead 删残留）
            status = "DEAD"
        else:
            status = "UNKNOWN"
        return {"status": status, "tmux_alive": alive,
                "has_sentinel": has_sentinel, "l1_fail_streak": fail_streak}

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

        # Blackboard 不可用时不能返回 producing=False——那会让调用方把"查不到"
        # 当成"没产出"从而误杀角色。返回 None 让 _overall 走"未知"分支。
        if self._bb is None:
            return {
                "producing": None,
                "age_seconds": None,
                "recent_count": 0,
                "categories_found": [],
                "targets_coverage": "?",
                "detail": "Blackboard 不可用，L3 不可判",
            }

        # 进程内查询 Blackboard：read 子命令无 --src（只有 --cat），走 CLI 必然
        # argparse 退出 2 → 恒返回 producing=False → 角色被误判 idle 击杀
        # （实测 2026-10-16）。Blackboard.read() 原生支持 src=，且 Fact.ts 是
        # 真实时间戳字段（旧代码找 created_at/timestamp 双重取不到）。
        # 同款修法见 role_lifecycle.py:374 _recently_producing。
        try:
            facts = self._bb.read(src=role, limit=5)
            if facts:
                recent_ts = max(float(getattr(f, "ts", 0) or 0) for f in facts)
                age = now - recent_ts
                producing = age < 600

                # 分类覆盖度
                found_cats = set(getattr(f, "cat", "") for f in facts)
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
            # WARNING 而非 debug：L3 是 idle 击杀的判据来源，异常必须可见
            LOGGER.warning("L3 产出检测异常 (%s): %s", role, e)
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
            # ORPHAN 但最近有 bus 产出 → 进程可能刚重启/检查瞬断，宽容为 idle 待观察
            if l3.get("producing") is True:
                return "idle"
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
            health_dir = HEALTH_DIR
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
            # R3: 注入 lifecycle（idle 回收判断用）
            _lifecycle = _load_role_lifecycles().get(role, "ondemand")
            result = {
                "l1": l1, "l1_ts": now,
                "l2": l2, "l2_ts": now if "thinking" in l2 else prev.get("l2_ts", 0),
                "l3": l3, "l3_ts": now if "producing" in l3 else prev.get("l3_ts", 0),
                "overall": overall,
                "lifecycle": _lifecycle,
            }
            self._cache[role] = result
            results[role] = result

            # 写健康数据到 HEALTH_DIR（默认 ~/.hermes/run/ccs-health/，可经 CCS_HEALTH_DIR 隔离）
            self._write_health(role,
                survival_overall=overall,
                survival_l2_thinking=l2.get("thinking"),
                survival_l3_producing=l3.get("producing"),
                survival_ts=now,
            )

            # 状态变更或异常时写 bus（接轨 eval_checker notice 模式）
            # ponytail: idle/stale 每 600s 最多报一次，避免海量噪声
            prev_overall = prev.get("overall", "unknown")
            _now_report_ts = time.time()
            _last_report = prev.get("last_report_ts", 0)
            if overall != prev_overall:
                self._write_bus("architecture",
                    f"[survival:{overall}] {role} L1={l1['status']} L2={l2.get('thinking')} L3={l3.get('producing')}",
                    evidence=f"prev={prev_overall} | {l2.get('detail','')} | {l3.get('detail','')}")
                result["last_report_ts"] = _now_report_ts
            elif overall in ("stale", "idle") and _now_report_ts - _last_report > 600:
                self._write_bus("notice",
                    f"@ccs-monitor [survival] {role} 状态: {overall}",
                    evidence=f"{l2.get('detail','')} | {l3.get('detail','')}")
                result["last_report_ts"] = _now_report_ts

            # 僵尸清理（仅 overall=orphan 且连续 3 次判定才 kill，防 tmux 瞬时故障误杀；
            # ORPHAN 但有产出降级 idle 时不走清理）
            prev_dead_checks = prev.get("dead_checks_since", 0)
            if overall == "dead" and l1.get("status") != "ORPHAN":
                dead_checks = prev_dead_checks + 1
                result["dead_checks_since"] = dead_checks
                self._cleanup_dead(role, dead_checks)
            elif overall == "orphan" and l1.get("status") == "ORPHAN":
                orphan_n = prev.get("orphan_checks_since", 0) + 1
                result["orphan_checks_since"] = orphan_n
                if orphan_n >= 3:
                    self._cleanup_orphan(role)
            else:
                result["dead_checks_since"] = 0
                result["orphan_checks_since"] = 0

        stalled = [r for r, h in results.items() if h["overall"] in ("stale", "dead")]
        orphaned = [r for r, h in results.items() if h.get("overall") == "orphan"]
        if stalled:
            LOGGER.warning("存活告警 stale/dead: %s", stalled)
            # D4 修复: 检测到 stale/dead 时尝试恢复（复用 engine._ensure_role_alive 拉起），
            # 冷却 600s 防重启风暴；不依赖人工介入。
            # 统一到实现 B（tmux 双验证 + 僵尸重启 + --drive infinite），不再内联 subprocess。
            now_ts = time.time()
            _last_rescue = getattr(self, "_last_rescue_ts", 0)
            if now_ts - _last_rescue > 600:
                try:
                    _eng = self._engine
                    if _eng is None:
                        from pipeflow.engine import WorkflowEngine
                        _eng = WorkflowEngine()
                        self._engine = _eng
                    for _r in stalled[:2]:
                        if _eng._ensure_role_alive(_r):
                            LOGGER.info("存活恢复: %s 已重新拉起", _r)
                        else:
                            LOGGER.warning("存活恢复失败 %s", _r)
                    self._last_rescue_ts = now_ts
                except Exception as _re:
                    LOGGER.warning("存活恢复动作异常（跳过本轮）: %s", _re)
        if orphaned and len(orphaned) <= 3:
            LOGGER.debug("orphan sessions (已清理): %s", orphaned)

        # R3 角色治理: idle+ondemand 角色超时自动回收（"只养不杀"→"能养能杀"）
        # 10 个 ondemand 角色常驻空转占 2.8GB（角色治理审查发现）。
        # ondemand 语义 = 用完退出；连 idle 累计 N 次（默认 6 次 = 12 分钟）自动 kill。
        # infinite 角色不回收（daemon 依赖常驻），仅 stale/dead 走上面的恢复。
        # FIX: 不回收有 pending 信号、L2 thinking 或 L3 producing 的角色——
        # "idle" 定义必须是无信号、无思考、无产出三无。
        try:
            _idle_roles = [r for r, h in results.items()
                           if h["overall"] == "idle" and h.get("lifecycle") == "ondemand"]
            for _ir in _idle_roles:
                _l1 = results[_ir].get("l1", {})
                _l2 = results[_ir].get("l2", {})
                _l3 = results[_ir].get("l3", {})
                # 有 pending 信号（L1 status=ALIVE 但有 unread）、或 L2 thinking、或 L3 producing → 不回收
                _has_pending = _l1.get("has_sentinel", False) and _l1.get("status") == "ALIVE"
                _thinking = _l2.get("thinking") is True
                _producing = _l3.get("producing") is True
                if _has_pending or _thinking or _producing:
                    # 有待处理工作 → 重置 idle_count，不回收
                    self._state.setdefault(_ir, {})["idle_count"] = 0
                    continue
                _prev = self._state.get(_ir, {})
                _idle_n = _prev.get("idle_count", 0) + 1
                self._state.setdefault(_ir, {})["idle_count"] = _idle_n
                if _idle_n >= 6:
                    LOGGER.info("角色回收: %s 连续 %d 次 idle（ondemand），kill session", _ir, _idle_n)
                    try:
                        subprocess.run(["tmux", "kill-session", "-t", f"ccs-{_ir}"],
                                capture_output=True, timeout=5)
                        # 删哨兵: 否则下轮 L1=ORPHAN 或降级 idle → idle_count 重新累积 → 无限 kill 循环
                        _sent = SENTINEL_DIR / f"{_ir}.json"
                        if _sent.exists():
                            _sent.unlink()
                        self._write_bus("notice",
                            f"@ccs-monitor [回收] {_ir} 连续 idle 6 次 → session+哨兵已回收",
                            evidence="ondemand 角色空转超时自动停止（R3 治理）")
                    except Exception as _ke:
                        LOGGER.warning("角色回收 kill 失败 %s: %s", _ir, _ke)
                    self._state.setdefault(_ir, {})["idle_count"] = 0
        except Exception as _ie:
            LOGGER.debug("idle 回收检查异常（非致命）: %s", _ie)

        LOGGER.debug("survival tick: %d roles, %d stale/dead, %d orphan",
                     len(results), len(stalled), len(orphaned))
        return results
