#!/usr/bin/env python3
"""survival_monitor.py — L1/L2/L3/L4 四层存活检测。

L1: 进程存活（一次 pane 快照 + 哨兵文件）
L2: 思考存活（tmux pane/window 最后活动时间）
L3: 产出存活（按角色 output_targets 分类查 bus 最近产出）
L4: 落盘/工作流进展（transcript 最近写入 + running 步骤 completed_at/bus_anchor）

集成方式：routing_daemon 每 120s 调 tick()。
独立于 CCS 侧代码，纯外部观测。
"""
import json, logging, os, re, sqlite3, subprocess, sys, time
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("session-pipeline.survival_monitor")

from liveness import tmux_snapshot   # 单次 pane 快照，L1 与 pipeflow 共用一份判据

try:
    from paths import CCS_SENTINEL_DIR as SENTINEL_DIR, SESSION_ROLES_ROOT, WORKFLOWS_DB
    _ROLES_ROOT = SESSION_ROLES_ROOT
except ImportError:
    SENTINEL_DIR = Path("~/.hermes/run/ccs-sentinels")
    _ROLES_ROOT = Path.home() / "hermes-session-roles"
    WORKFLOWS_DB = Path.home() / ".hermes" / "state" / "workflows.db"

# 与 liveness.HEALTH_DIR 同一口子：测试/隔离场景可经 CCS_HEALTH_DIR 重定向。
# 原先 _write_health 硬编码真实目录，导致单测把 survival_* 写进生产
# ~/.hermes/run/ccs-health/{role}.json（实测 2026-10-16）。
HEALTH_DIR = Path(os.environ.get(
    "CCS_HEALTH_DIR", str(Path.home() / ".hermes" / "run" / "ccs-health")))
TRANSCRIPT_ROOT = Path(os.environ.get(
    "CCS_TRANSCRIPT_ROOT", str(Path.home() / ".claude" / "projects")))

# L4 缓存 TTL / 证据窗口：与 _l2/_l3 的 120/300 同族但独立可调。
L4_CACHE_TTL_S = 120
L4_WORK_WINDOW_S = 900
# L3 缓存 TTL：pane 静默时更快刷新，避免拿旧值判 stale。
L3_CACHE_TTL_ACTIVE_S = 120
L3_CACHE_TTL_IDLE_S = 300
PERSONAS_DIR = _ROLES_ROOT / "personas" / "session-roles"
BUS_SCRIPT = Path(os.environ.get(
    "BUS_CLIENT",
    str(Path.home() / ".hermes" / "scripts" / "bus_client.py"),
))
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
        """L1: 进程存活 —— liveness 快照里有没有 `ccs-<role>` 这个 pane。

        返回的 status：ALIVE(有 pane 有哨兵) / ORPHAN(只剩哨兵) / DEAD(只剩 pane) /
        UNKNOWN(pane 与哨兵俱无，或探针不可用)。UNKNOWN 一律不判死、不进 orphan 清理；
        恢复队列只排除探针不可用（tmux_alive=None）的那种，pane 与哨兵俱无的 infinite
        角色仍按「从未启动」拉起（把「探不到」当成「死了」会误杀，历史见 §8）。
        """
        has_sentinel = (SENTINEL_DIR / f"{role}.json").exists()
        snap = tmux_snapshot()
        if snap is None:
            return {"status": "UNKNOWN", "tmux_alive": None,
                    "has_sentinel": has_sentinel}
        alive = role in snap
        if alive and has_sentinel:
            status = "ALIVE"
        elif has_sentinel:
            status = "ORPHAN"      # 哨兵残留：kill 仍要 dead/orphan_checks_since 连击
        elif alive:
            status = "DEAD"        # 会话在但哨兵缺失 = 状态丢失，走 _cleanup_dead
        else:
            status = "UNKNOWN"
        return {"status": status, "tmux_alive": alive,
                "has_sentinel": has_sentinel}

    # ── L2: 思考存活 ──

    def _l2_check(self, role: str) -> dict:
        """L2: pane 最后活动时间推断是否在推理。

        ponytail: 原第二信号（9Router /v1/token-usage）实测 404 —— 该 endpoint
        不存在，tokens_2m 恒为 0、has_token 恒 False，对判据零贡献。

        删除并非逐格等价：真值表有 3 格不同（均需 has_token=True，即
        pane_age=-1/300/600 且 tokens_2m>0 时旧=True 新=False）。因 endpoint
        404 使 has_token 恒 False，这 3 格现实中不可达，故实际行为不变。
        若该 endpoint 将来真的实现，须恢复"token 活跃也算 thinking"的语义
        —— 届时 stale 判定会更严（无输出但有 token 消耗的角色会被判 stale）。
        """
        now = time.time()
        # 复用 liveness 的同一份快照：每角色一次 list-panes 在 19 个角色 × 每 tick 下
        # 就是 19 次子进程，而快照里第 3 列就是 #{window_activity}（tmux 3.4 的
        # #{pane_activity} 恒空，故用 window_activity；ccs-* 单 window 单 pane，
        # 它即该 pane 最后一次输出时间，实测随 send-keys 递增）。
        last_activity = 0.0
        snap = tmux_snapshot()
        if snap is not None:
            _ent = snap.get(role)
            if _ent:
                try:
                    last_activity = float(_ent[1] or 0)
                except (TypeError, ValueError):
                    last_activity = 0.0
        # last_activity == 0 表示无数据（非 epoch 0），标记为 unknown(-1)
        pane_age = now - last_activity if last_activity > 0 else -1
        # pane_age < 0 表示探测不可用，退化为 unknown 让调用方走 L3 fallback
        thinking = None if pane_age < 0 else pane_age < 300

        return {
            "last_activity_epoch": last_activity,
            "pane_age_seconds": pane_age,
            "thinking": thinking,
            "detail": f"pane_age={pane_age:.0f}s",
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

    # ── L4: 落盘/工作流进展 ──

    def _l4_check(self, role: str) -> dict:
        """L4: 不依赖 bus 的“有活”证据。

        L3 只看角色自己写 bus；长测试、长文件编辑期间零 bus 写入会被误判 stale。
        这里补两类廉价证据：transcript 最近写入 + 该角色 running workflow 的
        最近活性戳。任一在 900s 内即视为有活；查询失败返回 None，不伪造 False。
        """
        now = time.time()
        transcript_age = None
        errors: list[str] = []
        transcript_root = (
            TRANSCRIPT_ROOT / f"-home-administrator-ccs-workspaces-{role.replace('_', '-')}"
        )
        try:
            if transcript_root.is_dir():
                latest = max(
                    (p.stat().st_mtime for p in transcript_root.glob("*.jsonl")),
                    default=0.0,
                )
                if latest:
                    transcript_age = now - latest
        except OSError as e:
            transcript_age = None
            errors.append(f"transcript_probe_failed: {e}")
            LOGGER.warning("L4 transcript 探测失败 (%s): %s", role, e)

        workflow_age = None
        try:
            if not WORKFLOWS_DB.exists():
                workflow_age = None
            else:
                uri = f"file:{WORKFLOWS_DB.as_posix()}?mode=ro"
                con = sqlite3.connect(uri, uri=True, timeout=3)
                try:
                    rows = con.execute(
                        "SELECT created_at, step_results FROM workflow_instances "
                        "WHERE status='running' AND assignee=?",
                        (role,),
                    ).fetchall()
                finally:
                    con.close()
                stamps = []
                for _created_at, raw in rows:
                    try:
                        data = json.loads(raw or "{}")
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if not isinstance(data, dict):
                        continue
                    for sdata in data.values():
                        if not isinstance(sdata, dict):
                            continue
                        for key, value in sdata.items():
                            # 只认角色推进产出的事实。notified_at / poll_since /
                            # kick_stalled_at 是引擎每轮刷新/催办写的——把它们算作
                            # “有活”会让卡得越死越被判 healthy，stale 永久失效。
                            # bus_anchor 在“推进到下一步”时也会被写成 now（等同
                            # notified_at），只有本步已有 exit_messages 才说明真匹配过产出。
                            if key == "completed_at":
                                if isinstance(value, (int, float)) and value > 0:
                                    stamps.append(float(value))
                            elif key == "bus_anchor" and sdata.get("exit_messages"):
                                if isinstance(value, (int, float)) and value > 0:
                                    stamps.append(float(value))
                if stamps:
                    workflow_age = now - max(stamps)
        except (OSError, sqlite3.Error, ValueError) as e:
            workflow_age = None
            errors.append(f"workflow_probe_failed: {e}")
            LOGGER.warning("L4 workflow 查询失败 (%s): %s", role, e)

        ages = [a for a in (transcript_age, workflow_age) if a is not None]
        work_evidence = bool(ages) and min(ages) < L4_WORK_WINDOW_S
        result = {
            "available": bool(ages),
            "work_evidence": work_evidence if ages else None,
            "transcript_age_seconds": transcript_age,
            "workflow_age_seconds": workflow_age,
            "detail": (f"transcript_age={transcript_age if transcript_age is None else round(transcript_age)}s "
                       f"workflow_age={workflow_age if workflow_age is None else round(workflow_age)}s"),
        }
        if errors:
            # 与 L3 的异常 WARNING 对称：查询失败 ≠ 无证据，调用方据此不误杀。
            result["error"] = "; ".join(errors)
        return result

    # ── 汇总 ──

    def _overall(self, l1: dict, l2: dict, l3: dict, l4: dict | None = None) -> str:
        if l1.get("status") == "DEAD":
            return "dead"
        if l1.get("status") == "ORPHAN":
            # ORPHAN 但最近有 bus 产出 → 进程可能刚重启/检查瞬断，宽容为 idle 待观察
            if l3.get("producing") is True:
                return "idle"
            return "orphan"
        if l1.get("status") != "ALIVE":
            return "unknown"
        work_evidence = bool((l4 or {}).get("work_evidence"))
        l4_unavailable = (l4 or {}).get("work_evidence") is None and bool((l4 or {}).get("error"))
        # thinking=None（两项信号均未知）→ 跳过 L2，让 L3/L4 定结论
        if l2.get("thinking") is False:
            # pane 静默只说明「此刻没有输出」，而 infinite 常驻角色的空闲是正常态；
            # L3 是落盘事实（bus 产出 600s 内），L4 补 transcript/workflow 进展。
            # 不能把「L3 不可判」当成「没产出」：那会把查不到误杀成 stale。
            # L4 查询失败同理：证据通道故障 ≠ 无证据，交给 unknown 兜底。
            if l3.get("producing") is True or work_evidence:
                return "healthy"
            if l3.get("producing") is None or l4_unavailable:
                return "unknown"
            return "stale"
        # L2 不确定时看 L3/L4——最近有落盘或工作流进展，仍算 healthy
        if l2.get("thinking") is None:
            if l3.get("producing") is True or work_evidence:
                return "healthy"
            if l3.get("producing") is False:
                return "idle"
            return "unknown"  # 信号均无数据
        if l3.get("producing") is False and not work_evidence:
            return "idle"
        return "healthy"

    # ── 写 bus（接轨 eval_checker 模式）──

    def _write_bus(self, cat: str, text: str, *, evidence: str = ""):
        try:
            subprocess.run(
                [sys.executable, str(BUS_SCRIPT), "write", cat, text,
                 "--evidence", evidence[:500], "--src", "survival_monitor"],
                # 10s 低于实测 bus 事务持锁（40min 窗口内 5 次，最长 9.8s）→
                # 10:57 连续 3 次写失败，存活/死亡事实没落盘。取 30s 留 3 倍余量。
                capture_output=True, timeout=30,
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

    def _rescue_failed_units(self) -> None:
        """ccs@*.service 处于 failed（熔断）→ reset-failed 解除限流。

        熔断的单元 tmux 不存在、哨兵可能已被 _cleanup_orphan 清掉，故不在
        tick 的扫描范围内——只能按单元名主动探测。infinite 角色必须恢复；
        ondemand 角色失败不重启（与 ccs_monitor 的退出码语义保持一致）。
        """
        failed = self._list_failed_ccs_units()
        for role in failed:
            if not self._is_resident(role):
                continue
            try:
                subprocess.run(["systemctl", "--user", "reset-failed", f"ccs@{role}.service"],
                               capture_output=True, timeout=10)
                subprocess.run(["systemctl", "--user", "start", f"ccs@{role}.service"],
                               capture_output=True, timeout=15)
                LOGGER.info("[survival] 熔断恢复: ccs@%s 已 reset-failed + start", role)
                self._write_bus("monitor_audit",
                    f"熔断恢复: ccs@{role} 单元 failed → 已重置并拉起",
                    evidence="StartLimitBurst 触发后 start 被挡，reset-failed 是唯一恢复路径")
            except (subprocess.TimeoutExpired, OSError) as e:
                LOGGER.warning("[survival] 熔断恢复失败 %s: %s", role, e)

    @staticmethod
    def _list_failed_ccs_units() -> list[str]:
        """列出 failed 状态的 ccs@*.service 角色名。失败时返回空表。

        注意：`list-units --state=failed <glob>` 会把 glob 当单元名去精确匹配
        而返回空（实测 2026-10-16：有 failed 单元仍返回 []），必须列全量后自行过滤。
        """
        try:
            r = subprocess.run(
                ["systemctl", "--user", "list-units", "--state=failed", "--no-legend", "--plain"],
                capture_output=True, text=True, timeout=10,
            )
            if r.returncode != 0:
                return []
        except (subprocess.TimeoutExpired, OSError):
            return []
        roles = []
        for line in r.stdout.splitlines():
            unit = line.split()[0] if line.split() else ""
            if unit.startswith("ccs@") and unit.endswith(".service"):
                roles.append(unit[len("ccs@"):-len(".service")])
        return roles

    @staticmethod
    def _is_resident(role: str) -> bool:
        """角色是否应常驻。读不到时保守判 False（不重启），避免误拉起 ondemand。"""
        try:
            return _load_role_lifecycles().get(role) == "infinite"
        except Exception:
            return False

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
        # infinite 角色无论有无哨兵都必须巡检：哨兵由 ccs start 写入，从未启动过的
        # 角色没有哨兵 → 旧枚举把它们排除在外，与"健康"无法区分（实测 2026-10-20
        # 重启后 scout/optimizer/knowledge_curator/codex-dev/ccs-monitor 五个
        # infinite 角色无哨兵、无人拉起，巡检整整 36 分钟没看见它们）。
        for _r, _lc in _load_role_lifecycles().items():
            if _lc == "infinite" and _r not in roles:
                roles.append(_r)
        now = time.time()
        results = {}
        for role in roles:
            prev = self._cache.get(role, {})
            l1 = self._l1_check(role)
            l2 = (self._l2_check(role) if l1["status"] == "ALIVE"
                  and now - prev.get("l2_ts", 0) > 120 else prev.get("l2", {}))
            # pane 静默时更依赖 L3/L4 救场，因此 thinking=False 反而要刷新 L3，
            # 不能拿 300s 前的旧值（首轮甚至是空 dict）判 stale。
            _l3_ttl = (L3_CACHE_TTL_ACTIVE_S if l2.get("thinking") is False
                       else L3_CACHE_TTL_IDLE_S)
            l3 = (self._l3_check(role)
                  if now - prev.get("l3_ts", 0) > _l3_ttl else prev.get("l3", {}))
            l4 = (self._l4_check(role) if l1["status"] == "ALIVE"
                  and now - prev.get("l4_ts", 0) > L4_CACHE_TTL_S else prev.get("l4", {}))
            overall = self._overall(l1, l2, l3, l4)
            # R3: 注入 lifecycle（idle 回收判断用）
            _lifecycle = _load_role_lifecycles().get(role, "ondemand")
            result = {
                "l1": l1, "l1_ts": now,
                "l2": l2, "l2_ts": now if "thinking" in l2 else prev.get("l2_ts", 0),
                "l3": l3, "l3_ts": now if "producing" in l3 else prev.get("l3_ts", 0),
                "l4": l4, "l4_ts": now if "work_evidence" in l4 else prev.get("l4_ts", 0),
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
                survival_l4_work=l4.get("work_evidence"),
                survival_transcript_age=l4.get("transcript_age_seconds"),
                survival_workflow_age=l4.get("workflow_age_seconds"),
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

        orphaned = [r for r, h in results.items() if h.get("overall") == "orphan"]
        # idle = L3 产出存活为假（最近 600s 无 bus 产出）。原先它既不告警也不
        # 进 stalled 恢复队列 → 永久静默。实测：17 个角色中 10 个恒 idle，
        # "最近产出 7140s 前"无人处置，Bus 上只剩告警没人看。
        # 与 stale 的区别：idle 的进程/pane 是好的，缺的只是产出，所以处理
        # 手段是提醒而非重启（重启会清掉它可能正在推理的上下文）。
        stalled = [r for r, h in results.items() if h["overall"] in ("stale", "dead")]
        idled = [r for r, h in results.items() if h["overall"] == "idle"]
        # infinite 角色「从未启动」（无 tmux、无哨兵）时 l1=UNKNOWN，既不在 stalled
        # 也不在 orphan 清理范围，旧逻辑永不拉起。判据必须是 "未在跑" 的确凿证据
        # 而非 "未在跑 + 探不到"：探测失败时 tmux_alive=None，把它当成 "没在跑"
        # 会虚报重启（同类虚报见 2026-10-16 的 _was_alive 修复）。
        # _ensure_role_alive 自带 tmux 双验证 + 300s 冷却，仍活着的是 no-op。
        recoverable = stalled + [
            r for r, h in results.items()
            if r not in stalled and h.get("lifecycle") == "infinite"
            and h.get("l1", {}).get("tmux_alive") is False
        ]
        # 真正未在跑的角色优先占用恢复名额：stalled 多为 pane 静默（tmux 尚在），
        # 对 _ensure_role_alive 是 no-op，却会占满每轮 2 个名额，使缺角色的恢复
        # 无限延后（实测 2026-10-20：scout/reviewer 每轮占据名额，4 个未启动的
        # infinite 角色连续数轮未被处理）。
        recoverable.sort(key=lambda r: results.get(r, {}).get("l1", {})
                         .get("tmux_alive") is not False)
        # 节流 600s：与 stalled 的恢复冷却同量级。无节流时每 120s（tick 周期）
        # 重复告警同一批 idle 角色，长期运行持续产生日志噪音。
        if idled:
            _now_idle = time.time()
            if _now_idle - getattr(self, "_last_idle_warn_ts", 0) > 600:
                LOGGER.warning("存活告警 idle（无产出）: %s", idled)
                self._last_idle_warn_ts = _now_idle
        if recoverable:
            # 节流 600s：与 idled 分支及下方恢复冷却同量级。此前只有 idled 有节流，
            # stalled 每 120s（tick 周期）重复告警同一批角色，实测 60min 内 9 条。
            _now_stale = time.time()
            if stalled and _now_stale - getattr(self, "_last_stale_warn_ts", 0) > 600:
                LOGGER.warning("存活告警 stale/dead: %s", stalled)
                self._last_stale_warn_ts = _now_stale
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
                    for _r in recoverable[:2]:
                        # stale 的角色 L1 本就 ALIVE（只是 pane 静默），
                        # _ensure_role_alive 对其为 no-op 且返回 True。
                        # 不加区分地把两种 True 都记为"已重新拉起"＝日志虚报动作
                        # （实测 2026-10-16：qa/reviewer 会话创建时间未变却报已拉起）。
                        _was_alive = results.get(_r, {}).get("l1", {}).get("status") == "ALIVE"
                        if not _eng._ensure_role_alive(_r):
                            LOGGER.warning("存活恢复失败 %s", _r)
                            self._write_bus("monitor_audit",
                                f"存活恢复失败: {_r} 拉起未成功",
                                evidence=f"survival_overall=stale/dead, l1={results.get(_r,{}).get('l1',{}).get('status')}")
                        elif _was_alive:
                            LOGGER.info("存活恢复: %s 会话本就在（L1 存活，未重启）", _r)
                        else:
                            LOGGER.info("存活恢复: %s 已重新拉起", _r)
                            # 写 bus 留痕：恢复是外部可见动作，仅 LOGGER 则监控方无从得知
                            # （实测 2026-10-16 全库 0 条 "存活恢复" 记录）
                            self._write_bus("monitor_audit",
                                f"决策: 存活恢复 → {_r} 已重新拉起",
                                evidence=f"survival_overall={results.get(_r,{}).get('overall')}, "
                                         f"{results.get(_r,{}).get('l2',{}).get('detail','')}")
                    self._last_rescue_ts = now_ts
                except Exception as _re:
                    LOGGER.warning("存活恢复动作异常（跳过本轮）: %s", _re)
        if orphaned and len(orphaned) <= 3:
            LOGGER.debug("orphan sessions (已清理): %s", orphaned)

        # systemd 熔断恢复：ccs_monitor.py 以 exit(1) 退出后，若 5 次/300s 触发
        # StartLimitBurst，单元进入 failed 且**任何 start 都会被挡回**
        # （实测：start → activating → failed，计数器在窗口内不清零；
        #  reset-failed 是唯一恢复路径）。此时 tmux 会话不存在 → tick 扫不到该角色
        # → 既有的 stale/dead 恢复路径也不会碰它，形成永久静默死亡。
        # 只在单元确实 failed 时才动，避免干扰正常的 active 单元。
        try:
            self._rescue_failed_units()
        except Exception as _fe:
            LOGGER.warning("熔断恢复检查异常（跳过本轮）: %s", _fe)

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
