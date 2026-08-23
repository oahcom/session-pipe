#!/usr/bin/env python3
"""contract_updater.py — 从 eval_checker 历史记录分析并建议改进。

Usage:
    python3 contract_updater.py --from-file <历史记录 JSON 文件>
    cat <history.json> | python3 contract_updater.py
"""

import json, logging, os, subprocess, sys
from pathlib import Path

BUS_SCRIPT = Path(os.environ.get(
    "BUS_CLIENT",
    os.path.expanduser("~/.hermes/scripts/bus_client.py"),
))
LOGGER = logging.getLogger("session-pipeline.contract_updater")
PERSONAS_DIR = Path(os.environ.get(
    "SESSION_ROLES_DIR",
    str(Path(__file__).resolve().parent.parent.parent / "hermes-session-roles" / "personas" / "session-roles"),
))


def _apply_suggestion(suggestion: dict) -> bool:
    """应用一条改进建议到角色 JSON。

    目前支持 type=add_verification:
      - 读取角色 JSON
      - 在 eval_criteria 中按文本匹配条目
      - 若条目是纯自然语言字符串且 suggestion 提供了 verification_command:
        替换为 {"text":, "verifiable":, "expected":}
      - 写回文件
    返回 True 表示成功应用。
    """
    role = suggestion.get("role")
    criterion_text = suggestion.get("criterion")
    stype = suggestion.get("type")
    if stype != "add_verification" or not role or not criterion_text:
        return False

    # 找到对应的角色 JSON 文件
    target: dict | None = None
    target_path: Path | None = None
    for f in PERSONAS_DIR.glob("persona_*.json"):
        try:
            d = json.loads(f.read_text())
            if d.get("name") == role:
                target, target_path = d, f
                break
        except Exception:
            continue
    if target is None:
        return False

    # 按文本匹配 eval_criteria 条目
    criteria = target.get("eval_criteria", [])
    idx = None
    for i, entry in enumerate(criteria):
        if isinstance(entry, str) and criterion_text in entry:
            idx = i
            break
        if isinstance(entry, dict) and criterion_text in entry.get("text", ""):
            idx = i
            break
    if idx is None:
        return False

    entry = criteria[idx]
    if not isinstance(entry, str):
        return False  # 已结构化，跳过

    verification_cmd = suggestion.get("verification_command")
    if not verification_cmd:
        return False

    criteria[idx] = {
        "text": entry,
        "verifiable": verification_cmd,
        "expected": suggestion.get("expected", ""),
    }

    try:
        # 备份原文件
        if target_path.exists():
            backup = target_path.with_suffix(".json.bak")
            target_path.replace(backup)
        # 原子写（tempfile + rename）
        tmp = target_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(target, ensure_ascii=False, indent=2) + "\n")
        tmp.replace(target_path)
        return True
    except Exception:
        return False


def apply_all_suggestions(suggestions: list[dict]) -> dict:
    """批量应用建议。返回 {applied, failed, details}。"""
    applied = 0
    failed = 0
    details: list[dict] = []
    for s in suggestions:
        role = s.get("role", "unknown")
        try:
            ok = _apply_suggestion(s)
            if ok:
                applied += 1
            else:
                failed += 1
            details.append({"role": role, "success": ok, "error": "" if ok else "apply returned False"})
        except Exception as e:
            failed += 1
            details.append({"role": role, "success": False, "error": str(e)})
    return {"applied": applied, "failed": failed, "details": details}


def _has_output_schema(role_name: str) -> bool:
    """检查角色 JSON 是否定义了 output_schema。"""
    for f in PERSONAS_DIR.glob("persona_*.json"):
        try:
            d = json.loads(f.read_text())
            if d.get("name") == role_name:
                return bool(d.get("output_schema"))
        except Exception:
            continue
    return False


# 角色别名映射 (中文 title / 英文别称 → persona name)。
# WHY: eval_passrate 的 role_filter 可能是中文 title(维护者) 或英文(engineer)，
# review_gate 的 target 是文件名(user_intent_reminder.py) — 三者粒度不同，
# 需统一映射到 persona name 才能让 add_output_schema 对角色真生效。
_ROLE_ALIAS: dict[str, str] = {}


# 已知別稱漂移補丁: eval_passrate 的 role_filter 用詞(维护者) 與 persona title(运维者)
# 不一致 → 手工對齊, 避免映射失敗導致建議失效。
_ROLE_DRIFT_PATCH = {
    "维护者": "maintainer",
    "运维": "maintainer",
    "开发者": "engineer",
}


def _build_role_alias_map() -> dict[str, str]:
    """从 persona JSON 构建 中文title/英文name → name 的别名表 (幂等缓存)。"""
    if _ROLE_ALIAS:
        return _ROLE_ALIAS
    for f in PERSONAS_DIR.glob("persona_*.json"):
        try:
            d = json.loads(f.read_text())
        except Exception:
            continue
        name = d.get("name")
        if not name:
            continue
        _ROLE_ALIAS[name] = name                       # 英文直过
        title = d.get("title")
        if title:
            _ROLE_ALIAS[title.strip()] = name           # 中文 title → name
    _ROLE_ALIAS.update(_ROLE_DRIFT_PATCH)              # 已知漂移補丁
    return _ROLE_ALIAS


def _resolve_role_for_suggestion(entry: dict) -> str:
    """将单条历史记录解析为 persona name (供 suggest_improvements 使用)。

    优先级: role_filter(中文title/英文) → role → target 末段(文件名回退)。
    映射失败回退到原始末段标识，绝不返回字面 'unknown' 导致建议失效。
    """
    alias = _build_role_alias_map()
    rf = entry.get("role_filter") or entry.get("role")
    if rf:
        if rf in alias:
            return alias[rf]
        # 部分匹配: role_filter 含 title 子串 (e.g. '管理者 / Manager')
        for k, v in alias.items():
            if k and k in rf:
                return v
    # 文件名回退: review_gate 的 target 屬於文件, 非角色。檢測 drift 並告警。
    target = str(entry.get("target", "") or "")
    leaf = target.rstrip("/").split("/")[-1]
    if leaf and leaf not in ("src", "engine.py", "manager.py", "daemon.py"):
        _detect_drift(rf, leaf=leaf)  # 文件名回退 → 告警缺 file→persona mapping
        return leaf
    return ""  # 無法解析 → 空 (調用方跳過, 不產生 unknown 垃圾建議)


def _detect_drift(role_filter: str | None, *, leaf: str = "") -> None:
    """檢測數據源詞彙漂移，寫 bus notice (not fail)。

    WHY: eval_passrate 的 role_filter(维护者) 與 persona title(运维者) 可能漂移；
    review_gate 的 target 是文件名而非角色名 → 兩類皆是跨系統契約缺口。
    此函數**告警不失敗**: 新漂移詞不應阻塞 contract_updater, 但必須可觀測。
    """
    alias = _build_role_alias_map()
    if role_filter and role_filter not in alias and not any(k in role_filter for k in alias):
        _write_bus("notice",
                   f"[contract_updater] role_filter 漂移: {role_filter!r} 無對應 persona title",
                   f"role_filter={role_filter}", "--src", "contract_updater")
    if leaf:
        # 文件名回退代表 file→persona mapping 缺失 → 告警 (ponytail: 文件屬義邊界未實現)
        _write_bus("notice",
                   f"[contract_updater] target={leaf!r} 是文件名, 無法映射到角色 persona —— 需建立 file→role 映射表",
                   f"file_target={leaf}", "--src", "contract_updater")


def _assert_role_coverage(feed: list[dict]) -> int:
    """覆蓋斷言: 驗證每個非 None role_filter 能解析到 persona。

    返回**未解析數** (0=全部覆蓋)。將 silent skip 轉為可觀測:
    未解析項會寫 bus notice, 但不丟異常(避免阻塞自改進閉環)。
    """
    unmapped = 0
    for e in feed:
        rf = e.get("role_filter") or e.get("role")
        if rf is None:
            continue  # None 是合法 (無角色標記的統計行)
        resolved = _resolve_role_for_suggestion(e)
        if not resolved or resolved == rf:
            unmapped += 1
            _write_bus("notice",
                       f"[contract_updater] role_filter 未解析: {rf!r}",
                       f"role_filter={rf}", "--src", "contract_updater")
    if unmapped:
        _write_bus("code_fix",
                   f"[contract_updater] 發現 {unmapped} 個 role_filter 未解析 —— 覆蓋率<100%",
                   f"unmapped={unmapped}/100%", "--src", "contract_updater")
    return unmapped


def _extract_role(entry: dict) -> str:
    """从单条历史记录提取角色名。

    review_gate 真实 schema 无 role 字段，只有 target(文件路径)/parties(dims 内)。
    旧实现读 entry.get('role') 永远得 'unknown'，导致建议失效。
    修复: 优先用 target 路径末段（persona 名常等于路径末段），
    其次从 dims[].parties 取首个非空角色，最后回退 target 末段。
    """
    target = str(entry.get("target", "") or "")
    # target 末段可能是角色名 (coordinator / session-pipeline/src/pipeflow/engine.py)
    leaf = target.rstrip("/").split("/")[-1]
    if leaf and leaf != "src" and leaf not in ("engine.py", "manager.py", "daemon.py"):
        return leaf
    # 从 dims 的 parties 提取
    try:
        dims = entry.get("dims", "[]")
        if isinstance(dims, str):
            dims = json.loads(dims)
        for d in dims:
            for p in (d.get("parties") or []):
                if p and p != "unknown":
                    return p
    except Exception:
        pass
    return leaf or "unknown"


def suggest_improvements(check_history: list[dict]) -> list[dict]:
    """分析 review_gate 历史记录（真实 schema: target/verdict/dims），生成改进建议。

    规则:
    - 某角色 output_schema 不存在 → 建议加 output_schema
    - 某角色最近 dims 含 FAIL/WARN 维度 → 建议调整（阈值/服务/验证）
    - verdict=BLOCK 且 retry_reasons 非空 → 建议复盘根因
    """
    suggestions: list[dict] = []
    # 按角色分组 (统一解析为 persona name, 支持 review_gate/eval_passrate 多源)
    by_role: dict[str, list[dict]] = {}
    for entry in check_history:
        role = _resolve_role_for_suggestion(entry)
        if not role:
            continue  # 無法解析 (role_filter=None 且無 target) → 跳過, 不產 unknown 垃圾
        by_role.setdefault(role, []).append(entry)

    for role, entries in by_role.items():
        # 规则 1: 最近 dims 含 FAIL 维度 → 建议调整验证/服务
        recent = entries[-5:]
        fail_dims = []
        for e in recent:
            dims = e.get("dims", "[]")
            if isinstance(dims, str):
                try:
                    dims = json.loads(dims)
                except Exception:
                    dims = []
            for d in dims:
                if d.get("status") in ("FAIL", "WARN"):
                    fail_dims.append(d.get("dim_id"))
        if fail_dims:
            suggestions.append({
                "role": role,
                "criterion": "eval_criteria",
                "suggestion": f"角色 {role} 最近审查维度 {fail_dims} 判 FAIL/WARN，建议调整验证阈值或检查对应服务状态",
                "type": "adjust_threshold_or_service",
            })

        # 规则 2: verdict=BLOCK 且 retry_reasons 非空 → 复盘根因
        if any(e.get("verdict") == "BLOCK" and e.get("retry_reasons") for e in recent):
            suggestions.append({
                "role": role,
                "criterion": "root_cause",
                "suggestion": f"角色 {role} 审查 verdict=BLOCK 且有 retry_reasons，建议复盘根因后再放行",
                "type": "review_root_cause",
            })

        # 规则 3: output_schema 不存在（始终检查，不依赖历史格式）
        if not _has_output_schema(role):
            suggestions.append({
                "role": role,
                "criterion": "output_schema",
                "suggestion": f"角色 {role} 未定义 output_schema，建议添加以规范输出格式",
                "type": "add_output_schema",
            })

    # 覆蓋斷言: 每個非 None role_filter 應可解析到 persona, 否則寫 bus notice
    # (將 silent skip 轉為可觀測信號, 不阻塞建議生成)
    try:
        _assert_role_coverage(check_history)
    except Exception as _ce:
        LOGGER.warning("role coverage assert 異常(不阻塞): %s", _ce)

    return suggestions


def _write_bus(category: str, text: str, evidence: str = "", *extra: str) -> bool:
    """寫一条 bus 訊息 (notice/code_fix/architecture) — 用於 drift 检测与覆盖断言告警。

    獨立 guard: bus I/O 故障不得阻塞自改進閉環或消息投递。
    """
    try:
        subprocess.run(
            [sys.executable, str(BUS_SCRIPT), "write", category, text,
             "--evidence", evidence, "--src", "contract_updater", *extra],
            capture_output=True, timeout=10)
        return True
    except Exception as e:
        LOGGER.warning("bus write fail (notice): %s", e)
        return False


def write_suggestions(suggestions: list[dict]) -> int:
    """将建议写入 bus cat=architecture，带 tag contract_improvement。

    返回写入数量。
    """
    count = 0
    for s in suggestions:
        text = f"[contract_updater] {s['type']}: {s['suggestion'][:80]}"
        evidence = "tag=contract_improvement | " + json.dumps(s, ensure_ascii=False)
        try:
            subprocess.run(
                [sys.executable, str(BUS_SCRIPT), "write", "architecture", text,
                 "--evidence", evidence, "--src", "contract_updater",
                 "--tags", "contract_improvement"],
                capture_output=True, timeout=15,
            )
            count += 1
        except Exception as e:
            LOGGER.warning("bus write fail: %s", e)
    return count


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    import argparse
    parser = argparse.ArgumentParser(description="从 eval_checker 历史记录分析并建议改进")
    parser.add_argument("--from-file", type=str, default=None,
                        help="从 JSON 文件读取检查历史，默认从 stdin 读取")
    parser.add_argument("--no-write", action="store_true",
                        help="仅输出建议到 stdout，不写入 bus")
    parser.add_argument("--apply", action="store_true",
                        help="从 stdin 读取 JSON 格式的 suggestion 列表并执行应用")
    args = parser.parse_args()

    if args.apply:
        suggestions = json.load(sys.stdin)
        if not isinstance(suggestions, list):
            LOGGER.error("--apply 输入应为 suggestion 列表 (list[dict])")
            return 1
        result = apply_all_suggestions(suggestions)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if args.from_file:
        with open(args.from_file) as f:
            history = json.load(f)
    else:
        history = json.load(sys.stdin)

    if not isinstance(history, list):
        LOGGER.error("输入应为 check_history 列表 (list[dict])")
        return 1

    suggestions = suggest_improvements(history)
    print(json.dumps(suggestions, ensure_ascii=False, indent=2))

    if suggestions and not args.no_write:
        n = write_suggestions(suggestions)
        LOGGER.info("已写入 %d 条建议到 bus", n)
    elif not suggestions:
        LOGGER.info("无改进建议")

    return 0


if __name__ == "__main__":
    sys.exit(main())
