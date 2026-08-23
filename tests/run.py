#!/usr/bin/env python3
"""
SET — 统一测试运行器

SET (Software Engineer in Test) 维护此文件。
TE (Test Engineer) 通过 --te 模式使用。

用法:
    python3 tests/run.py              # 运行全部自动化测试
    python3 tests/run.py --list       # 列出所有测试文件
    python3 tests/run.py --quick      # 只跑核心测试
    python3 tests/run.py --te         # TE 模式：运行可执行场景
    python3 tests/run.py --coverage   # 报告覆盖盲区
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR = ROOT / "tests"

TEST_FILES = [
    ("路由逻辑", "test_router.py"),
    ("集成测试", "test_integration.py"),
    ("角色交互", "test_role_interaction.py"),
]

QUICK_FILES = [
    ("路由逻辑", "test_router.py"),
    ("集成测试", "test_integration.py"),
]


def _run_file(name: str, path: Path) -> dict:
    start = time.time()
    result = subprocess.run(
        [sys.executable, str(path)],
        capture_output=True, text=True, timeout=120,
    )
    elapsed = time.time() - start
    return {
        "name": name, "path": str(path),
        "returncode": result.returncode,
        "elapsed": round(elapsed, 2),
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def do_list():
    print("可用测试文件:")
    for name, fname in TEST_FILES:
        p = TESTS_DIR / fname
        exists = "✅" if p.exists() else "❌"
        print(f"  {exists} {name:12s} → {fname}")


def do_all():
    print(f"{'='*60}")
    print(f"  SET 测试运行器 — 全部 {len(TEST_FILES)} 个测试")
    print(f"{'='*60}\n")
    results = []
    for name, fname in TEST_FILES:
        path = TESTS_DIR / fname
        if not path.exists():
            print(f"  ⏭️  {name}: 文件不存在")
            continue
        r = _run_file(name, path)
        results.append(r)
        status = "✅" if r["returncode"] == 0 else "❌"
        print(f"  {status} {name:12s} ({r['elapsed']}s)")
        if r["returncode"] != 0:
            for line in r["stdout"].split("\n")[-5:]:
                print(f"       {line}")
            for line in r["stderr"].split("\n")[-3:]:
                if line.strip():
                    print(f"       err: {line}")
    print(f"\n{'='*60}")
    passed = sum(1 for r in results if r["returncode"] == 0)
    total = len(results)
    print(f"  结果: {passed}/{total} 通过")
    return 0 if passed == total else 1


def do_coverage():
    """报告当前测试覆盖盲区 (SET 维护此分析)。"""
    print("覆盖分析:\n")
    src_files = {
        "router.py": "test_router.py",
        "auto_route.py": "test_integration.py",
        "reliability.py": "test_integration.py",
        "workflow_db.py": None,  # 无覆盖
        "workflow_engine.py": None,
        "workflow_daemon.py": None,
    }
    for src, test_file in src_files.items():
        covered = "✅" if test_file else "❌"
        test_info = f"→ {test_file}" if test_file else "⚠️  无覆盖 (SET 任务)"
        print(f"  {covered} {src:25s} {test_info}")


def main():
    parser = argparse.ArgumentParser(description="SET 统一测试运行器")
    parser.add_argument("--list", action="store_true", help="列出测试文件")
    parser.add_argument("--quick", action="store_true", help="只跑核心测试")
    parser.add_argument("--coverage", action="store_true", help="覆盖分析")
    args = parser.parse_args()

    if args.list:
        return do_list()
    if args.coverage:
        return do_coverage()
    return do_all()


if __name__ == "__main__":
    sys.exit(main())
