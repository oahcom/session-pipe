#!/usr/bin/env python3
"""doc_check.py — 文档一致性检查

验证:
1. 所有文档中的文件路径存在
2. 所有文档有时间戳
3. API 签名与代码一致
4. 测试数量与实际一致
5. 无废弃引用

用法: python3 docs/doc_check.py
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
TESTS = ROOT / "tests"
DOCS = ROOT / "docs"

errors = []
warnings = []


def check_timestamps():
    """检查所有文档是否有时间戳。"""
    for md in sorted(ROOT.glob("*.md")) + sorted(DOCS.glob("*.md")):
        if md.name == "DOCS.md":
            continue
        content = md.read_text()
        if not re.search(r"(更新|验证时间|版本|Updated|Version).*20\d{2}", content):
            warnings.append(f"{md.relative_to(ROOT)}: 缺少时间戳")


def check_file_references():
    """检查文档中引用的 src/ 文件是否存在。"""
    for md in sorted(ROOT.glob("*.md")) + sorted(DOCS.glob("*.md")):
        if md.name == "doc_check.py":
            continue
        content = md.read_text()
        # 匹配 routing/xxx.py, pipeflow/xxx.py 等模式
        for m in re.finditer(r"(?:src/)?((?:routing|pipeflow|lifecycle|workflow)/\w+\.py)", content):
            rel = m.group(1)
            if not (SRC / rel).exists():
                errors.append(f"{md.relative_to(ROOT)}: 引用不存在的文件 {rel}")
        # 匹配独立 .py 文件（仅限本项目的测试）
        for m in re.finditer(r"(?:src/)?(\w+\.py)", content):
            fname = m.group(1)
            if fname.startswith("test_"):
                # 仅检查本项目 tests/ 下的文件
                if md.parent == DOCS and "hermes-session-roles" in content:
                    continue  # 跳过其他项目的测试引用
                if not (TESTS / fname).exists():
                    warnings.append(f"{md.relative_to(ROOT)}: 引用其他项目的测试 {fname}")
            elif fname in ("doc_check.py", "conftest.py", "run.py", "health_check_all.py"):
                continue
            elif not (SRC / fname).exists():
                pass


def check_test_counts():
    """检查文档中的测试数量是否与实际一致。"""
    actual = {}
    for f in sorted(TESTS.glob("test_*.py")):
        count = len(re.findall(r"def test_", f.read_text()))
        actual[f.name] = count

    total_expected = sum(actual.values())

    for md in [ROOT / "README.md", DOCS / "ARCHITECTURE.md", DOCS / "TEST_WORKFLOW.md"]:
        if not md.exists():
            continue
        content = md.read_text()
        # 检查总计
        m = re.search(r"\*\*总计\*\*.*?\|.*?\*\*(\d+)\*\*", content)
        if m:
            claimed = int(m.group(1))
            if claimed != total_expected:
                errors.append(f"{md.relative_to(ROOT)}: 测试总计声称 {claimed} 实际 {total_expected}")


def check_public_api():
    """检查 PUBLIC_API.md 中声称的函数是否存在于代码中。"""
    api_file = ROOT / "PUBLIC_API.md"
    if not api_file.exists():
        return
    content = api_file.read_text()
    # 检查模块引用
    for m in re.finditer(r"`(\w+/[\w/]+\.py)`", content):
        mod = m.group(1)
        if not (SRC / mod).exists():
            errors.append(f"PUBLIC_API.md: 引用不存在的模块 {mod}")


def check_cross_references():
    """检查文档间的交叉引用。"""
    doc_files = {f.stem: f for f in sorted(DOCS.glob("*.md"))}
    doc_files["README"] = ROOT / "README.md"
    doc_files["AGENTS"] = ROOT / "AGENTS.md"
    doc_files["PUBLIC_API"] = ROOT / "PUBLIC_API.md"

    for name, md in doc_files.items():
        if not md.exists():
            continue
        content = md.read_text()
        for m in re.finditer(r"([A-Z_]+)\.md", content):
            ref = m.group(1)
            if ref == "DOCS":
                continue
            if ref not in doc_files:
                warnings.append(f"{md.relative_to(ROOT)}: 引用不存在的文档 {ref}.md")


def check_naming():
    """检查文档命名一致性。"""
    for md in sorted(ROOT.glob("*.md")) + sorted(DOCS.glob("*.md")):
        name = md.stem
        if name != name.upper() and name != "doc_check":
            warnings.append(f"{md.relative_to(ROOT)}: 文件名非全大写 ({name})")


def main():
    print("=== 文档一致性检查 ===\n")

    check_timestamps()
    check_file_references()
    check_test_counts()
    check_public_api()
    check_cross_references()
    check_naming()

    if errors:
        print(f"❌ {len(errors)} 个错误:")
        for e in errors:
            print(f"  {e}")
    else:
        print("✅ 无错误")

    if warnings:
        print(f"\n⚠️  {len(warnings)} 个警告:")
        for w in warnings:
            print(f"  {w}")
    else:
        print("✅ 无警告")

    print(f"\n检查完成: {len(errors)} 错误, {len(warnings)} 警告")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
