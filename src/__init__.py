"""src 包 — session-pipeline 核心代码。

自动将 src/ 目录加入 sys.path，使子包间 import 简化。
"""
import sys
from pathlib import Path
_SRC_DIR = str(Path(__file__).resolve().parent)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)
