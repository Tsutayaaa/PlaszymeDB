"""
structure_loader.py

A lightweight utility for retrieving PDB file paths by PLZ_ID
from either experimental or predicted structure datasets.

结构文件路径检索模块：
- 默认优先使用实验结构（X-ray 等）
- 可指定使用预测结构
- 输出为绝对路径，可直接用于加载、复制或下载操作

使用示例：
    from structure_loader import get_pdb_path
    path = get_pdb_path("41cce49ced")
"""

import pandas as pd
from pathlib import Path
from typing import Optional

# === 默认 CSV 路径配置（可根据项目结构调整）===
BASE_DIR = Path(__file__).resolve().parent
EXPERIMENTAL_CSV = (BASE_DIR / "experimental/exp_metadata.csv").resolve()
PREDICTED_CSV = (BASE_DIR / "predicted/pred_metadata.csv").resolve()

# === 加载结构数据表格 ===
try:
    _experimental_df = pd.read_csv(EXPERIMENTAL_CSV)
except FileNotFoundError:
    _experimental_df = pd.DataFrame()

try:
    _predicted_df = pd.read_csv(PREDICTED_CSV)
except FileNotFoundError:
    _predicted_df = pd.DataFrame()


def _resolve_csv_relative_path(csv_path: Path, relative_path: str) -> Path:
    """
    将 CSV 文件中的相对路径转换为绝对路径。

    Args:
        csv_path (Path): CSV 文件路径。
        relative_path (str): 相对于 CSV 文件的结构路径（如 pdb/2CZQ.pdb）。

    Returns:
        Path: 绝对路径
    """
    if not isinstance(relative_path, (str, Path)):
        return None
    return (csv_path.parent / Path(relative_path)).resolve()


def get_pdb_path(plz_id: str, source: str = "prefer_experimental") -> Optional[str]:
    """
    获取指定 PLZ_ID 对应的 PDB 文件绝对路径。

    Args:
        plz_id (str): 要查询的 PLZ_ID。
        source (str): 来源选择，可为：
            - "experimental"：仅查实验结构
            - "predicted"：仅查预测结构
            - "prefer_experimental"（默认）：优先查实验结构，若无则查预测结构

    Returns:
        Optional[str]: 返回绝对路径字符串，若找不到或路径无效则返回 None。
    """
    row = None
    csv_path = None

    if source == "experimental":
        row = _experimental_df[_experimental_df["PLZ_ID"] == plz_id]
        csv_path = EXPERIMENTAL_CSV
    elif source == "predicted":
        row = _predicted_df[_predicted_df["PLZ_ID"] == plz_id]
        csv_path = PREDICTED_CSV
    elif source == "prefer_experimental":
        row = _experimental_df[_experimental_df["PLZ_ID"] == plz_id]
        csv_path = EXPERIMENTAL_CSV
        if row.empty:
            row = _predicted_df[_predicted_df["PLZ_ID"] == plz_id]
            csv_path = PREDICTED_CSV
    else:
        raise ValueError(f"无效 source 参数: {source}")

    if row.empty:
        return None

    relative_path = row.iloc[0].get("pdb_path", None)
    if pd.isna(relative_path):
        return None

    absolute_path = _resolve_csv_relative_path(csv_path, relative_path)
    return str(absolute_path) if absolute_path is not None and absolute_path.exists() else None