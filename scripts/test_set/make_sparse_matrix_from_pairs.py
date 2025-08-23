#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_sparse_matrix_from_pairs.py
—— 将“1-1 对应”的长表（sequence × polymer × label）转成
   【样本×塑料种类】的稀疏矩阵，并补出 dense 预览（缺失用 None）。

【功能】
1) 读取只有三列的配对长表（序列列、塑料列、标签列），其中标签：正=1，负=0。
2) 行=样本（按序列分组），列=塑料种类；已观测的配对位置填 0/1，未观测位置视为“未知”（缺失）。
3) 保存：
   - 稀疏标签矩阵 (CSR)：<prefix>_labels_csr.npz   （仅 0/1 的观测点）
   - 稀疏观测掩码 (CSR)：<prefix>_observed_mask_csr.npz  （观测点=1；未知=0）
   - 行/列 ID 映射：<prefix>_row_ids.txt, <prefix>_col_ids.txt
   - 信息摘要：<prefix>_matrix_info.json
   - 可选 dense 预览 CSV（把未知补为 None）：<prefix>_preview.csv（仅在规模较小时）

【注意】
- 该脚本假定输入“1-1 对应”（同一 sequence × polymer 只有一行）。
  若检测到重复同一配对，将直接报错并给出样例，避免含糊聚合。
- 稀疏矩阵中“未知”并不占位；请结合 observed_mask 来区分“负样本(0)”与“未知(None)”。
- 若你确实希望把重复配对做聚合（比如多数表决/任一为正），请告知，我再给聚合版本。

【使用方式（不走命令行）】
直接在 CONFIG 中修改路径与列名，然后运行本脚本即可。
"""

import os
import json
from typing import Tuple, List

import numpy as np
import pandas as pd
from scipy import sparse

# ========= 可在此区块修改参数 =========
CONFIG = {
    # 输入数据
    "input_path": "/Users/shulei/PycharmProjects/Dataset/dataset/PlaszymeDB_v0.3.1._deduplicated.csv",  # CSV/TSV 均可
    "sep": ",",                             # 分隔符：CSV 用 ","；TSV 用 "\t"

    # 列名设置（区分大小写）
    "seq_col": "sequence",                  # 序列列名（样本键）
    "polymer_col": "plastic",               # 塑料种类列名（类别/列键）
    "label_col": "label",                   # 标签列名（正=1，负=0）

    # 输出
    "output_prefix": "./matrix_out/sparse_matrix_v0.3.1",

    # 预览导出：矩阵规模不大时，导出 dense 预览 CSV（未知= None）
    "enable_preview": True,
    "preview_max_rows": 1000,                # 行、列均不超过该阈值才导出预览
    "preview_max_cols": 50,
}
# ====================================


def _coerce_label_to_01(x) -> int:
    """
    将输入标签严格映射到 {0,1}。
    允许的输入形式：0/1, "0"/"1", True/False, "true"/"false", "pos"/"neg"。
    其它情况报错，避免默默出错。
    """
    if pd.isna(x):
        raise ValueError("label 列存在空值（NaN），1-1 表不应有空标签。")
    # 先尝试数值
    try:
        v = float(x)
        if v == 0.0:
            return 0
        if v == 1.0:
            return 1
    except Exception:
        pass
    # 尝试字符串
    s = str(x).strip().lower()
    if s in {"0", "neg", "negative", "false", "no", "n"}:
        return 0
    if s in {"1", "pos", "positive", "true", "yes", "y"}:
        return 1
    raise ValueError(f"无法将标签值 {x!r} 解释为 0/1，请清洗后再试。")


def _validate_no_duplicates(df: pd.DataFrame, seq_col: str, polymer_col: str) -> None:
    """
    确认 (sequence, polymer) 唯一。
    若存在重复，将报错并列出前若干个样例。
    """
    dup_mask = df.duplicated(subset=[seq_col, polymer_col], keep=False)
    if dup_mask.any():
        dup = df.loc[dup_mask, [seq_col, polymer_col]].value_counts().head(10)
        examples = dup.reset_index().values.tolist()
        msg = (
            "检测到重复的 (sequence, polymer) 配对，当前脚本假定 1-1 表，不做聚合。\n"
            "请先去重/聚合，或让我给你一版带聚合策略的脚本。\n"
            f"重复示例（最多列出 10 条）：\n{examples}"
        )
        raise RuntimeError(msg)


def build_sparse_from_pairs(df: pd.DataFrame,
                            seq_col: str,
                            polymer_col: str,
                            label_col: str
                            ) -> Tuple[sparse.csr_matrix, sparse.csr_matrix, List[str], List[str], dict]:
    """
    从 1-1 配对长表构建：
      - labels_csr：CSR 稀疏矩阵（仅观测到的 0/1 标签）
      - observed_mask_csr：CSR 稀疏观测掩码（观测点=1，未知=0）
      - row_ids：行顺序对应的 sequence 键
      - col_ids：列顺序对应的 polymer 键
      - info：统计信息
    """
    # 规范列存在
    for c in [seq_col, polymer_col, label_col]:
        if c not in df.columns:
            raise KeyError(f"缺少列：{c}")

    # 检查 1-1 唯一性
    _validate_no_duplicates(df, seq_col, polymer_col)

    # 标签严格映射到 {0,1}
    labels = df[label_col].apply(_coerce_label_to_01).astype(np.int8).values

    # 建立行/列索引编码
    row_keys = df[seq_col].astype(str).values
    col_keys = df[polymer_col].astype(str).values
    row_ids, row_inv = np.unique(row_keys, return_inverse=True)
    col_ids, col_inv = np.unique(col_keys, return_inverse=True)

    # 仅将“观测到的配对”写入稀疏
    data_labels = labels.astype(np.int8)
    data_obs = np.ones_like(data_labels, dtype=np.int8)

    coo_labels = sparse.coo_matrix(
        (data_labels, (row_inv, col_inv)),
        shape=(row_ids.size, col_ids.size),
        dtype=np.int8
    )
    coo_obs = sparse.coo_matrix(
        (data_obs, (row_inv, col_inv)),
        shape=(row_ids.size, col_ids.size),
        dtype=np.int8
    )

    labels_csr = coo_labels.tocsr()
    observed_mask_csr = coo_obs.tocsr()

    info = {
        "rows": int(row_ids.size),
        "cols": int(col_ids.size),
        "observed_pairs": int(observed_mask_csr.nnz),
        "positives": int((labels == 1).sum()),
        "negatives": int((labels == 0).sum()),
        "density_over_observed": 1.0,  # 标签矩阵与观测掩码同稀疏结构
        "matrix_dtype": "int8",
        "note": "未知/未观测位置不存储；请结合 observed_mask 区分 0 与 None。",
    }
    return labels_csr, observed_mask_csr, list(map(str, row_ids)), list(map(str, col_ids)), info


def save_outputs(prefix: str,
                 labels_csr: sparse.csr_matrix,
                 observed_mask_csr: sparse.csr_matrix,
                 row_ids: List[str],
                 col_ids: List[str],
                 info: dict,
                 enable_preview: bool,
                 preview_max_rows: int,
                 preview_max_cols: int) -> None:
    """
    保存稀疏矩阵、行列映射、摘要，以及可选的 dense 预览（未知= None）。
    """
    os.makedirs(os.path.dirname(prefix) or ".", exist_ok=True)

    # 稀疏矩阵
    sparse.save_npz(f"{prefix}_labels_csr.npz", labels_csr)
    sparse.save_npz(f"{prefix}_observed_mask_csr.npz", observed_mask_csr)

    # 行列 ID
    with open(f"{prefix}_row_ids.txt", "w", encoding="utf-8") as f:
        for r in row_ids:
            f.write(f"{r}\n")
    with open(f"{prefix}_col_ids.txt", "w", encoding="utf-8") as f:
        for c in col_ids:
            f.write(f"{c}\n")

    # 摘要
    with open(f"{prefix}_matrix_info.json", "w", encoding="utf-8") as f:
        json.dump(info, f, ensure_ascii=False, indent=2)

    # 可选 dense 预览（仅在规模较小时）
    if enable_preview and labels_csr.shape[0] <= preview_max_rows and labels_csr.shape[1] <= preview_max_cols:
        # 先用 None 填满，再把观测到的位置填 0/1
        preview = [[None for _ in range(labels_csr.shape[1])] for __ in range(labels_csr.shape[0])]
        # 用 CSR 索引填值
        labels_csr = labels_csr.tocsr()
        for i in range(labels_csr.shape[0]):
            start, end = labels_csr.indptr[i], labels_csr.indptr[i+1]
            for idx in range(start, end):
                j = int(labels_csr.indices[idx])
                v = int(labels_csr.data[idx])
                preview[i][j] = v
        df_prev = pd.DataFrame(preview, index=row_ids, columns=col_ids)
        df_prev.to_csv(f"{prefix}_preview.csv", encoding="utf-8")


def main():
    cfg = CONFIG
    # 读取
    df = pd.read_csv(cfg["input_path"], sep=cfg["sep"])
    # 构建稀疏矩阵
    labels_csr, observed_mask_csr, row_ids, col_ids, info = build_sparse_from_pairs(
        df=df,
        seq_col=cfg["seq_col"],
        polymer_col=cfg["polymer_col"],
        label_col=cfg["label_col"]
    )
    # 保存
    save_outputs(
        prefix=cfg["output_prefix"],
        labels_csr=labels_csr,
        observed_mask_csr=observed_mask_csr,
        row_ids=row_ids,
        col_ids=col_ids,
        info=info,
        enable_preview=cfg["enable_preview"],
        preview_max_rows=cfg["preview_max_rows"],
        preview_max_cols=cfg["preview_max_cols"]
    )


if __name__ == "__main__":
    main()