import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from itertools import combinations
from collections import defaultdict
import numpy as np
import os

def compute_co_degradation_matrix(file_path, output_prefix="co_degradation", sep=",", normalize="jaccard", clustered=True):
    # 读取数据
    df = pd.read_csv(file_path, sep=sep)
    assert all(col in df.columns for col in ["sequence", "plastic", "label"]), \
        "必须包含列: sequence, plastic, label"

    # 只保留正例
    df_pos = df[df["label"] == 1]

    # 构建酶 -> [可降解塑料列表] 映射
    enzyme_to_plastics = defaultdict(set)
    for _, row in df_pos.iterrows():
        enzyme_to_plastics[row["sequence"]].add(row["plastic"])

    all_plastics = sorted(df_pos["plastic"].unique())
    plastic_set = set(all_plastics)

    # 初始化共降解矩阵（计数）
    co_counts = pd.DataFrame(0, index=all_plastics, columns=all_plastics, dtype=int)

    for plastics in enzyme_to_plastics.values():
        plastics = plastics & plastic_set
        for p1, p2 in combinations(plastics, 2):
            co_counts.loc[p1, p2] += 1
            co_counts.loc[p2, p1] += 1
        for p in plastics:
            co_counts.loc[p, p] += 1

    # === Jaccard 相似度归一化 ===
    if normalize == "jaccard":
        matrix = pd.DataFrame(0.0, index=all_plastics, columns=all_plastics)
        for i in all_plastics:
            for j in all_plastics:
                inter = co_counts.loc[i, j]
                union = co_counts.loc[i, i] + co_counts.loc[j, j] - inter
                matrix.loc[i, j] = inter / union if union > 0 else 0
    else:
        matrix = co_counts

    # === 保存矩阵为 CSV ===
    os.makedirs(os.path.dirname(output_prefix), exist_ok=True)
    matrix.to_csv(f"{output_prefix}_matrix.csv")
    print(f"[✔] 共降解矩阵已保存: {output_prefix}_matrix.csv")

    # === 自动过滤无交互塑料 ===
    # 构造掩码，仅保留“至少与其他塑料”有共降解关系的塑料
    non_diag_mask = matrix.copy()
    np.fill_diagonal(non_diag_mask.values, 0)  # 将对角线设为 0
    active_mask = (non_diag_mask.sum(axis=1) > 0)
    filtered_plastics = list(active_mask[active_mask].index)
    dropped_plastics = list(active_mask[~active_mask].index)

    if dropped_plastics:
        print(f"[i] 以下塑料因无共降解关系被排除：{dropped_plastics}")

    matrix_filtered = matrix.loc[filtered_plastics, filtered_plastics]

    if matrix_filtered.shape[0] < 2:
        print("[⚠] 过滤后只剩 1 个或 0 个塑料，跳过绘图。")
        return matrix

    # === 绘图 ===
    if clustered:
        n = matrix_filtered.shape[0]
        figsize = (max(14, n * 0.7), max(12, n * 0.7))  # 动态扩大画布尺寸

        g = sns.clustermap(
            matrix_filtered.astype(float),
            annot=True,
            fmt=".2f" if normalize else "d",
            cmap="Reds",
            figsize=figsize,
            annot_kws={"size": 9},
            cbar_pos=(0.02, 0.8, 0.05, 0.18),
            dendrogram_ratio=(0.15, 0.15)
        )
        g.ax_heatmap.set_xticklabels(g.ax_heatmap.get_xticklabels(), rotation=45, ha="right", fontsize=9)
        g.ax_heatmap.set_yticklabels(g.ax_heatmap.get_yticklabels(), rotation=0, fontsize=9)
        plt.savefig(f"{output_prefix}_clustermap.png", dpi=300)
        print(f"[✔] 聚类热图已保存: {output_prefix}_clustermap.png")

    return matrix

# 示例入口
if __name__ == "__main__":
    input_file = "/Users/shulei/PycharmProjects/Dataset/dataset/PlaszymeDB_v0.2.4.csv"
    compute_co_degradation_matrix(
        file_path=input_file,
        output_prefix="matrix/plastic_co",
        sep=",",
        normalize="jaccard",   # 可设为 None 使用共现频数
        clustered=True         # 设为 False 不聚类
    )