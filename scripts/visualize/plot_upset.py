import os
import pandas as pd
import matplotlib.pyplot as plt
from upsetplot import UpSet, from_memberships
from itertools import combinations
from typing import Optional, List


def load_sequence_plastic_table(
    file_path: str,
    sequence_col: str = "sequence",
    plastic_col: str = "plastic"
) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ 文件不存在: {file_path}")

    ext = os.path.splitext(file_path)[-1].lower()
    if ext == ".csv":
        df = pd.read_csv(file_path)
    elif ext in [".tsv", ".txt"]:
        df = pd.read_csv(file_path, sep="\t")
    else:
        raise ValueError(f"❌ 不支持的文件类型: {ext}，请使用 .csv 或 .tsv")

    for col in [sequence_col, plastic_col]:
        if col not in df.columns:
            raise ValueError(f"❌ 缺少必要列: '{col}'，实际列为: {df.columns.tolist()}")

    return df[[sequence_col, plastic_col]].dropna().drop_duplicates().rename(
        columns={sequence_col: "sequence", plastic_col: "plastic"})


def extract_overlap_memberships(
    df: pd.DataFrame,
    min_len: int = 2,
    max_len: int = 5
) -> List[frozenset]:
    """
    提取所有共降解组合（长度在 min_len 到 max_len 之间）。
    """
    df["plastic"] = df["plastic"].str.upper().str.strip()
    seq_to_plastics = df.groupby("sequence")["plastic"].apply(set)

    memberships = []
    for plastic_set in seq_to_plastics:
        n = len(plastic_set)
        for r in range(min_len, min(n, max_len) + 1):
            for combo in combinations(sorted(plastic_set), r):
                memberships.append(frozenset(combo))

    return memberships


def plot_upset_from_memberships(
    memberships: List[frozenset],
    title: Optional[str] = "Partial Overlap Co-degraded Plastics",
    figsize: tuple = (14, 8),
    min_subset_size: int = 1,
    save_path: Optional[str] = None
):
    if not memberships:
        print("⚠️ 没有共降解组合可绘图。")
        return

    data = from_memberships(memberships)

    fig = plt.figure(figsize=figsize)
    upset = UpSet(
        data,
        show_counts=True,
        sort_by='cardinality',
        subset_size='count',  # ✅ 关键修复
        min_subset_size=min_subset_size
    )
    upset.plot()

    if title:
        plt.suptitle(title)

    if save_path:
        base, ext = os.path.splitext(save_path)
        if ext == "":
            save_path = os.path.join(save_path, "upset_plot.png")
        else:
            if ext.lower() not in [".png", ".pdf"]:
                ext = ".png"
                save_path = base + ext
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, bbox_inches="tight")
        print(f"✅ 图像已保存至: {save_path}")

    plt.tight_layout()
    plt.show()


def main(
    database_input: str,
    output_path: Optional[str] = None,
    sequence_col: str = "sequence",
    plastic_col: str = "plastic",
    min_combo_len: int = 2,
    max_combo_len: int = 5
):
    print(f"📥 载入数据：{database_input}")
    df = load_sequence_plastic_table(database_input, sequence_col, plastic_col)
    memberships = extract_overlap_memberships(df, min_len=min_combo_len, max_len=max_combo_len)
    print(f"📊 共提取组合（len ≥ {min_combo_len}）：{len(memberships)} 次组合")
    plot_upset_from_memberships(memberships, save_path=output_path)


if __name__ == "__main__":
    main(
        database_input="/Users/shulei/PycharmProjects/Dataset/dataset/PlaszymeDB_v0.2.4.csv",
        output_path="figures/",
        sequence_col="sequence",
        plastic_col="plastic",
        min_combo_len=2,
        max_combo_len=2
    )