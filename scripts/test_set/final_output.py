# -*- coding: utf-8 -*-
"""
Compare test/train distribution at polymer-enzyme pair level
+ Multi-plastic enzyme proportion per split
+ Export full rows for test/train (rebuilt from original by 'sequence')

Inputs:
 - test_sequences.csv  (must have a 'sequence' column; sequences are unique)
 - PlaszymeDB_v0.3.1._deduplicated.csv (must have 'sequence' + 'plastic'; rows are sequence-plastic pairs)

Outputs:
 - Console summary
 - {OUT_DIR}/distribution_summary_pairs.csv        (plastic-wise pair counts)
 - {OUT_DIR}/multi_plastic_summary.csv            (per-split multi-plastic proportions)
 - {OUT_DIR}/test_full_pairs.csv                  (all original columns for test sequences)
 - {OUT_DIR}/train_full_pairs.csv                 (all original columns for train sequences)
 - {OUT_DIR}/per_sequence_plastic_span.csv        (sequence-level span summary)
"""

import os
import pandas as pd

# === 路径配置 ===
TEST_CSV = "/Users/shulei/PycharmProjects/Dataset/scripts/test_set/run2/pipeline_out/test_sequences.csv"
ORIG_CSV = "/Users/shulei/PycharmProjects/Dataset/dataset/PlaszymeDB_v0.3.1._deduplicated.csv"
OUT_DIR  = "./final_out_1"      # 结果目录
OUT_SUMMARY_CSV = os.path.join(OUT_DIR, "distribution_summary_pairs.csv")

# === 读取数据 ===
test_df = pd.read_csv(TEST_CSV)
orig_df = pd.read_csv(ORIG_CSV)

# 标准化列名
for df in (test_df, orig_df):
    df.columns = [c.strip().lower() for c in df.columns]

if "sequence" not in test_df.columns:
    raise ValueError("test_sequences.csv must contain 'sequence' column.")
if not {"sequence", "plastic"}.issubset(orig_df.columns):
    raise ValueError("Original CSV must contain 'sequence' and 'plastic' columns.")

os.makedirs(OUT_DIR, exist_ok=True)

# === 标记 test/train （按序列匹配，不区分大小写）===
test_set = set(test_df["sequence"].astype(str).str.upper())
orig_df["sequence_upper"] = orig_df["sequence"].astype(str).str.upper()
orig_df["is_test"] = orig_df["sequence_upper"].isin(test_set)

test_pairs = orig_df[orig_df["is_test"]].copy()
train_pairs = orig_df[~orig_df["is_test"]].copy()

# === 按“对（sequence, plastic）”计数 ===
total_pairs = len(orig_df)
test_pairs_n = len(test_pairs)
train_pairs_n = len(train_pairs)

print(f"✅ 总对数: {total_pairs}")
print(f"🧪 测试集对数: {test_pairs_n}")
print(f"📘 训练集对数: {train_pairs_n}")

# === 塑料分布（pair 级别）===
dist_test = test_pairs["plastic"].value_counts().rename("test_count")
dist_train = train_pairs["plastic"].value_counts().rename("train_count")
dist_total = orig_df["plastic"].value_counts().rename("total_count")

dist_df = pd.concat([dist_total, dist_train, dist_test], axis=1).fillna(0).astype(int)
dist_df["test_frac"] = (dist_df["test_count"] / dist_df["total_count"]).round(6)
dist_df["train_frac"] = (dist_df["train_count"] / dist_df["total_count"]).round(6)

print("\n=== 塑料分布 (pair 数量) ===")
print(dist_df)

# 保存分布表
dist_df.to_csv(OUT_SUMMARY_CSV)
print(f"\n📁 已保存分布表到 {OUT_SUMMARY_CSV}")

# === 序列层面的“塑料覆盖跨度”统计（每条序列关联的独立塑料种类数）===
# 在原始表上计算每条序列关联的 unique plastics 数量
seq_span = (
    orig_df.groupby("sequence_upper")["plastic"]
    .nunique(dropna=True)
    .rename("n_plastics")
    .reset_index()
)

# 便利的列：具体塑料列表（可选）
seq_plastics_list = (
    orig_df.groupby("sequence_upper")["plastic"]
    .apply(lambda s: "|".join(sorted(pd.Series(s).dropna().astype(str).unique())))
    .rename("plastics_list")
    .reset_index()
)

seq_span = seq_span.merge(seq_plastics_list, on="sequence_upper", how="left")

# 标注该序列在 test/train
seq_span["is_test"] = seq_span["sequence_upper"].isin(test_set)

# 每个集合的“多塑料（>=2）蛋白”比例
def summarize_multi_ratio(df_seq_span, mask, name):
    sub = df_seq_span[mask]
    total_seq = len(sub)
    multi_seq = int((sub["n_plastics"] >= 2).sum())
    ratio = (multi_seq / total_seq) if total_seq > 0 else 0.0
    return {
        "split": name,
        "n_sequences": total_seq,
        "n_multi_plastic_sequences": multi_seq,
        "multi_plastic_ratio": round(ratio, 6),
    }

summary_rows = []
summary_rows.append(summarize_multi_ratio(seq_span, seq_span["is_test"], "test"))
summary_rows.append(summarize_multi_ratio(seq_span, ~seq_span["is_test"], "train"))
multi_summary_df = pd.DataFrame(summary_rows)

print("\n=== 序列层面的多塑料比例（n_plastics >= 2） ===")
print(multi_summary_df)

# 保存多塑料比例表
multi_summary_path = os.path.join(OUT_DIR, "multi_plastic_summary.csv")
multi_summary_df.to_csv(multi_summary_path, index=False)
print(f"\n📁 已保存多塑料比例表到 {multi_summary_path}")

# 额外：分布直方（n_plastics 的频数）各自集合
def span_hist(df_seq_span, mask, name):
    sub = df_seq_span[mask]
    hist = sub["n_plastics"].value_counts().sort_index().rename(name)
    return hist

hist_test  = span_hist(seq_span, seq_span["is_test"], "test")
hist_train = span_hist(seq_span, ~seq_span["is_test"], "train")
span_hist_df = pd.concat([hist_test, hist_train], axis=1).fillna(0).astype(int)
span_hist_path = os.path.join(OUT_DIR, "per_split_n_plastics_hist.csv")
span_hist_df.to_csv(span_hist_path)
print(f"📁 已保存每个集合的 n_plastics 频数表到 {span_hist_path}")

# === 导出完整内容到两个 CSV（以输入的序列为键重组）===
# 原表保留所有原始信息；只去掉我们添加的辅助列 sequence_upper / is_test 中你不需要的可自选
cols_out = [c for c in orig_df.columns if c != "sequence_upper"]  # 保留 is_test 方便检查
test_full_path = os.path.join(OUT_DIR, "test_full_pairs.csv")
train_full_path = os.path.join(OUT_DIR, "train_full_pairs.csv")

test_pairs[cols_out].to_csv(test_full_path, index=False)
train_pairs[cols_out].to_csv(train_full_path, index=False)

print(f"\n📁 已导出完整表：")
print(f" - Test 全量对：{test_full_path}")
print(f" - Train 全量对：{train_full_path}")

# === 另存一个序列层面的汇总，方便人工审阅 ===
seq_span_out = os.path.join(OUT_DIR, "per_sequence_plastic_span.csv")
seq_span.rename(columns={"sequence_upper": "sequence_upper_key"}, inplace=True)
seq_span.to_csv(seq_span_out, index=False)
print(f"📁 已保存序列层面的塑料跨度汇总到 {seq_span_out}")