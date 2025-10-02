import pandas as pd
from collections import defaultdict, Counter

# === 用户参数 ===
INPUT_CSV = "/Users/shulei/PycharmProjects/Dataset/dataset/PlaszymeDB_v0.3.1.csv"
OUTPUT_CSV = INPUT_CSV.replace(".csv", "._deduplicated.csv")
LOG_TXT = INPUT_CSV.replace(".csv", "._deduplication_log.txt")

# === 主键列定义 ===
key_cols = ["plastic", "label", "sequence"]

# === 加载数据并标注原始行号 ===
df = pd.read_csv(INPUT_CSV)
df["original_index"] = df.index.astype(str)

# === 统一大小写 ===
if "sequence" in df.columns:
    df["sequence"] = df["sequence"].astype(str).str.upper().str.strip()

# === 移除 sequence 为空的行（其余两列可为空）===
df = df[~df["sequence"].isnull()].copy()

# === 合并列为除主键列、PLZ_ID、Other_PLZ_ID外的其他列 ===
merge_cols = [
    col for col in df.columns
    if col not in key_cols + ["PLZ_ID", "Other_PLZ_ID"]
]

# === 原始列顺序（不改变除添加 Other_PLZ_ID 外的任何顺序）
column_order = list(df.columns)
if "Other_PLZ_ID" not in column_order:
    column_order.append("Other_PLZ_ID")

# === 初始化统计容器 ===
group_to_indices = defaultdict(list)
group_to_content = {}
source_combinations = []

# === 合并函数，主键通过外部绑定传入 ===
def merge_group(group, group_key):
    merged = {}

    # 主键
    for i, col in enumerate(key_cols):
        merged[col] = group_key[i]

    group_to_indices[group_key] = group["original_index"].tolist()

    for col in merge_cols:
        values = group[col].dropna().astype(str).unique()
        clean_values = [v.strip() for v in values if v.strip() and v.lower() != "nan"]
        merged[col] = "/".join(sorted(set(clean_values)))

    # === PLZ_ID 专项处理 ===
    if "PLZ_ID" in group.columns:
        plz_subframe = group[["PLZ_ID"] + merge_cols].copy()
        plz_subframe["non_empty_fields"] = plz_subframe.apply(
            lambda row: sum(bool(str(row[col]).strip()) for col in merge_cols), axis=1
        )
        plz_sorted = plz_subframe.sort_values("non_empty_fields", ascending=False)

        best_plz_raw = plz_sorted.iloc[0]["PLZ_ID"]
        best_plz = str(best_plz_raw).strip().split()[-1]

        all_plz = sorted(set(
            str(v).strip().split()[-1]
            for v in group["PLZ_ID"].dropna()
            if str(v).strip()
        ))
        other_plz = [v for v in all_plz if v != best_plz]

        merged["PLZ_ID"] = best_plz
        merged["Other_PLZ_ID"] = "/".join(other_plz)
    else:
        merged["Other_PLZ_ID"] = ""

    # 来源组合（可选）
    if "source_name" in group.columns:
        combined_sources = group["source_name"].dropna().astype(str).tolist()
        combo = "/".join(sorted(set(
            s.strip()
            for entry in combined_sources
            for s in entry.split("/")
            if s.strip()
        )))
        if combo:
            source_combinations.append(combo)

    group_to_content[group_key] = merged
    return merged

# === 手动 groupby 循环 ===
rows = []
for group_key, group in df.groupby(key_cols, dropna=False):
    merged_row = merge_group(group, group_key)
    rows.append(merged_row)

df_dedup = pd.DataFrame(rows)
df_dedup = df_dedup[column_order]

# === 查找仍有重复主键（理论上不应有） ===
dedup_keys = df_dedup[key_cols].apply(lambda row: tuple(row), axis=1)
duplicate_counts = dedup_keys.value_counts()
still_duplicates = duplicate_counts[duplicate_counts > 1]

# === 来源组合统计 ===
source_combo_counter = Counter(source_combinations)

# === 写入日志文件 ===
with open(LOG_TXT, "w") as f:
    f.write("=== 📊 去重统计信息 ===\n")
    f.write(f"- 去重主键字段: {', '.join(key_cols)}\n")
    f.write(f"- 原始总行数（剔除 sequence 为空的行后）: {len(df)}\n")
    f.write(f"- 去重后总行数: {len(df_dedup)}\n")
    f.write(f"- 共合并重复组数: {sum(1 for v in group_to_indices.values() if len(v) > 1)}\n")
    f.write(f"- 共合并记录数: {sum(len(v)-1 for v in group_to_indices.values() if len(v) > 1)}\n\n")

    f.write("=== 🔁 重复来源组合统计 (source_name) ===\n")
    for combo, count in source_combo_counter.most_common():
        f.write(f"{combo} : {count} 组\n")
    f.write("\n")

    f.write("=== 🔎 合并详情 ===\n\n")
    for key, indices in group_to_indices.items():
        if len(indices) <= 1:
            continue
        f.write(f"[Merged Group] Key = {key}\n")
        f.write(f" - Merged from lines: {', '.join(indices)}\n")
        merged_row = group_to_content[key]
        for col in df_dedup.columns:
            f.write(f"   {col}: {merged_row.get(col, '')}\n")
        f.write("\n")

    if not still_duplicates.empty:
        f.write("\n=== ⚠️ 合并后仍存在重复主键的条目 ===\n")
        for key, count in still_duplicates.items():
            f.write(f"Duplicate key: {key} → Appeared {count} times\n")

# === 保存最终结果 ===
df_dedup.to_csv(OUTPUT_CSV, index=False)
print(f"✅ 去重完成，共合并 {sum(len(v)-1 for v in group_to_indices.values() if len(v) > 1)} 条记录")
print(f"📁 新文件已保存为: {OUTPUT_CSV}")
print(f"📝 日志文件已保存为: {LOG_TXT}")