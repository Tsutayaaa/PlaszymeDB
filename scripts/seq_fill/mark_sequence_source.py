import pandas as pd

# === 用户参数 ===
OLD_CSV = "/Users/shulei/PycharmProjects/Dataset/seq_fill/v0.1.3_pdb/PlaszymeDB_v0.1.3_(FilledByPDB)_marked_pdb.csv"
NEW_CSV = "/Users/shulei/PycharmProjects/Dataset/seq_fill/v0.1.4_refseq/PlaszymeDB_v0.1.4.1_(ManualReviewed).csv"
SOURCE_TAG = "refseq"  # 当前轮标记
OUTPUT_CSV = NEW_CSV.replace(".csv", f"_marked_{SOURCE_TAG}.csv")

# === 加载数据 ===
df_old = pd.read_csv(OLD_CSV)
df_new = pd.read_csv(NEW_CSV)

# === 校验长度一致 ===
if len(df_old) != len(df_new):
    raise ValueError("❌ 表格行数不一致，无法逐行对比")

# === 检查或创建 sequence_source 列 ===
if 'sequence_source' not in df_new.columns:
    df_new['sequence_source'] = ""

# === 执行逐行处理 ===
for idx in range(len(df_new)):
    old_seq = df_old.at[idx, "sequence"]
    new_seq = df_new.at[idx, "sequence"]

    # 原始标记（继承旧表的标签）
    old_tag = df_old.at[idx, "sequence_source"] if "sequence_source" in df_old.columns else ""
    old_tag = "" if pd.isna(old_tag) else str(old_tag).strip()
    current_tag = old_tag

    # 若旧为空、新不为空，说明本轮填入了新序列
    if (pd.isna(old_seq) or str(old_seq).strip() == "") and not pd.isna(new_seq) and str(new_seq).strip() != "":
        tag_list = [tag.strip() for tag in current_tag.split("/") if tag.strip()]
        if SOURCE_TAG not in tag_list:
            tag_list.append(SOURCE_TAG)
        current_tag = "/".join(tag_list)

    df_new.at[idx, "sequence_source"] = current_tag

# === 保存新表 ===
df_new.to_csv(OUTPUT_CSV, index=False)
print(f"✅ 序列来源标记完成（继承+追加），保存为: {OUTPUT_CSV}")