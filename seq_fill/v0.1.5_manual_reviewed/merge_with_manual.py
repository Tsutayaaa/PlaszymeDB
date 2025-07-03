import pandas as pd

# === 用户参数 ===
OLD_CSV = "/Users/shulei/PycharmProjects/Dataset/seq_fill/v0.1.4_refseq/PlaszymeDB_v0.1.4_(FilledByRefSeq)_marked_refseq.csv"
MANUAL_CSV = "/Users/shulei/PycharmProjects/Dataset/seq_fill/v0.1.5_manual_reviewed/PlaszymeDB_v0.1.5.csv"
OUTPUT_CSV = MANUAL_CSV.replace(".csv", "_final_merged.csv")

# === 加载数据 ===
df_old = pd.read_csv(OLD_CSV)
df_manual = pd.read_csv(MANUAL_CSV)

# === 校验行数一致 ===
if len(df_old) != len(df_manual):
    raise ValueError("❌ 表格行数不一致，可能不能直接一对一对齐。请确认是否手动排序后再比较。")

# === 补全人工表中的 sequence_source（如果缺失） ===
if "sequence_source" not in df_manual.columns:
    df_manual["sequence_source"] = ""

for idx in range(len(df_manual)):
    manual_val = df_manual.at[idx, "sequence_source"]
    old_val = df_old.at[idx, "sequence_source"] if "sequence_source" in df_old.columns else ""
    if (pd.isna(manual_val) or str(manual_val).strip() == "") and pd.notna(old_val):
        df_manual.at[idx, "sequence_source"] = old_val

# === 标记发生变动的行并添加 * ===
for idx in range(len(df_manual)):
    row_old = df_old.iloc[idx]
    row_new = df_manual.iloc[idx]

    # 判断除 sequence_source 外是否有变化（任意一个单元格内容变动）
    changed = any(
        str(row_old[col]).strip() != str(row_new[col]).strip()
        for col in df_manual.columns if col != "sequence_source"
    )

    if changed:
        # 添加 * 标记
        tag = str(df_manual.at[idx, "sequence_source"]).strip()
        if "*" not in tag:
            df_manual.at[idx, "sequence_source"] = tag + "*" if tag else "*"

# === 保存最终合并表 ===
df_manual.to_csv(OUTPUT_CSV, index=False)
print(f"✅ 人工修订合并完成，已保存为: {OUTPUT_CSV}")