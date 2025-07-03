import pandas as pd
import os
import glob

# === 字段合并映射配置 ===
COLUMN_MERGE_RULES = {
    "GenBank/UniProt/MGnify": [
        "GenBank/ UniProt",
        "GenBank/ UniProt/MGnify",
        "GenBank ID / UniProt",
        "UniProt"
    ],
    "host_enzyme_gene": [
        "Microbial host/enzyme/gene",
        "Host/enzyme/gene"
    ]
}

def extract_plastic_type(filename):
    return os.path.splitext(os.path.basename(filename))[0].replace("PAZy_", "")

def clean_text(val):
    if isinstance(val, str):
        return val.replace('\xa0', ' ').replace('\u200b', '').strip()
    return val

def process_pazy_file(path):
    plastic = extract_plastic_type(path)
    df_raw = pd.read_excel(path, header=None)

    header = df_raw.iloc[0].map(str)
    df = df_raw[1:].copy()
    df.columns = header
    df = df.dropna(how="all").copy()
    df = df.map(clean_text)

    data_rows = []
    taxonomy_list = []
    current_tax = None

    for _, row in df.iterrows():
        if pd.notna(row.iloc[0]) and row.iloc[1:].dropna().empty:
            current_tax = row.iloc[0]
        else:
            data_rows.append(row)
            taxonomy_list.append(current_tax)

    df_cleaned = pd.DataFrame(data_rows)
    df_cleaned["taxonomy"] = taxonomy_list
    df_cleaned["Plastic"] = plastic

    # === 合并列逻辑 ===
    for unified_col, source_cols in COLUMN_MERGE_RULES.items():
        df_cleaned[unified_col] = None
        for col in source_cols:
            if col in df_cleaned.columns:
                if df_cleaned[unified_col].isnull().all():
                    df_cleaned[unified_col] = df_cleaned[col]
                else:
                    conflict_mask = df_cleaned[unified_col].notnull() & df_cleaned[col].notnull()
                    if conflict_mask.any():
                        raise ValueError(
                            f"❌ 字段冲突: '{unified_col}' 有多个来源列在同一行中非空:\n"
                            f"{df_cleaned[conflict_mask][[unified_col, col]].head()}"
                        )
                    df_cleaned[unified_col] = df_cleaned[unified_col].combine_first(df_cleaned[col])

        for col in source_cols:
            if col in df_cleaned.columns:
                df_cleaned.drop(columns=col, inplace=True)

    # === 拆分 host_enzyme_gene 字段为 host/enzyme/gene ===
    if "host_enzyme_gene" in df_cleaned.columns:
        split_cols = df_cleaned["host_enzyme_gene"].str.split(",", n=2, expand=True)
        df_cleaned["host"] = split_cols[0].str.strip()
        df_cleaned["enzyme"] = split_cols[1].str.strip() if 1 in split_cols.columns else None
        df_cleaned["gene"] = split_cols[2].str.strip() if 2 in split_cols.columns else None
        df_cleaned.drop(columns=["host_enzyme_gene"], inplace=True)

    # === Plastic 放首列 ===
    cols = ["Plastic"] + [col for col in df_cleaned.columns if col != "Plastic"]
    df_cleaned = df_cleaned[cols]

    return df_cleaned

def batch_process_pazy_folder(folder_path, output_csv="merged_pazy.csv", output_dir="PAZy"):
    all_files = glob.glob(os.path.join(folder_path, "PAZy_*.xlsx"))
    merged_list = []

    for file_path in all_files:
        try:
            df = process_pazy_file(file_path)
            merged_list.append(df)
            print(f"✅ Processed: {os.path.basename(file_path)} ({len(df)} rows)")
        except Exception as e:
            print(f"❌ Failed: {file_path} - {str(e)}")

    if not merged_list:
        print("⚠️ 没有有效数据文件被处理。")
        return

    merged_df = pd.concat(merged_list, ignore_index=True)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_csv)
    merged_df.to_csv(output_path, index=False, encoding="utf-8")

    print(f"\n📦 合并完成：共 {len(merged_df)} 条记录，已保存至 {output_path}")

# === 示例调用 ===
if __name__ == "__main__":
    batch_process_pazy_folder(
        folder_path="/PAZy_data",
        output_csv="pazy1.csv",
        output_dir="/PAZy"
    )