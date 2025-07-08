import pandas as pd
import re
import os

# === 分类函数，自动保留带版本号 ===
def classify_ids_with_warning(cell, row_idx):
    categories = {
        "UniProt": [],
        "GenBank": [],
        "RefSeq": [],
        "PDB": [],
        "MGnify": [],
        "Other": []
    }

    if pd.isna(cell):
        return categories

    # === 分割 ID ===
    raw_ids = re.split(r"[,\s;/]+", str(cell).strip())
    raw_ids = [id_.strip() for id_ in raw_ids if id_ and id_.lower() != "and" and id_.lower() != "others"]

    base_id_map = {}
    kept_ids = []

    for id_ in raw_ids:
        base_id = re.sub(r"\.\d+$", "", id_)

        if base_id not in base_id_map:
            base_id_map[base_id] = id_
        else:
            prev = base_id_map[base_id]
            if prev == id_:
                continue  # 完全重复，跳过
            elif re.search(r"\.\d+$", id_) and not re.search(r"\.\d+$", prev):
                base_id_map[base_id] = id_  # 保留版本号更完整的 ID
            elif re.search(r"\.\d+$", prev) and not re.search(r"\.\d+$", id_):
                continue  # 已保留更好的 ID，跳过
            else:
                # 两个都带版本号但不同，全部保留并警告
                if prev not in kept_ids:
                    kept_ids.append(prev)
                kept_ids.append(id_)
                print(f"⚠️ Row {row_idx + 2}: Conflicting versions for ID '{base_id}': {prev}, {id_}")

    # 将唯一 ID 整合入 kept_ids
    for final_id in base_id_map.values():
        if final_id not in kept_ids:
            kept_ids.append(final_id)

    # === 分类 ===
    for id_ in kept_ids:
        # === UniProt (标准 + isoform + 带版本号) ===
        if re.match(r"^[A-NR-Z][0-9][A-Z0-9]{3}[0-9](\.\d+)?(_[A-Z0-9]+)?$", id_) or \
                re.match(r"^[A-Z0-9]{6,10}(_[A-Z0-9]+)?$", id_):
            categories["UniProt"].append(id_)

        # === RefSeq (WP, NP, XP, YP 等) ===
        elif re.match(r"^(WP|NP|XP|YP)_[0-9]+\.\d+$", id_):
            categories["RefSeq"].append(id_)

        # === GenBank ===
        elif re.match(r"^[A-Z]{2,4}[0-9]+(\.\d+)?$", id_):
            categories["GenBank"].append(id_)

        # === PDB ===
        elif re.match(r"^[0-9][A-Za-z0-9]{3}(_?[A-Z])?$", id_):
            categories["PDB"].append(id_)

        # === MGnify ===
        elif re.match(r"^MGYP[0-9]+$", id_):
            categories["MGnify"].append(id_)

        else:
            categories["Other"].append(id_)

    return categories

# === 主处理函数 ===
def classify_and_expand_ids(input_csv_path, output_csv_path):
    df = pd.read_csv(input_csv_path)

    # 合并多个列用于 ID 提取
    combined_ids = df[["GenBank/UniProt/MGnify", "NCBI BLAST", "PDB entry"]].fillna("").agg("/".join, axis=1)

    classified_rows = []
    for idx, id_string in enumerate(combined_ids):
        classified_rows.append(classify_ids_with_warning(id_string, row_idx=idx))

    # 生成 DataFrame 并写入原表
    classified_df = pd.DataFrame(classified_rows)
    for key in ["UniProt", "GenBank", "RefSeq", "PDB", "MGnify", "Other"]:
        df[key] = classified_df[key].apply(lambda x: "/".join(x) if x else None)

    # 写入输出
    df.to_csv(output_csv_path, index=False)
    print(f"✅ 分类完成，输出保存至: {output_csv_path}")

# === 模块调用入口 ===
if __name__ == "__main__":
    input_path = "/PAZy/pazy.csv"
    output_path = "/PAZy/pazy_with_id_types.csv"
    classify_and_expand_ids(input_path, output_path)