import pandas as pd
import os
import yaml

# === 加载配置文件 ===
def load_config(config_path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config["standard_fields"], config["file_configs"]

# === 加载并标准化文件 ===
def load_file_with_mapping(config, standard_fields):
    path = config["path"]
    mapping = config["mapping"]
    source = config["source"]
    static_values = config.get("static_values", {})

    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext in [".xls", ".xlsx"]:
        df = pd.read_excel(path)
    else:
        raise ValueError(f"❌ Unsupported file type: {path}")

    df = df.rename(columns=mapping)

    for key, value in static_values.items():
        df[key] = value

    df["source_name"] = source

    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    for col in standard_fields:
        if col not in df.columns:
            df[col] = ""

    return df[standard_fields]

# === 合并所有文件 ===
def merge_all_files(file_configs, standard_fields):
    all_dfs = [load_file_with_mapping(cfg, standard_fields) for cfg in file_configs]
    merged = pd.concat(all_dfs, ignore_index=True)
    return merged

# === 主程序入口 ===
if __name__ == "__main__":
    config_path = "/Users/shulei/PycharmProjects/Dataset/dataset_config_v1.0.yaml"
    output_file = "dataset/PlaszymeDB_v0.1_(Raw, Unmerged).csv"

    standard_fields, file_configs = load_config(config_path)
    merged_df = merge_all_files(file_configs, standard_fields)
    merged_df.to_csv(output_file, index=False)
    print(f"✅ 合并完成，共 {len(merged_df)} 条记录，已保存至 {output_file}")