import pandas as pd

# === 参数配置 ===
INPUT_CSV = "/Users/shulei/PycharmProjects/Dataset/deduplicate/PlaszymeDB_v0.2.csv"      # 替换为你的实际路径
TARGET_COLUMN = "plastic"               # 替换为你要提取的列名
OUTPUT_CSV = "plastic_types.csv"        # 输出的文件名

def extract_and_count(input_csv, column_name, output_csv):
    df = pd.read_csv(input_csv)

    # 清洗空值并统计
    df_cleaned = df[df[column_name].notna()]
    df_cleaned[column_name] = df_cleaned[column_name].astype(str).str.strip()

    count_series = df_cleaned[column_name].value_counts()

    # 转为 DataFrame 并保存
    out_df = count_series.reset_index()
    out_df.columns = [column_name, "Count"]
    out_df.to_csv(output_csv, index=False)

    print(f"✅ 已保存塑料类型计数表，共 {len(out_df)} 种类型，保存至：{output_csv}")

if __name__ == "__main__":
    extract_and_count(INPUT_CSV, TARGET_COLUMN, OUTPUT_CSV)