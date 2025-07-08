import pandas as pd
import re

# === 配置路径 ===
INPUT_CSV = 'structure_metadata.csv'
OUTPUT_CSV = 'structure_metadata_deduplicated.csv'

# === 解析 resolution 数值 ===
def parse_resolution(res_str):
    if pd.isna(res_str) or not isinstance(res_str, str):
        return float('inf')  # 无值视为最差
    numbers = re.findall(r"[\d.]+", res_str)
    if not numbers:
        return float('inf')
    return float(numbers[0])  # 取第一个数作为主要分辨率

# === 加载与处理 ===
df = pd.read_csv(INPUT_CSV)
df['resolution_value'] = df['resolution'].apply(parse_resolution)

# === 按 PLZ_ID 去重，保留 resolution 最小的行 ===
dedup_df = df.sort_values(by='resolution_value').drop_duplicates(subset='PLZ_ID', keep='first')

# === 清理并保存 ===
dedup_df.drop(columns=['resolution_value'], inplace=True)
dedup_df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ 已完成去重，保存至: {OUTPUT_CSV}")