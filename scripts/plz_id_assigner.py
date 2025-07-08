import pandas as pd
import hashlib

# === 用户参数 ===
INPUT_CSV = "/Users/shulei/PycharmProjects/Dataset/dataset/PlaszymeDB_v0.2.1.csv"  # 替换为你的原始文件路径
OUTPUT_CSV = INPUT_CSV.replace(".csv", ".2.csv")
ID_COL = "PLZ_ID"  # 唯一ID列名，可自定义
KEY_FIELDS = ["plastic", "label", "sequence"]  # 参与生成哈希ID的字段

# === 读取数据 ===
df = pd.read_csv(INPUT_CSV)

# === 生成哈希ID函数（含空值提醒）===
def generate_id(row):
    missing_fields = [field for field in KEY_FIELDS if pd.isna(row[field]) or str(row[field]).strip() == ""]
    if missing_fields:
        print(f"⚠️ 缺失字段: {missing_fields} → 行号: {row.name}")
    key = "_".join(str(row[field]).strip() if not pd.isna(row[field]) else "" for field in KEY_FIELDS)
    return hashlib.md5(key.encode()).hexdigest()[:10]  # 取前10位哈希值

# === 应用哈希函数并插入新列 ===
df.insert(0, ID_COL, df.apply(generate_id, axis=1))

# === 保存新文件 ===
df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ 哈希ID生成完成，已添加列: {ID_COL}")
print(f"📁 新文件保存至: {OUTPUT_CSV}")