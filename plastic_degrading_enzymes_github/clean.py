import pandas as pd
import re

# === 读取原始数据 ===
df = pd.read_csv("plastic_degrading_enzymes_github.csv")

# === 清理并拆分 plastic_type（支持 "/" 和 ","） ===
df['plastic_type'] = df['plastic_type'].fillna("").astype(str)

# 使用正则表达式统一按 / 和 , 拆分
df_expanded = df.assign(
    plastic_type=df['plastic_type'].apply(lambda x: re.split(r'[,/]', x))
).explode('plastic_type')

# 去除空格并重命名为 Plastic
df_expanded['Plastic'] = df_expanded['plastic_type'].str.strip()

# === 添加标签列：unknown 或 unknown_plastic → 0，其余为 1 ===
df_expanded['label'] = df_expanded['Plastic'].apply(
    lambda x: 0 if re.fullmatch(r'unknown(_plastic)?', x.strip().lower()) else 1
)

# 删除旧列
df_expanded = df_expanded.drop(columns=['plastic_type'])

# === 将 Plastic 列放到第一列位置 ===
cols = df_expanded.columns.tolist()
cols.insert(0, cols.pop(cols.index('Plastic')))
df_expanded = df_expanded[cols]

# === 保存结果 ===
df_expanded.to_csv("plastic_degrading_enzymes_processed.csv", index=False)
print("✅ 拆分、重命名与标签处理完成，输出文件为 plastic_degrading_enzymes_processed.csv")