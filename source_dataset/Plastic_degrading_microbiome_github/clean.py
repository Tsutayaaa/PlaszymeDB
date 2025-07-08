import pandas as pd
import re

# === 设置文件路径 ===
input_path = "Plastic_degrading_microbiome_github.csv"
output_path = "Plastic_degrading_microbiome_github_clean.csv"

# === 读取数据 ===
df = pd.read_csv(input_path)

# === 分类函数 ===
import re

def classify_id(id_str: str) -> str:
    """
    分类输入的 ID 字符串，返回所属的数据库类型字段名。
    """
    id_str = id_str.strip()

    # === UniProt ID ===
    # 6位老格式或10位新格式，如 P12345、Q9ZNH8、A0A0B4XYZ1
    if re.match(r'^[A-NR-Z][0-9][A-Z0-9]{3}[0-9]$', id_str) or re.match(r'^A0A[0-9A-Z]{7}$', id_str):
        return "uniprot_ids"

    # === GenBank ID ===
    # 常见格式如 AB123456.1、U10470、NZ_CP011371.1（包含版本号）
    if re.match(r'^(?:[A-Z]{1,2}[0-9]{5,6})(\.\d+)?$', id_str) or re.match(r'^NZ_[A-Z]{4}[0-9]{6}\.\d+$', id_str):
        return "genbank_ids"

    # === RefSeq ID ===
    # 格式如 NP_123456、XP_123456.1
    if re.match(r'^[NXWZY]P_\d+(\.\d+)?$', id_str):
        return "refseq_ids"

    # === PDB ID ===
    # 通常是4字符，如 1A2B，可能后面带链如 1A2B_A
    if re.match(r'^[0-9][A-Z0-9]{3}(_[A-Z])?$', id_str, re.IGNORECASE):
        return "pdb_ids"

    # === MGnify ID ===
    # 格式如 MGYA000000000（极少出现）
    if re.match(r'^MGY[A-Z0-9]+$', id_str):
        return "mgnify_ids"

    # === 其他或自定义 ===
    return "other_ids"

# === 初始化新列 ===
id_columns = ['uniprot_ids', 'genbank_ids', 'pdb_ids', 'refseq_ids', 'mgnify_ids', 'other_ids']
for col in id_columns:
    df[col] = ""

# === 分类填入 ===
for idx, row in df.iterrows():
    id_val = str(row['id']).strip()
    category = classify_id(id_val)
    df.at[idx, category] = id_val

# === 删除原始 'id' 列，保存新文件 ===
df.drop(columns=['id'], inplace=True)
df.to_csv(output_path, index=False)
print(f"✅ 分类完成，已保存至: {output_path}")