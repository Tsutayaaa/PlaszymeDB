import pandas as pd
import re

# 提取 UniProt ID
def extract_uniprot_id(link):
    if pd.isna(link):
        return None
    match = re.search(r'uniprot\.org/(?:uniprot|uniprotkb)/([A-Z0-9]+)', link)
    return match.group(1) if match else None

# 提取 NCBI Gene ID
def extract_ncbi_gene_id(link):
    if pd.isna(link):
        return None
    # 匹配 nuccore 或 nucleotide 类型链接
    match = re.search(r'ncbi\.nlm\.nih\.gov/(?:nuccore|nucleotide)/([\w\.]+)', link)
    return match.group(1) if match else None

# 判断是否应该删除的行（只有 Genome Links）
def should_remove_row(row):
    return (
        pd.isna(row.get("GenbankID")) and
        pd.isna(row.get("NCBI Links for Genes")) and
        pd.isna(row.get("UniProt links")) and
        pd.notna(row.get("NCBI Genome Links for Microbes"))
    )

# 主函数
def clean_pmbd_file(input_path, output_path):
    df = pd.read_excel(input_path)

    # 删除无效行
    df = df[~df.apply(should_remove_row, axis=1)].copy()

    # 替换链接为 ID
    df["NCBI Links for Genes"] = df["NCBI Links for Genes"].apply(extract_ncbi_gene_id)
    df["UniProt links"] = df["UniProt links"].apply(extract_uniprot_id)

    # 删除无用列
    df.drop(columns=["NCBI Genome Links for Microbes"], inplace=True)

    # Plastic 列移动到最前
    plastic_col = df.pop("Plastic")
    df.insert(0, "Plastic", plastic_col)

    # 保存结果
    df.to_csv(output_path, index=False)
    print(f"✅ 清洗完成，已保存到：{output_path}")

# 调用示例
if __name__ == "__main__":
    clean_pmbd_file("PMBD.xlsx", "PMBD_cleaned.csv")