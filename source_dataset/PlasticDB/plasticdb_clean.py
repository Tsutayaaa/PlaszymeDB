import pandas as pd

def clean_plasticdb(input_path, output_path):
    # 读取 CSV
    df = pd.read_csv(input_path)

    # 仅保留 Gene = Yes 的行（大小写不敏感）
    df = df[df['Gene'].astype(str).str.strip().str.lower() == 'yes'].copy()

    # 清洗 GenbankID 中的非法字符
    if 'GenbankID' in df.columns:
        df['GenbankID'] = df['GenbankID'].astype(str).str.replace(r'[\xa0\s]+', '', regex=True)

    # 将 'Plastic' 列移动到第一列
    if 'Plastic' in df.columns:
        cols = ['Plastic'] + [col for col in df.columns if col != 'Plastic']
        df = df[cols]

    # 保存为新文件
    df.to_csv(output_path, index=False)
    print(f"✅ Cleaned PlasticDB saved to {output_path}")

# 示例调用
if __name__ == "__main__":
    clean_plasticdb("PlasticDB.csv", "PlasticDB_cleaned.csv")