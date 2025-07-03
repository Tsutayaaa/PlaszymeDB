import pandas as pd
import requests
from time import sleep

# === 参数配置 ===
INPUT_CSV = "/Users/shulei/PycharmProjects/Dataset/PlaszymeDB_v0.1_(Raw, Unmerged).csv"
OUTPUT_CSV = "PlaszymeDB_v0.1.1_(FilledByUniprot).csv"
ERROR_LOG = "uniprot_sequence_retrieval_errors.txt"

# === 加载数据 ===
print(f"📂 读取数据文件: {INPUT_CSV}")
df = pd.read_csv(INPUT_CSV)
print(f"📊 样本总数: {len(df)}")

error_logs = []
filled_count = 0
skipped_count = 0

# === UniProt FASTA 检索函数 ===
def fetch_uniprot_sequence(uniprot_id):
    url = f"https://rest.uniprot.org/uniprotkb/{uniprot_id}.fasta"
    response = requests.get(url)
    if response.status_code == 200 and response.text.startswith(">"):
        lines = response.text.strip().splitlines()
        return "".join(lines[1:])
    else:
        raise ValueError(f"Fetch failed ({response.status_code}) or invalid format.")

# === 遍历样本并填充 sequence 列 ===
print("🚀 开始检索并填充序列...")
for idx, row in df.iterrows():
    if idx % 10 == 0:
        print(f"🔄 进度: 第 {idx}/{len(df)} 行")

    original_seq = row.get("sequence")
    if pd.isna(original_seq) or str(original_seq).strip() == "":
        raw_ids = row.get("uniprot_ids", "")
        if pd.isna(raw_ids) or str(raw_ids).strip() == "":
            skipped_count += 1
            continue  # 没有 UniProt ID

        id_list = [i.strip() for i in str(raw_ids).split("/") if i.strip()]
        if len(id_list) != 1:
            error_logs.append(f"[Line {idx}] UniProt ID = '{raw_ids}' → Skipped (not one unique ID)")
            skipped_count += 1
            continue

        uid = id_list[0]
        print(f"🔍 第 {idx} 行：尝试查询 UniProt ID: {uid}")
        try:
            seq = fetch_uniprot_sequence(uid)
            df.at[idx, "sequence"] = seq
            print(f"✅ 填充成功 → 长度: {len(seq)}")
            filled_count += 1
            sleep(0.5)  # 限速
        except Exception as e:
            error_msg = f"[Line {idx}] UniProt ID = '{uid}' → Error: {str(e)}"
            print(f"❌ {error_msg}")
            error_logs.append(error_msg)
            skipped_count += 1

# === 保存结果 ===
df.to_csv(OUTPUT_CSV, index=False)
print(f"\n✅ 补全完成，结果已保存: {OUTPUT_CSV}")
print(f"✅ 成功填充序列数: {filled_count}")
print(f"⚠️ 未处理样本数（跳过/错误）: {skipped_count}")

# === 保存错误日志 ===
with open(ERROR_LOG, "w") as f:
    f.write("\n".join(error_logs))
print(f"📝 错误日志写入: {ERROR_LOG}（共 {len(error_logs)} 条）")