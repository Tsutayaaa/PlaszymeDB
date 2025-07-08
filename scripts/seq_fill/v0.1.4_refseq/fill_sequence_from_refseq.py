import pandas as pd
import requests
from time import sleep

# === 参数区 ===
INPUT_CSV = "/Users/shulei/PycharmProjects/Dataset/seq_fill/v0.1.3_pdb/PlaszymeDB_v0.1.3_(FilledByPDB).csv"
OUTPUT_CSV = "PlaszymeDB_v0.1.4.1_(ManualReviewed).csv"
ERROR_LOG = "refseq_sequence_retrieval_errors.txt"
EMAIL = "shuleihe@outlook.com"  # ⚠️ 修改为你的邮箱以符合 NCBI 要求
DELAY = 0.3  # 请求间隔秒数（可以安全调至 0.3–0.5）

# === 数据加载 ===
df = pd.read_csv(INPUT_CSV)
error_logs = []

# === RefSeq → FASTA 解析函数 ===
def fetch_refseq_sequence(refseq_id):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db": "protein",
        "id": refseq_id,
        "rettype": "fasta",
        "retmode": "text",
        "email": EMAIL
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200 and response.text.startswith(">"):
        lines = response.text.strip().splitlines()
        return "".join(lines[1:])  # 去掉描述行
    else:
        raise ValueError(f"Fetch failed (HTTP {response.status_code}) or invalid format")

# === 遍历 DataFrame ===
for idx, row in df.iterrows():
    if pd.isna(row.get("sequence")) or str(row["sequence"]).strip() == "":
        raw_ids = row.get("refseq_ids", "")
        if pd.isna(raw_ids) or str(raw_ids).strip() == "":
            continue  # 无 refseq_id，跳过

        id_list = [i.strip() for i in str(raw_ids).split("/") if i.strip()]
        if len(id_list) != 1:
            error_logs.append(f"[Line {idx}] RefSeq ID = '{raw_ids}' → Skipped (not one unique ID)")
            continue

        refseq_id = id_list[0]
        try:
            seq = fetch_refseq_sequence(refseq_id)
            df.at[idx, "sequence"] = seq
            print(f"[Line {idx}] ✅ RefSeq ID '{refseq_id}' filled.")
        except Exception as e:
            error_logs.append(f"[Line {idx}] RefSeq ID = '{refseq_id}' → Error: {str(e)}")
            print(f"[Line {idx}] ❌ RefSeq ID = '{refseq_id}' → {str(e)}")
        sleep(DELAY)

# === 保存结果 ===
df.to_csv(OUTPUT_CSV, index=False)
print(f"\n✅ 序列补全完成，保存至: {OUTPUT_CSV}")

with open(ERROR_LOG, "w") as f:
    f.write("\n".join(error_logs))
print(f"⚠️ 错误信息写入: {ERROR_LOG}（共 {len(error_logs)} 条）")