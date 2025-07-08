import pandas as pd
import requests
from time import sleep

# === 参数配置 ===
INPUT_CSV = "/Users/shulei/PycharmProjects/Dataset/seq_fill/v0.1.1_uniport/PlaszymeDB_v0.1.1_(FilledByUniprot).csv"
OUTPUT_CSV = "PlaszymeDB_v0.1.2_(FilledByGenBank).csv"
ERROR_LOG = "genbank_sequence_retrieval_errors.txt"

# === 加载数据 ===
df = pd.read_csv(INPUT_CSV)

# === 初始化错误日志 ===
error_logs = []

# === GenBank FASTA 检索函数 ===
EMAIL = "shuleihe@outlook.com"     # ← 填写你的真实邮箱
TOOL_NAME = "plaszyme_seq_filler"

def fetch_genbank_sequence(genbank_id):
    efetch_url = (
        f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?"
        f"db=protein&id={genbank_id}&rettype=fasta&retmode=text"
        f"&tool={TOOL_NAME}&email={EMAIL}"
    )
    response = requests.get(efetch_url)
    if response.status_code == 200 and response.text.startswith(">"):
        lines = response.text.strip().splitlines()
        return "".join(lines[1:])
    else:
        raise ValueError(f"Fetch failed (HTTP {response.status_code}) or invalid format")

# === 遍历样本并填充 sequence 列 ===
for idx, row in df.iterrows():
    original_seq = row.get("sequence")
    if pd.isna(original_seq) or str(original_seq).strip() == "":
        raw_ids = row.get("genbank_ids", "")
        if pd.isna(raw_ids) or str(raw_ids).strip() == "":
            continue

        id_list = [i.strip() for i in str(raw_ids).split("/") if i.strip()]
        if len(id_list) != 1:
            error_logs.append(f"[Line {idx}] GenBank ID = '{raw_ids}' → Skipped (not one unique ID)")
            continue

        gid = id_list[0]
        try:
            seq = fetch_genbank_sequence(gid)
            df.at[idx, "sequence"] = seq
            print(f"✅ [Line {idx}] GenBank ID '{gid}' → Sequence filled.")
            sleep(0.1)
        except Exception as e:
            error_logs.append(f"[Line {idx}] GenBank ID = '{gid}' → Error: {str(e)}")
            print(f"❌ [Line {idx}] GenBank ID '{gid}' → {str(e)}")

# === 保存结果 ===
df.to_csv(OUTPUT_CSV, index=False)
print(f"\n✅ GenBank 补全完成，已保存至: {OUTPUT_CSV}")

# === 保存错误日志 ===
with open(ERROR_LOG, "w") as f:
    f.write("\n".join(error_logs))

print(f"⚠️ 错误信息已保存至: {ERROR_LOG}（共 {len(error_logs)} 条）")