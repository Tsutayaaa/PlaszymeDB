import pandas as pd
import requests
from time import sleep

# === 参数配置 ===
INPUT_CSV = "/Users/shulei/PycharmProjects/Dataset/seq_fill/v0.1.2_genbank/PlaszymeDB_v0.1.2_(FilledByGenBank).csv"
OUTPUT_CSV = "PlaszymeDB_v0.1.3_(FilledByPDB).csv"
ERROR_LOG = "pdb_sequence_retrieval_errors.txt"

# === 加载数据 ===
df = pd.read_csv(INPUT_CSV)
error_logs = []

# === PDB 序列检索函数 ===
def fetch_pdb_sequence(pdb_id):
    pdb_id = pdb_id.lower()
    url = f"https://www.rcsb.org/fasta/entry/{pdb_id}"
    response = requests.get(url)
    if response.status_code == 200 and response.text.startswith(">"):
        lines = response.text.strip().splitlines()
        return "".join(lines[1:])
    else:
        raise ValueError(f"Fetch failed (HTTP {response.status_code}) or invalid format")

# === 遍历数据并填充 ===
for idx, row in df.iterrows():
    if pd.isna(row.get("sequence")) or str(row["sequence"]).strip() == "":
        raw_ids = row.get("pdb_ids", "")
        if pd.isna(raw_ids) or str(raw_ids).strip() == "":
            continue  # 无 PDB ID

        id_list = [i.strip() for i in str(raw_ids).split("/") if i.strip()]
        sequences = []

        for pdb_id in id_list:
            try:
                seq = fetch_pdb_sequence(pdb_id)
                sequences.append(seq)
                print(f"[Line {idx}] ✅ PDB ID '{pdb_id}' fetched.")
                sleep(0.3)
            except Exception as e:
                error_logs.append(f"[Line {idx}] PDB ID = '{pdb_id}' → Error: {str(e)}")
                print(f"[Line {idx}] ❌ PDB ID = '{pdb_id}' → Error: {str(e)}")

        # 判断是否所有序列一致
        unique_seqs = list(set(sequences))
        if len(unique_seqs) == 1:
            df.at[idx, "sequence"] = unique_seqs[0]
            print(f"[Line {idx}] ✅ Sequences from all PDB IDs matched and filled.")
        elif len(unique_seqs) > 1:
            error_logs.append(f"[Line {idx}] PDB IDs = '{raw_ids}' → Conflicting sequences (not filled)")
            print(f"[Line {idx}] ⚠️ Sequence mismatch between multiple PDB IDs.")

# === 保存结果 ===
df.to_csv(OUTPUT_CSV, index=False)
print(f"\n✅ 填充完成，保存至: {OUTPUT_CSV}")

with open(ERROR_LOG, "w") as f:
    f.write("\n".join(error_logs))
print(f"⚠️ 错误信息写入: {ERROR_LOG}（共 {len(error_logs)} 条）")