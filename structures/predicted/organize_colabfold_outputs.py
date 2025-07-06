import os
import re
import shutil
import pandas as pd

# ====== 用户配置区域 ======
INPUT_DIR = "/Users/shulei/PycharmProjects/Dataset/structures/predicted/pdb_1"
OUTPUT_PDB_DIR = "./pdb"
OUTPUT_JSON_DIR = "./json"
CSV_OUTPUT_PATH = "./prediction_summary.csv"
# ==========================

# 获取 CSV 目录的绝对路径，用于相对路径计算
base_path = os.path.dirname(os.path.abspath(CSV_OUTPUT_PATH))

os.makedirs(OUTPUT_PDB_DIR, exist_ok=True)
os.makedirs(OUTPUT_JSON_DIR, exist_ok=True)

new_records = []

for subfolder in os.listdir(INPUT_DIR):
    sub_path = os.path.join(INPUT_DIR, subfolder)
    if not os.path.isdir(sub_path):
        continue

    sample_id = subfolder
    pdb_file = os.path.join(sub_path, f"{sample_id}.pdb")
    json_file = os.path.join(sub_path, f"{sample_id}.json")
    log_file = os.path.join(sub_path, "log.txt")

    pdb_rel_path = ""
    json_rel_path = ""
    plddt = ""
    ptm = ""

    # PDB 文件复制
    dest_pdb = os.path.join(OUTPUT_PDB_DIR, f"{sample_id}.pdb")
    if os.path.isfile(pdb_file):
        if not os.path.exists(dest_pdb):
            shutil.copy2(pdb_file, dest_pdb)
        pdb_rel_path = os.path.relpath(dest_pdb, start=base_path)

    # JSON 文件复制
    dest_json = os.path.join(OUTPUT_JSON_DIR, f"{sample_id}.json")
    if os.path.isfile(json_file):
        if not os.path.exists(dest_json):
            shutil.copy2(json_file, dest_json)
        json_rel_path = os.path.relpath(dest_json, start=base_path)

    # 提取日志中的 pLDDT / pTM
    if os.path.isfile(log_file):
        with open(log_file, "r") as f:
            lines = f.readlines()
        for line in reversed(lines):
            if "rank_001" in line and "pLDDT=" in line and "pTM=" in line:
                match = re.search(r"pLDDT=([\d.]+)\s+pTM=([\d.]+)", line)
                if match:
                    plddt = float(match.group(1))
                    ptm = float(match.group(2))
                break

    new_records.append({
        "PLZ_ID": sample_id,
        "pLDDT": plddt,
        "pTM": ptm,
        "pdb_path": pdb_rel_path,
        "json_path": json_rel_path,
    })

# 合并已有 CSV 文件并去重优化
if os.path.exists(CSV_OUTPUT_PATH):
    existing_df = pd.read_csv(CSV_OUTPUT_PATH)
    existing_df.set_index("PLZ_ID", inplace=True)
    for record in new_records:
        sid = record["PLZ_ID"]
        if sid in existing_df.index:
            row = existing_df.loc[sid]

            # 判断是否为空记录
            if row.isnull().all() or row.dropna().eq("").all():
                print(f"🔁 Updating empty entry for {sid}")
                existing_df.loc[sid] = pd.Series(record).drop("PLZ_ID")
                continue

            # 比较质量（pLDDT & pTM）
            try:
                old_plddt = float(row.get("pLDDT", 0) or 0)
                old_ptm = float(row.get("pTM", 0) or 0)
                new_plddt = float(record["pLDDT"] or 0)
                new_ptm = float(record["pTM"] or 0)
            except (ValueError, TypeError):
                print(f"⏭️ Skipping {sid} due to invalid score format.")
                continue

            if (new_plddt, new_ptm) > (old_plddt, old_ptm):
                print(f"🔄 Replacing {sid} with better scores: ({old_plddt}, {old_ptm}) → ({new_plddt}, {new_ptm})")
                existing_df.loc[sid] = pd.Series(record).drop("PLZ_ID")
            elif (new_plddt, new_ptm) == (old_plddt, old_ptm):
                print(f"🔁 Same scores for {sid}, keeping new file paths.")
                for field in ["pdb_path", "json_path"]:
                    existing_df.at[sid, field] = record[field]
            else:
                print(f"⚠️ Duplicate {sid} found, keeping existing better score.")
        else:
            existing_df.loc[sid] = pd.Series(record).drop("PLZ_ID")

    combined_df = existing_df.reset_index()
else:
    combined_df = pd.DataFrame(new_records)

# 保存最终表格
os.makedirs(os.path.dirname(CSV_OUTPUT_PATH), exist_ok=True)
combined_df.to_csv(CSV_OUTPUT_PATH, index=False)