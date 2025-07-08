import os
import time
import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# === 用户配置：输入CSV、结构保存目录、输出表格 ===
INPUT_CSV = '/Users/shulei/PycharmProjects/Dataset/pdb_fill/PlaszymeDB_v0.2.3_pdb.csv'
PDB_FOLDER = '/Users/shulei/PycharmProjects/Dataset/structures/experimental/pdb/'  # 最终路径格式为 pdb/XXXX.pdb
OUTPUT_CSV = '/Users/shulei/PycharmProjects/Dataset/structures/experimental/structure_metadata.csv'

# === 请求头：加上邮箱防限速 ===
HEADERS = {
    "User-Agent": "PlaszymeDownloader/1.0 (shuleihe@outlook.com)"
}

# === 创建结构文件夹 ===
Path(PDB_FOLDER).mkdir(parents=True, exist_ok=True)

# === 获取结构方法和分辨率（RCSB）===
def get_structure_metadata(pdb_id: str, max_retries=3):
    url = f"https://data.rcsb.org/rest/v1/core/entry/{pdb_id}"
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            data = r.json()

            method = data.get("exptl", [{}])[0].get("method", "Unknown")
            res = data.get("rcsb_entry_info", {}).get("resolution_combined", [])
            if res:
                resolution = f"{res[0]} Å" if len(res) == 1 else f"{min(res)}–{max(res)} Å"
            else:
                resolution = ""
            return method, resolution
        except Exception as e:
            print(f"⚠️ 获取 {pdb_id} 元信息失败（尝试 {attempt}）：{e}")
            time.sleep(1.5 * attempt)
    return "Unknown", ""

# === 下载结构文件（RCSB）===
def download_from_rcsb(pdb_id: str, dest_path: Path, max_retries=3):
    url = f"https://files.rcsb.org/download/{pdb_id}.pdb"
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code == 404:
                return False  # RCSB 不存在
            r.raise_for_status()
            with open(dest_path, 'w') as f:
                f.write(r.text)
            return True
        except Exception as e:
            print(f"❌ RCSB 下载 {pdb_id} 失败（尝试 {attempt}）：{e}")
            time.sleep(1.5 * attempt)
    return False

# === 下载结构文件（PDBe/UniProt）===
def download_from_uniprot(pdb_id: str, dest_path: Path):
    try:
        url = f"https://www.ebi.ac.uk/pdbe/entry-files/download/{pdb_id.lower()}.pdb"
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        with open(dest_path, 'w') as f:
            f.write(r.text)
        print(f"✅ 从 UniProt/PDBe 成功下载 {pdb_id}")
        return True
    except Exception as e:
        print(f"❌ PDBe 下载 {pdb_id} 失败: {e}")
        return False

# === 主函数 ===
def main():
    df = pd.read_csv(INPUT_CSV)
    existing_df = pd.read_csv(OUTPUT_CSV) if Path(OUTPUT_CSV).exists() else pd.DataFrame()
    records = []
    failed = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="🔍 Processing"):
        plz_id = str(row['PLZ_ID']).strip()
        pdb_ids_raw = str(row['pdb_ids']).strip()
        if not pdb_ids_raw or pdb_ids_raw.lower() == 'nan':
            continue

        pdb_ids = pdb_ids_raw.split('/') if '/' in pdb_ids_raw else [pdb_ids_raw]

        for pdb_id in pdb_ids:
            pdb_id = pdb_id.strip().upper()
            if '_' in pdb_id:
                pdb_id = pdb_id.split('_')[0]
            if not pdb_id:
                continue

            # 若已记录，跳过
            is_recorded = (
                not existing_df.empty and
                ((existing_df['pdb_id'] == pdb_id) & (existing_df['PLZ_ID'] == plz_id)).any()
            )
            if is_recorded:
                print(f"⏭️ 已记录 {pdb_id}，跳过")
                continue

            pdb_path = Path(PDB_FOLDER) / f"{pdb_id}.pdb"
            rel_path = f"pdb/{pdb_id}.pdb"

            if pdb_path.exists():
                print(f"📂 本地已存在 {pdb_id}.pdb，跳过下载")
                method, resolution = get_structure_metadata(pdb_id)
                records.append({
                    "PLZ_ID": plz_id,
                    "pdb_id": pdb_id,
                    "structure_source": "Local (preexisting)",
                    "structure_method": method,
                    "resolution": resolution,
                    "source_file_path": rel_path
                })
                continue

            # 开始下载
            source = ""
            if download_from_rcsb(pdb_id, pdb_path):
                source = "RCSB_PDB"
            elif download_from_uniprot(pdb_id, pdb_path):
                source = "PDBe"
            else:
                failed.append({"PLZ_ID": plz_id, "pdb_id": pdb_id})
                continue

            method, resolution = get_structure_metadata(pdb_id)
            records.append({
                "PLZ_ID": plz_id,
                "pdb_id": pdb_id,
                "structure_source": source,
                "structure_method": method,
                "resolution": resolution,
                "source_file_path": rel_path
            })

            time.sleep(0.1)

    # === 合并写入结构表 ===
    final_df = pd.concat([existing_df, pd.DataFrame(records)], ignore_index=True)
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ 已写入或更新结构信息表：{OUTPUT_CSV}")

    # === 输出失败记录 ===
    if failed:
        failed_csv = Path(OUTPUT_CSV).with_name("download_failed.csv")
        pd.DataFrame(failed).to_csv(failed_csv, index=False)
        print(f"⚠️ 下载失败记录已保存至：{failed_csv}")

if __name__ == "__main__":
    main()