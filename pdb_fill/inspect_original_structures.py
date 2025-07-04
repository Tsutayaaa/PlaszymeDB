import csv
import requests

INPUT_CSV = "/Users/shulei/PycharmProjects/Dataset/pdb_fill/PlaszymeDB_v0.2.3_pdb.csv"
OUTPUT_CSV = "PlaszymeDB_v0.2.4_pdb_fixed.csv"
EMAIL = "shuleihe@outlook.com"

HEADERS = {
    "User-Agent": f"Plaszyme-OriginFixer/1.0 ({EMAIL})"
}


def get_best_method(uniprot_id, pdb_ids):
    url = f"https://rest.uniprot.org/uniprotkb/{uniprot_id}.txt"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return "UNKNOWN"

        lines = r.text.splitlines()
        pdb_records = []
        for line in lines:
            if line.startswith("DR   PDB;"):
                parts = line.strip().split(";")
                if len(parts) >= 4:
                    pdb_id = parts[1].strip()
                    method = parts[2].strip()
                    try:
                        resolution = float(parts[3].strip().split()[0]) if "A" in parts[3] else 100.0
                    except:
                        resolution = 100.0
                    pdb_records.append((pdb_id, method, resolution))

        target_ids = [pid.strip() for pid in pdb_ids.split("/") if pid.strip()]
        filtered = [(pid, m, res) for (pid, m, res) in pdb_records if pid in target_ids]

        if not filtered:
            return "UNKNOWN"

        best = sorted(filtered, key=lambda x: x[2])[0]  # resolution 最小
        return best[1]

    except:
        return "UNKNOWN"


# === 主流程 ===
with open(INPUT_CSV, newline='') as infile, open(OUTPUT_CSV, "w", newline='') as outfile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        if row.get("structure_source") == "From original":
            pdb_ids = row.get("pdb_ids", "").strip()
            uniprot_ids = row.get("uniprot_ids", "").strip()
            new_source = "UNKNOWN"

            if pdb_ids and uniprot_ids:
                for uid in uniprot_ids.split("/"):
                    uid = uid.strip()
                    if uid:
                        method = get_best_method(uid, pdb_ids)
                        if method != "UNKNOWN":
                            new_source = method
                            break

            row["structure_source"] = new_source

        writer.writerow(row)

print(f"✅ 替换完成，新文件已保存为: {OUTPUT_CSV}")