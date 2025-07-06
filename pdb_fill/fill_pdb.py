import csv
import requests

INPUT_CSV = "/Users/shulei/PycharmProjects/Dataset/pdb_fill/PlaszymeDB_v0.2.2.csv"
OUTPUT_CSV = "PlaszymeDB_v0.2.3_pdb.csv"
EMAIL = "shuleihe@outlook.com"
HEADERS = {
    "User-Agent": f"Plaszyme-StructureCheck/1.0 ({EMAIL})"
}

def get_structure_info(uniprot_id):
    url = f"https://rest.uniprot.org/uniprotkb/{uniprot_id}.txt"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return "", ""
        for line in r.text.splitlines():
            if line.startswith("DR   PDB;"):
                parts = line.strip().split(";")
                if len(parts) >= 3:
                    return parts[1].strip(), parts[2].strip()  # PDB_ID, Method
        return "", "AlphaFoldDB"
    except:
        return "", ""

# === 主体逻辑 ===
with open(INPUT_CSV, newline='') as infile, open(OUTPUT_CSV, "w", newline='') as outfile:
    reader = list(csv.DictReader(infile))
    fieldnames = reader[0].keys()

    # 插入结构来源字段
    fieldnames = list(fieldnames)
    if "pdb_ids" not in fieldnames:
        fieldnames.append("pdb_ids")
    if "structure_source" not in fieldnames:
        fieldnames.insert(fieldnames.index("sequence_source") + 1, "structure_source")

    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    total = len(reader)
    filled = 0
    missing = 0

    for idx, row in enumerate(reader):
        print(f"[{idx+1}/{total}] Processing PLZ_ID: {row.get('PLZ_ID', '(no ID)')}")

        if not row.get("pdb_ids"):
            uid_list = [uid.strip() for uid in row.get("uniprot_ids", "").split("/") if uid.strip()]
            for uid in uid_list:
                print(f"   🔍 Trying UniProt ID: {uid}")
                pdb_id, source = get_structure_info(uid)
                if pdb_id or source:
                    row["pdb_ids"] = pdb_id
                    row["structure_source"] = source
                    print(f"   ✅ Found structure: {pdb_id or '[None]'} ({source})")
                    filled += 1
                    break
            else:
                row["structure_source"] = ""
                print("   ❌ No structure found.")
                missing += 1
        else:
            row["structure_source"] = "From original"
            print("   ✅ Structure already present.")

        writer.writerow(row)

    print("\n=== ✅ Structure filling complete ===")
    print(f"Total entries processed: {total}")
    print(f"New structures filled:   {filled}")
    print(f"Still missing structures:{missing}")
    print("Output written to:", OUTPUT_CSV)