import pandas as pd
from pathlib import Path
import shutil
from structures.structure_loader import get_pdb_path

# === 配置项 ===
MAIN_CSV_PATH = "/Users/shulei/PycharmProjects/Dataset/pdb_fill/PlaszymeDB_v0.2.3_pdb.csv"
OUTPUT_DIR = Path("fri_dataset")

# === 输出结构目录 ===
PDB_DIR = OUTPUT_DIR / "pdb"
FASTA_PATH = OUTPUT_DIR / "fasta" / "combined.fasta"
LABEL_PATH = OUTPUT_DIR / "labels.tsv"

# === 创建输出目录 ===
PDB_DIR.mkdir(parents=True, exist_ok=True)
FASTA_PATH.parent.mkdir(parents=True, exist_ok=True)

# === 加载主表格 ===
df = pd.read_csv(MAIN_CSV_PATH)
if "PLZ_ID" not in df.columns or "plastic" not in df.columns or "sequence" not in df.columns:
    raise ValueError("主数据集缺少必要字段：PLZ_ID, plastic, sequence")

# === 初始化输出 ===
fasta_lines = []
label_records = []

# === 主循环 ===
for _, row in df.iterrows():
    plz_id = row["PLZ_ID"]
    label = row["plastic"]
    sequence = row["sequence"]

    pdb_path = get_pdb_path(plz_id)
    if not pdb_path:
        print(f"❌ 未找到结构文件: {plz_id}")
        continue

    # === 拷贝结构文件 ===
    dst_pdb = PDB_DIR / f"{plz_id}.pdb"
    shutil.copyfile(pdb_path, dst_pdb)

    # === 收集 fasta 信息 ===
    fasta_lines.append(f">{plz_id}\n{sequence}\n")

    # === 添加标签记录 ===
    label_records.append((plz_id, label))
    print(f"✅ {plz_id} | {'experimental' if 'experimental' in pdb_path else 'predicted'}")

# === 写入合并的 FASTA 文件 ===
with open(FASTA_PATH, "w") as f:
    f.writelines(fasta_lines)

# === 写入标签文件（两列，含标题） ===
label_df = pd.DataFrame(label_records, columns=["PLZ_ID", "plastic"])
label_df.to_csv(LABEL_PATH, sep="\t", index=False)

print(f"\n📦 FRI 训练数据集导出完成，共 {len(label_records)} 条样本。")
print(f"📁 结构目录: {PDB_DIR.resolve()}")
print(f"📄 FASTA文件: {FASTA_PATH.resolve()}")
print(f"📝 标签文件: {LABEL_PATH.resolve()}")