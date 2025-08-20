import os
import csv
import pandas as pd
from typing import Optional
from rdkit import Chem
from rdkit.Chem import AllChem
# import gbigsmiles
from  generate_sdf import generate_polymer
from  psmiles_preprocessor import check


def batch_generate_mols_from_csv(
    input_csv: str,
    key_column: str,
    left_cap: str,
    right_cap: str,
    repeat_column: str,
    unit_num: int,
    output_dir: str = "generated_mols",
):
    """
    从 CSV 中批量生成 .mol 文件和 SMILES 表格（用于下游模型）
    """
    df = pd.read_excel(input_csv)

    if key_column not in df.columns or repeat_column not in df.columns or left_cap not in df.columns or right_cap not in df.columns:
        raise ValueError(f"❌ 输入 CSV 缺少列：{key_column} 或 {repeat_column}或{left_cap}或{right_cap}")

    os.makedirs(output_dir, exist_ok=True)
    smiles_records = []

    for idx, row in df.iterrows():
        name = str(row[key_column])
        repeat = str(row[repeat_column])
        right = str(row[right_cap])
        left = str(row[left_cap])
        check(repeat*2)
        check(right)
        check(left)

        mol = generate_polymer(repeat, unit_num, left, right)

        if mol is None:
            continue

        mol_path = os.path.join(output_dir, f"{name}.sdf")
        os.makedirs(os.path.dirname(mol_path), exist_ok=True)
        with Chem.SDWriter(mol_path) as writer:
            writer.write(mol)

        smiles = Chem.MolToSmiles(mol)
        smiles_records.append((name, smiles))

        print(f"✅ [{name}] -> {mol_path}")

    # 保存 SMILES 表格
    smiles_path = os.path.join(output_dir, "generated_smiles1.csv")
    with open(smiles_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "SMILES"])
        writer.writerows(smiles_records)

    print(f"\n📄 已保存 SMILES 表格至: {smiles_path}")


# === 示例调用 ===
if __name__ == "__main__":
    batch_generate_mols_from_csv(
        input_csv="E:\\ohter\\trans\\plastic\\already2.xlsx",
        key_column="plastic",
        left_cap="left",
        right_cap="right",
        repeat_column="repeat",
        unit_num=5,
        output_dir="mols_for_unimol_10_sdf"
    )