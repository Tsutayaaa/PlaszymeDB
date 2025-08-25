import os
import csv
import pandas as pd
from typing import Optional

from numexpr.necompiler import double
from rdkit import Chem
from rdkit.Chem import AllChem
# import gbigsmiles
from  generate_sdf import generate_polymer, generate_polymer_smile
from  psmiles_preprocessor import check


def batch_generate_mols_from_csv(
    input_csv: str,
    key_column: str,
    rate_column1: str,
    rate_column2: str,
    repeat_column1: str,
    repeat_column2: str,
    left_cap: str,
    right_cap: str,
    unit_num: int,
    output_dir: str = "generated_mols",
):
    """
    从 CSV 中批量生成 .mol 文件和 SMILES 表格（用于下游模型）
    """
    df = pd.read_excel(input_csv)

    if key_column not in df.columns or repeat_column1 not in df.columns or repeat_column2 not in df.columns or rate_column1 not in df.columns or rate_column2 not in df.columns or left_cap not in df.columns or right_cap not in df.columns:
        raise ValueError(f"❌ 输入 CSV 缺少列：{key_column} 或 {repeat_column1} 或 {repeat_column2} 或 {rate_column1} 或 {rate_column2} 或 {left_cap} 或 {right_cap}")

    os.makedirs(output_dir, exist_ok=True)
    smiles_records = []

    for idx, row in df.iterrows():
        name = str(row[key_column])
        repeat1 = str(row[repeat_column1])
        repeat2 = str(row[repeat_column2])
        left = str(row[left_cap])
        right = str(row[right_cap])
        rate1 = float(row[rate_column1])


        num1 = round(unit_num * rate1)
        num2 = unit_num - num1
        smiles = left + repeat1 * num1 + repeat2 * num2 + right

        mol = generate_polymer_smile(smiles)

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
        input_csv="E:\\ohter\\trans\\plastic\\already3.xlsx",
        key_column="plastic",
        rate_column1="rate1",
        rate_column2="rate2",
        repeat_column1="repeat1",
        repeat_column2="repeat2",
        left_cap="left",
        right_cap="right",
        unit_num=10,
        output_dir="mols_for_unimol_10_sdf"
    )
