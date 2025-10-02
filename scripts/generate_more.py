from openmm_generate_diverse import main_openmm
from rdkit_generate_diverse import main_rdkit
import os
import csv
import pandas as pd


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
        name1 = name+"rdkit"
        name2 = name+"openmm"

        main_openmm(left,repeat,right,n_units=unit_num, output_folder=output_dir, output_filename=name2)
        main_rdkit(repeat,left,right,n_unit=unit_num, output_folder=output_dir, output_filename=name1)

        print(f"✅ [{name}] -> {output_dir}/{name}")

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
        input_csv="E:\\ohter\\trans\\plastic\\plastic_new.xlsx",
        key_column="plastic",
        left_cap="left",
        right_cap="right",
        repeat_column="repeat",
        unit_num=3,
        output_dir="mols_for_unimol_3_sdf_rdkit_openmm"
    )
