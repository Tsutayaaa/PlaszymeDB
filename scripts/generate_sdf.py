
import os
from rdkit import Chem
# from rdkit import Chem
from rdkit.Chem import AllChem
# from rdkit import Chem
from rdkit.Chem import RWMol, Atom, BondType
import time

from rdkit.VLib.NodeLib.demo import output


def generate_polymer(repeat_smiles, n_units, left_cap, right_cap):
    # 生成重复单元链
    core = repeat_smiles.strip('*')
    # core = repeat_smiles.strip('*')
    polymer_smiles = left_cap.strip('*') + core * n_units + right_cap.strip('*')
    # polymer_smiles = left_cap + core * n_units + right_cap
    # print(polymer_smiles)

    # 创建分子对象
    mol = Chem.MolFromSmiles(polymer_smiles)
    if mol is None:
        raise ValueError("SMILES解析失败，请检查化学结构")

    # 添加氢并生成3D结构
    if mol is not None:
        # 添加氢原子（UFF优化需要完整的分子结构）
        mol = Chem.AddHs(mol)

        # 生成初始3D构象
        params = AllChem.ETKDGv3()
        params.randomSeed = int(time.time())  # 使用时间作为随机种子
        params.useRandomCoords = True  # 使用随机坐标生成初始构象

        # 尝试多次生成初始构象
        for attempt in range(5):
            print(f"尝试生成初始构象 ({attempt + 1}/5)...")
            status = AllChem.EmbedMolecule(mol, params)
            # AllChem.UFFOptimizeMolecule(mol)
            if status == 0:
                print("初始构象生成成功")
                break
            else:
                print(f"尝试 {attempt + 1} 失败，重新尝试...")
    else:
        print("无法从SMILES创建分子对象")

    return mol

def generate_polymer_smile(smiles):
    mol = Chem.MolFromSmiles(smiles)
    if mol is not None:
        # 添加氢原子（UFF优化需要完整的分子结构）
        mol = Chem.AddHs(mol)

        # 生成初始3D构象
        params = AllChem.ETKDGv3()
        params.randomSeed = int(time.time())  # 使用时间作为随机种子
        params.useRandomCoords = True  # 使用随机坐标生成初始构象

        # 尝试多次生成初始构象
        for attempt in range(5):
            print(f"尝试生成初始构象 ({attempt + 1}/5)...")
            status = AllChem.EmbedMolecule(mol, params)
            # AllChem.UFFOptimizeMolecule(mol)
            if status == 0:
                print("初始构象生成成功")
                break
            else:
                print(f"尝试 {attempt + 1} 失败，重新尝试...")
    else:
        print("无法从SMILES创建分子对象")

    return mol

if __name__ == '__main__':
    total_num = 10
    num1 = round(total_num * 0.7)
    num2 = round(total_num * 0.15)
    num3 = round(total_num * 0.1)
    num4 = total_num - num1 - num2 - num3
    left = "[H]"
    right = "O"
    repeat1 = "OC(C)CC(=O)"
    repeat2 = "OC(CC)CC(=O)"
    repeat3 = "OC(CCC)CC(=O)"
    repeat4 = "OC(CCCC)CC(=O)"
    smiles =  left + repeat1 * num1 + repeat2 * num2 + repeat3 * num3 + repeat4*num4 +right
    mol = generate_polymer_smile(smiles)
    output_dir = "mols_for_unimol_10_sdf"
    name = "PHBVH"
    mol_path = os.path.join(output_dir, f"{name}.sdf")
    os.makedirs(os.path.dirname(mol_path), exist_ok=True)
    with Chem.SDWriter(mol_path) as writer:
        writer.write(mol)

    print(f"✅ [{name}] -> {mol_path}")




