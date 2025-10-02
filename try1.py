from rdkit.Chem import AllChem
from rdkit import Chem
from rdkit.Chem import SDWriter
import os
from mol_try import generate_polymer


def generate_diverse_conformers(mol, num_confs=5, optimize=True, output_dir="conformers", base_filename="conf"):
    """
    生成多样化构象并保存为单独的SDF文件

    参数:
        mol: RDKit分子对象
        num_confs: 要生成的构象数量
        optimize: 是否对每个构象进行力场优化
        output_dir: 输出文件保存目录
        base_filename: 输出文件名的前缀
    """
    # 创建输出目录（如果不存在）
    os.makedirs(output_dir, exist_ok=True)

    # 创建分子副本
    mol_copy = Chem.Mol(mol)
    mol_copy.RemoveAllConformers()

    # 使用ETKDGv3方法生成多样化的构象
    params = AllChem.ETKDGv3()
    params.randomSeed = 42
    params.numThreads = 0  # 使用所有可用核心
    conf_ids = AllChem.EmbedMultipleConfs(mol_copy, numConfs=num_confs, params=params)

    if optimize:
        # 对每个构象进行优化
        for conf_id in conf_ids:
            try:
                # 使用UFF力场进行优化
                AllChem.UFFOptimizeMolecule(mol_copy, confId=conf_id)
            except:
                try:
                    # 如果UFF失败，尝试MMFF
                    AllChem.MMFFOptimizeMolecule(mol_copy, confId=conf_id)
                except:
                    print(f"警告: 构象 {conf_id} 优化失败")

    # 保存每个构象为单独的SDF文件
    for conf_id in conf_ids:
        # 创建只包含当前构象的分子副本
        single_conf_mol = Chem.Mol(mol_copy)
        single_conf_mol.RemoveAllConformers()

        # 添加当前构象
        conf = Chem.Conformer(mol_copy.GetConformer(conf_id))
        single_conf_mol.AddConformer(conf)

        # 生成文件名
        filename = os.path.join(output_dir, f"{base_filename}_{conf_id + 1}.sdf")

        # 写入文件
        writer = SDWriter(filename)
        writer.write(single_conf_mol)
        writer.close()
        print(f"已保存构象 {conf_id + 1} 到 {filename}")

    # 返回包含所有构象的分子
    return mol_copy

def main_rdkit(repeat, left, right, n_unit, output_folder, output_filename):
    mol = generate_polymer(repeat, n_units=n_unit, left_cap=left, right_cap=right)

    mol = Chem.AddHs(mol)

    # 生成5个构象并保存到"my_conformers"目录
    diverse_mol = generate_diverse_conformers(
        mol,
        num_confs=5,
        output_dir=output_folder,
        base_filename=output_filename
    )

    print(f"生成完成，共{diverse_mol.GetNumConformers()}个构象")
# 使用示例
if __name__ == '__main__':
    # 创建一个示例分子
    nylon_smiles = "*OCCOC(=O)c1ccc(cc1)C(=O)*"  # 己内酰胺单元
    left_cap = "[H]*"  # 乙二醇封端 (HO-CH2-CH2-O-)
    right_cap = "*O"  # 氢封端
    mol = generate_polymer(nylon_smiles, n_units=3, left_cap=left_cap, right_cap=right_cap)

    mol = Chem.AddHs(mol)

    # 生成5个构象并保存到"my_conformers"目录
    diverse_mol = generate_diverse_conformers(
        mol,
        num_confs=5,
        output_dir="try",
        base_filename="ethanol_conf"
    )

    print(f"生成完成，共{diverse_mol.GetNumConformers()}个构象")