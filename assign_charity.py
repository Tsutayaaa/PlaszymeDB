from rdkit import Chem
from rdkit.Chem import AllChem, SDWriter
import logging
from generate_sdf import generate_polymer
import os
import datetime
import sys
import shutil
from rdkit.Chem import rdCIPLabeler  # 新增：用于手性标记

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


# 新增函数：自动检测和分配手性中心
def assign_stereochemistry(mol):
    """
    自动检测并分配分子中的手性中心
    返回带有手性标记的分子
    """
    try:
        # 复制分子以避免修改原始对象
        mol_copy = Chem.Mol(mol)

        # 添加氢原子以便完整分析
        mol_copy = Chem.AddHs(mol_copy)

        # 生成3D坐标（如果不存在）
        if not mol_copy.GetNumConformers():
            AllChem.EmbedMolecule(mol_copy)

        # 从3D坐标推断立体化学
        Chem.AssignStereochemistryFrom3D(mol_copy)

        # 分配CIP标签（R/S）
        rdCIPLabeler.AssignCIPLabels(mol_copy)

        # 验证所有手性中心是否已定义
        chiral_centers = Chem.FindMolChiralCenters(mol_copy, includeUnassigned=True)
        undefined = [idx for idx, tag in chiral_centers if tag == '?']

        if undefined:
            logger.warning(f"仍有未定义的手性中心: {undefined}")
            # 对于未定义的手性中心，设置为任意构型
            for atom_idx in undefined:
                atom = mol_copy.GetAtomWithIdx(atom_idx)
                atom.SetChiralTag(Chem.ChiralType.CHI_TETRAHEDRAL_CCW)  # 设为R构型
                atom.SetProp("_CIPCode", "R")
        print("已分配手性")
        return mol_copy

    except Exception as e:
        logger.error(f"手性分配失败: {str(e)}")
        return mol  # 返回原始分子

if __name__ == '__main__':
    left = "[H]"
    repeat = "OCCOC(=O)c1ccc(cc1)C(=O)"
    right = "O"
    mol = generate_polymer(repeat, n_units=3, left_cap=left, right_cap=right)
    assign_stereochemistry(mol)