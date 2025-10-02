from rdkit import Chem
from rdkit.Chem import AllChem, SDWriter
import logging
from generate_sdf import generate_polymer
import os
import datetime
import sys
import shutil
from assign_charity import assign_stereochemistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def apply_md_perturbation_openmm(mol, output_folder, output_filename,
                                 steps=500, temperature=350, confId=0):
    """
    使用OpenMM进行MD扰动并保存结果到SDF文件，使用GAFF2力场

    参数:
        mol: RDKit分子对象
        output_folder: 输出文件保存目录
        output_filename: 输出文件名（不含扩展名）
        steps: MD模拟步数
        temperature: 模拟温度(K)
        confId: 使用的构象ID
    """
    try:
        from openmm import app, unit, Vec3
        from openmm import LangevinMiddleIntegrator
        from openmmforcefields.generators import GAFFTemplateGenerator
        from openff.toolkit.topology import Molecule as OFFMolecule
    except ImportError:
        logger.error("OpenMM或相关库未安装，无法运行MD模拟")
        return mol, None

    try:
        # 创建分子的副本
        mol_copy = Chem.Mol(mol)

        # 将RDKit分子转换为OpenFF分子对象
        off_mol = OFFMolecule.from_rdkit(mol_copy)

        # 创建GAFF2力场生成器
        gaff = GAFFTemplateGenerator(molecules=off_mol, forcefield='gaff-2.11')

        # 创建力场系统 (AMBER14 + GAFF2)
        forcefield = app.ForceField('amber14-all.xml')
        forcefield.registerTemplateGenerator(gaff.generator)

        # 在当前工作目录下创建临时目录
        temp_dir = os.path.join(os.getcwd(), "md_temp")
        os.makedirs(temp_dir, exist_ok=True)

        # 创建临时PDB文件路径
        temp_pdb_path = os.path.join(temp_dir, "temp_molecule.pdb")

        # 将分子写入临时PDB文件
        Chem.MolToPDBFile(mol_copy, temp_pdb_path, confId=confId)

        # 检查文件是否成功创建
        if not os.path.exists(temp_pdb_path):
            raise IOError(f"临时PDB文件创建失败: {temp_pdb_path}")
        else:
            logger.info(f"临时PDB文件已创建: {temp_pdb_path}")

        # 读取PDB文件
        pdb_file = app.PDBFile(temp_pdb_path)
        topology = pdb_file.topology
        positions = pdb_file.positions

        # 创建分子系统 - 添加周期性边界条件(PBC)和PME长程静电
        system = forcefield.createSystem(
            topology,
            nonbondedMethod=app.PME,  # 粒子网格埃瓦尔德方法
            nonbondedCutoff=1.0 * unit.nanometer,  # 非键截断距离
            constraints=app.HBonds,  # 约束键长
            rigidWater=True,  # 约束水分子
            ewaldErrorTolerance=0.0005  # PME误差容限
        )

        # 创建朗之万积分器
        integrator = LangevinMiddleIntegrator(
            temperature * unit.kelvin,
            1.0 / unit.picosecond,  # 摩擦系数
            0.002 * unit.picoseconds  # 时间步长
        )

        # 创建分子动力学模拟器
        simulation = app.Simulation(topology, system, integrator)
        simulation.context.setPositions(positions)

        # 能量最小化
        simulation.minimizeEnergy(maxIterations=200)

        # 运行MD模拟
        simulation.step(steps)

        # 获取最终位置
        state = simulation.context.getState(getPositions=True)
        final_positions = state.getPositions()

        # 更新分子构象
        conf_copy = Chem.Conformer(mol_copy.GetNumAtoms())
        for i, pos in enumerate(final_positions):
            # OpenMM的位置单位是纳米，RDKit使用埃，所以需要转换 (1 nm = 10 Å)
            conf_copy.SetAtomPosition(i, (pos.x * 10, pos.y * 10, pos.z * 10))

        # 移除旧构象，添加新构象
        mol_copy.RemoveConformer(0)
        mol_copy.AddConformer(conf_copy)

        # 确保目录存在
        os.makedirs(output_folder, exist_ok=True)

        # 添加时间戳确保文件名唯一
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_filename}_{timestamp}_{temperature}.sdf"
        output_path = os.path.join(output_folder, filename)

        # 写入SDF文件
        with SDWriter(output_path) as writer:
            writer.write(mol_copy)
        logger.info(f"MD模拟结果已保存到: {output_path}")

        # 添加模拟参数作为分子属性
        mol_copy.SetProp("MD_Steps", str(steps))
        mol_copy.SetProp("MD_Temperature", str(temperature))
        mol_copy.SetProp("Simulation_Date", timestamp)
        mol_copy.SetProp("ForceField", "GAFF2")

        # 清理临时文件
        try:
            if os.path.exists(temp_pdb_path):
                os.remove(temp_pdb_path)
                logger.info(f"已清理临时文件: {temp_pdb_path}")
        except Exception as e:
            logger.warning(f"清理临时文件失败: {str(e)}")

        return mol_copy, output_path

    except Exception as e:
        logger.error(f"OpenMM MD模拟失败: {str(e)}")
        # 获取详细的错误信息
        import traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        tb_text = ''.join(tb_lines)
        logger.error(f"详细错误信息:\n{tb_text}")
        return mol, None


def prepare_polymer(smiles, n_units, left_cap, right_cap):
    """准备聚合物分子"""
    # 生成聚合物分子
    polymer = generate_polymer(smiles, n_units, left_cap=left_cap, right_cap=right_cap)

    # 添加氢原子
    polymer = Chem.AddHs(polymer)

    # 生成初始构象
    try:
        # 尝试使用ETKDG方法生成构象
        params = AllChem.ETKDGv3()
        params.randomSeed = 42
        params.useBasicKnowledge = True
        params.useSmallRingTorsions = True
        AllChem.EmbedMolecule(polymer, params=params)
    except:
        # 如果失败，使用基本方法
        AllChem.EmbedMolecule(polymer)

    # 优化初始构象
    try:
        # 使用UFF进行初步优化
        AllChem.UFFOptimizeMolecule(polymer)

        # 尝试使用MMFF进行更精确优化
        try:
            AllChem.MMFFOptimizeMolecule(polymer)
        except:
            pass
    except:
        pass

    return polymer


def check_rdkit_molecule(mol):
    """检查RDKit分子是否有效"""
    if mol is None:
        raise ValueError("无效的分子对象")

    if not mol.GetNumAtoms():
        raise ValueError("分子中没有原子")

    # 检查是否有3D构象
    if not mol.GetNumConformers():
        logger.warning("分子没有3D构象，尝试生成...")
        AllChem.EmbedMolecule(mol)
        if not mol.GetNumConformers():
            raise ValueError("无法为分子生成3D构象")

    return True


def set_safe_working_directory():
    """设置安全的纯英文工作目录"""
    # 尝试在当前目录下创建md_temp目录
    safe_dir = os.path.join(os.getcwd(), "md_temp")
    os.makedirs(safe_dir, exist_ok=True)
    os.chdir(safe_dir)
    return safe_dir


def generate_forcefield_parameters(mol, forcefield_type='gaff-2.11'):
    """为自定义聚合物生成力场参数"""
    try:
        from openff.toolkit.topology import Molecule as OFFMolecule
        from openmmforcefields.generators import GAFFTemplateGenerator

        # 将RDKit分子转换为OpenFF分子
        off_mol = OFFMolecule.from_rdkit(mol)

        # 创建GAFF生成器
        generator = GAFFTemplateGenerator(molecules=off_mol, forcefield=forcefield_type)

        # 生成参数
        generator.generate_residue_template(off_mol)

        return generator
    except Exception as e:
        logger.error(f"生成力场参数失败: {str(e)}")
        return None
def main_openmm(left, repeat, right, n_units, output_folder, output_filename,steps=500):
    original_dir = os.getcwd()
    safe_dir = set_safe_working_directory()
    logger.info(f"工作目录设置为: {safe_dir}")

    try:
        # 准备聚合物分子
        polymer = prepare_polymer(repeat, n_units=n_units, left_cap=left, right_cap=right)

        # 检查分子有效性
        check_rdkit_molecule(polymer)

        # 为聚合物生成力场参数
        logger.info("为聚合物生成GAFF2力场参数...")
        generator = generate_forcefield_parameters(polymer)
        if generator is None:
            logger.warning("力场参数生成失败，尝试使用默认参数")


        # 确保输出目录存在
        os.makedirs(output_folder, exist_ok=True)
        temp_list  = [200, 250, 300, 350, 400]
        # 运行MD模拟并保存结果
        for element in temp_list:
            perturbed_mol, output_path = apply_md_perturbation_openmm(
                polymer,
                output_folder=output_folder,
                output_filename=output_filename,
                steps=steps,  # 增加步数以获得更好的结果
                temperature=element  # 提高温度以增强分子运动
            )
            print(f"MD模拟完成! 结果保存在 {output_path}")

        # 打印分子属性
        print(f"模拟参数: 步数={perturbed_mol.GetProp('MD_Steps')}, 温度={perturbed_mol.GetProp('MD_Temperature')}K")
        print(f"使用的力场: {perturbed_mol.GetProp('ForceField')}")

        # 将结果文件移动到原始目录
        # final_output_path = os.path.join(original_dir, os.path.basename(output_path))
        # shutil.move(output_path, final_output_path)
        # print(f"结果文件已移动到: {final_output_path}")

    except Exception as e:
        print(f"发生错误: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # 恢复原始工作目录
        os.chdir(original_dir)
        logger.info(f"恢复工作目录到: {original_dir}")

if __name__ == '__main__':
    # 设置安全的工作目录（纯英文路径）
    original_dir = os.getcwd()
    safe_dir = set_safe_working_directory()
    logger.info(f"工作目录设置为: {safe_dir}")

    nylon_smiles = "*O[C@H](CC)CC(=O)*"  # 己内酰胺单元
    left_cap = "[H]*"  # 乙二醇封端 (HO-CH2-CH2-O-)
    right_cap = "*O"  # 氢封端

    try:
        # 准备聚合物分子
        polymer = prepare_polymer(nylon_smiles, n_units=3, left_cap=left_cap, right_cap=right_cap)

        # 检查分子有效性
        check_rdkit_molecule(polymer)

        # 为聚合物生成力场参数
        logger.info("为聚合物生成GAFF2力场参数...")
        generator = generate_forcefield_parameters(polymer)
        if generator is None:
            logger.warning("力场参数生成失败，尝试使用默认参数")

        # 定义输出文件夹和文件名
        output_folder = "try"
        output_filename = "nylon_polymer_gaff2"

        # 确保输出目录存在
        os.makedirs(output_folder, exist_ok=True)

        # 运行MD模拟并保存结果
        perturbed_mol, output_path = apply_md_perturbation_openmm(
            polymer,
            output_folder=output_folder,
            output_filename=output_filename,
            steps=1000,  # 增加步数以获得更好的结果
            temperature=400  # 提高温度以增强分子运动
        )
        print(f"MD模拟完成! 结果保存在 {output_path}")

        # 打印分子属性
        print(f"模拟参数: 步数={perturbed_mol.GetProp('MD_Steps')}, 温度={perturbed_mol.GetProp('MD_Temperature')}K")
        print(f"使用的力场: {perturbed_mol.GetProp('ForceField')}")

        # 将结果文件移动到原始目录
        final_output_path = os.path.join(original_dir, os.path.basename(output_path))
        shutil.move(output_path, final_output_path)
        print(f"结果文件已移动到: {final_output_path}")

    except Exception as e:
        print(f"发生错误: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # 恢复原始工作目录
        os.chdir(original_dir)
        logger.info(f"恢复工作目录到: {original_dir}")
