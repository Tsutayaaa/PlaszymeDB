from generate_sdf import generate_polymer_smile

def pha_convert(left, right, repeat_unit, n_units):
    """
    pha家族转换
    :param
    left:左端基的smile格式化学式
    right:右端基的smile格式化学式
    repeat:重复单位的smile格式化学式（列表）
    n_units:重复单元的数量
    """
    repeat  = repeat_unit.split(',')
    final_smile = left.strip('*')
    for i in range(n_units):
        final_smile += repeat[i%len(repeat)].strip('*')
    final_smile += right.strip('*')
    mol = generate_polymer_smile(final_smile)
    return mol