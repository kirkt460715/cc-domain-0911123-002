"""SPM 数值工具。

只用 Python 标准库 ``math``，所有函数对相同输入逐位确定。统计量在输出前统一
按 ``SIGNIFICANT_DIGITS`` 位有效数字舍入，避免二进制浮点尾差导致同一输入在
不同批次/进程里产生不同的序列化结果（也让重放比较稳定）。
"""
from __future__ import annotations

import math

SIGNIFICANT_DIGITS = 12


def round_stat(value):
    """对输出级统计量做确定性舍入；非有限值保持原样交由上层报错。"""
    if value is None:
        return None
    if not math.isfinite(value):
        return value
    if value == 0.0:
        return 0.0
    magnitude = math.floor(math.log10(abs(value)))
    factor = 10.0 ** (SIGNIFICANT_DIGITS - 1 - magnitude)
    return round(value * factor) / factor


def mean(values):
    return sum(values) / len(values)


def sample_variance(values):
    """无偏样本方差（n-1 分母），要求至少两个点。"""
    n = len(values)
    if n < 2:
        raise ValueError('sample variance requires at least two values')
    average = mean(values)
    return sum((value - average) ** 2 for value in values) / (n - 1)


def pooled_within_sd(subgroup_values):
    """合并组内标准差。

    每个有效子组至少有 ``min_points`` 个点；自由度按各子组 (n_i-1) 加权，
    与子组大小无关，避免大子组主导尺度估计。
    """
    weighted = 0.0
    freedom = 0
    for values in subgroup_values:
        n = len(values)
        if n < 2:
            # 单点子组不贡献组内自由度；全部子组都是单点时由上层判定零尺度。
            continue
        weighted += sample_variance(values) * (n - 1)
        freedom += n - 1
    if freedom == 0:
        return 0.0
    return math.sqrt(weighted / freedom)


def is_zero_scale(scale):
    return scale == 0.0 or not math.isfinite(scale)
