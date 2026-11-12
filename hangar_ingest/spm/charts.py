"""双侧 EWMA 与 CUSUM 控制图（确定性纯函数）。

输入统一为标准化子组均值 ``z = (x_bar - center) / scale``，因此两张图的参数都
以 sigma 为单位，控制限在方案冻结时一并冻结。每个更新函数返回完整新状态，不修改
入参；状态可直接 JSON 序列化作为检查点。

* EWMA：``e_t = lambda*z_t + (1-lambda)*e_{t-1}``，稳态控制限
  ``±l*sqrt(lambda/(2-lambda))``（方案冻结时换算为实际量纲）。
* CUSUM（双侧 tabular）：
  ``C+_t = max(0, z_t - k + C+_{t-1})``，
  ``C-_t = max(0, -k - z_t + C-_{t-1})``，
  决策区间为 ``h``（sigma 单位）。
"""
from __future__ import annotations

import math

from hangar_ingest.spm.models import ChartKind, Side
from hangar_ingest.spm import numeric


def standardized(mean, plan):
    return (mean - plan.center) / plan.scale


def initial_state(kind):
    if kind == ChartKind.EWMA.value:
        return {'kind': kind, 'ewma': 0.0}
    if kind == ChartKind.CUSUM.value:
        return {'kind': kind, 'cusum_upper': 0.0, 'cusum_lower': 0.0}
    raise ValueError(f'unknown chart kind {kind}')


def update(kind, state, z, params):
    """返回 (新状态, 各方向统计量, 超限方向集合)。统计量已做输出舍入。"""
    if kind == ChartKind.EWMA.value:
        lam = params['lambda']
        ewma = lam * z + (1.0 - lam) * state['ewma']
        new_state = {'kind': kind, 'ewma': ewma}
        factor = math.sqrt(lam / (2.0 - lam))
        limit = params['l'] * factor
        statistics = {
            Side.UPPER.value: numeric.round_stat(ewma),
            Side.LOWER.value: numeric.round_stat(-ewma),
        }
        breaches = set()
        if ewma > limit:
            breaches.add(Side.UPPER.value)
        if ewma < -limit:
            breaches.add(Side.LOWER.value)
        return new_state, statistics, breaches

    if kind == ChartKind.CUSUM.value:
        k = params['k']
        upper = max(0.0, z - k + state['cusum_upper'])
        lower = max(0.0, -k - z + state['cusum_lower'])
        new_state = {'kind': kind, 'cusum_upper': upper, 'cusum_lower': lower}
        h = params['h']
        statistics = {
            Side.UPPER.value: numeric.round_stat(upper),
            Side.LOWER.value: numeric.round_stat(lower),
        }
        breaches = set()
        if upper > h:
            breaches.add(Side.UPPER.value)
        if lower > h:
            breaches.add(Side.LOWER.value)
        return new_state, statistics, breaches

    raise ValueError(f'unknown chart kind {kind}')


def reset_state(state):
    """维修后重置（仅当方案 reset_after_maintenance=True 时调用）。"""
    return initial_state(state['kind'])
