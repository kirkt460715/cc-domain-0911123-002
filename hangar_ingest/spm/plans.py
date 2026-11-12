"""监测方案：参数校验、基线冻结与持久化结构。

工程师选定机型（aircraft）、部件（component）、指标（metric）和已确认正常的
基线区间后，方案把固定网格、图表参数、控制限与最少基线子组数全部冻结。冻结后
的中心与尺度不可变；重新冻结必须新建方案，保证历史告警始终可按当时参数解释。
"""
from __future__ import annotations

import math
from dataclasses import dataclass, asdict

from hangar_ingest.types import PipelineError, fail, identity, instant, integer
from hangar_ingest.spm.models import ChartKind
from hangar_ingest.spm import numeric

# 参数允许范围（含端点），校验失败统一抛 PipelineError，不静默夹紧。
_LAMBDA_RANGE = (0.05, 1.0)
_EWMA_L_RANGE = (2.0, 5.0)
_K_RANGE = (0.25, 2.0)
_H_RANGE = (1.0, 10.0)
_GRID_SECONDS_RANGE = (1, 7 * 86400)
_MIN_POINTS_RANGE = (1, 10000)
_MIN_BASELINE_RANGE = (2, 100000)


def _bounded_float(value, name, low, high):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        fail('spm_invalid_parameter', 'Parameter must be a number', parameter=name)
    value = float(value)
    if not math.isfinite(value) or not low <= value <= high:
        fail('spm_invalid_parameter', 'Parameter outside accepted range',
             parameter=name, value=value, allowed=[low, high])
    return value


@dataclass(frozen=True)
class ChartSpec:
    kind: str
    # EWMA: lambda（平滑系数）、l（控制限倍数）
    # CUSUM: k（参考偏移，单位 sigma）、h（决策区间，单位 sigma）
    params: dict
    control_limits: dict          # 冻结后的数值控制限（含 sigma 单位表示）

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class Plan:
    plan_id: str
    aircraft: str
    component: str
    metric: str
    grid_seconds: int
    grid_origin: str              # 网格对齐起点（UTC ISO），桶为 [origin+k*g, origin+(k+1)*g)
    min_points_per_subgroup: int
    min_baseline_subgroups: int
    reset_after_maintenance: bool
    charts: tuple                 # tuple[ChartSpec, ...]
    baseline_start: str
    baseline_end: str
    # 冻结结果
    frozen: bool = False
    center: float | None = None
    scale: float | None = None
    baseline_subgroup_count: int | None = None
    baseline_subgroup_ids: tuple = ()
    frozen_at: str | None = None
    input_fingerprint: str | None = None  # 基线输入冻结摘要，基线输入变化可被发现

    def to_dict(self):
        data = asdict(self)
        data['charts'] = [chart.to_dict() for chart in self.charts]
        data['baseline_subgroup_ids'] = list(self.baseline_subgroup_ids)
        return data

    def chart(self, kind):
        for spec in self.charts:
            if spec.kind == kind:
                return spec
        fail('spm_unknown_chart', 'Plan does not contain requested chart', kind=kind)


def _validate_chart(spec):
    if not isinstance(spec, dict):
        fail('spm_invalid_chart', 'Chart specification must be an object')
    kind = spec.get('kind')
    if kind not in (ChartKind.EWMA.value, ChartKind.CUSUM.value):
        fail('spm_invalid_chart', 'Unsupported chart kind', kind=kind)
    params = spec.get('params', {})
    if not isinstance(params, dict):
        fail('spm_invalid_chart', 'Chart params must be an object')
    if kind == ChartKind.EWMA.value:
        required = {'lambda', 'l'}
        if set(params) != required:
            fail('spm_invalid_chart', 'EWMA requires exactly lambda and l', found=sorted(params))
        lam = _bounded_float(params['lambda'], 'lambda', *_LAMBDA_RANGE)
        limit = _bounded_float(params['l'], 'l', *_EWMA_L_RANGE)
        return kind, {'lambda': lam, 'l': limit}
    required = {'k', 'h'}
    if set(params) != required:
        fail('spm_invalid_chart', 'CUSUM requires exactly k and h', found=sorted(params))
    k = _bounded_float(params['k'], 'k', *_K_RANGE)
    h = _bounded_float(params['h'], 'h', *_H_RANGE)
    return kind, {'k': k, 'h': h}


def build_plan(config):
    """校验用户配置并构造未冻结的方案。``config`` 为普通 dict（来自 API/JSON）。"""
    if not isinstance(config, dict):
        fail('spm_invalid_plan', 'Plan configuration must be an object')
    plan_id = identity(config['plan_id'], 'plan_id')
    aircraft = identity(config['aircraft'], 'aircraft')
    component = identity(config['component'], 'component')
    metric = identity(config['metric'], 'metric')
    grid_seconds = integer(config['grid_seconds'], 'grid_seconds', *_GRID_SECONDS_RANGE)
    grid_origin = instant(config['grid_origin'])
    min_points = integer(config['min_points_per_subgroup'], 'min_points_per_subgroup',
                         *_MIN_POINTS_RANGE)
    min_baseline = integer(config['min_baseline_subgroups'], 'min_baseline_subgroups',
                           *_MIN_BASELINE_RANGE)
    if not isinstance(config.get('reset_after_maintenance'), bool):
        fail('spm_invalid_plan', 'reset_after_maintenance must be boolean')
    baseline_start = instant(config['baseline_start'])
    baseline_end = instant(config['baseline_end'])
    if baseline_start >= baseline_end:
        fail('spm_invalid_plan', 'Baseline interval must be nonempty')
    chart_inputs = config.get('charts')
    if not isinstance(chart_inputs, dict) or not chart_inputs:
        fail('spm_invalid_plan', 'At least one chart must be configured')
    specs = []
    seen = set()
    for kind, raw in chart_inputs.items():
        validated_kind, params = _validate_chart({'kind': kind, 'params': raw})
        if validated_kind in seen:
            fail('spm_invalid_plan', 'Duplicate chart kind', kind=validated_kind)
        seen.add(validated_kind)
        specs.append((validated_kind, params))
    # 控制限依赖尺度，冻结前只保留参数；顺序固定保证序列化确定。
    charts = tuple(ChartSpec(kind, params, {}) for kind, params in sorted(specs))
    return Plan(
        plan_id=plan_id, aircraft=aircraft, component=component, metric=metric,
        grid_seconds=grid_seconds, grid_origin=grid_origin,
        min_points_per_subgroup=min_points, min_baseline_subgroups=min_baseline,
        reset_after_maintenance=config['reset_after_maintenance'],
        charts=charts, baseline_start=baseline_start, baseline_end=baseline_end,
    )


def _control_limits(kind, params, scale):
    """按冻结尺度计算数值控制限（标准正态稳态口径）。"""
    if kind == ChartKind.EWMA.value:
        lam = params['lambda']
        # 稳态 EWMA 方差因子 sqrt(lambda/(2-lambda))，控制限 = ±l * sigma * 因子。
        factor = math.sqrt(lam / (2.0 - lam))
        half_width = params['l'] * scale * factor
        return {
            'center_offset_upper': numeric.round_stat(half_width),
            'center_offset_lower': numeric.round_stat(-half_width),
            'formula': 'l * sigma * sqrt(lambda/(2-lambda))',
        }
    # CUSUM 决策区间 h 以 sigma 为单位，参考偏移 k 同理。
    return {
        'h_distance': numeric.round_stat(params['h'] * scale),
        'k_reference': numeric.round_stat(params['k'] * scale),
        'formula': 'decision interval h*sigma, reference k*sigma',
    }


def freeze(plan, baseline_subgroups, *, at, input_fingerprint):
    """用已确认正常的基线有效子组冻结中心与尺度。

    ``baseline_subgroups`` 只接受状态为 VALID 的子组（由 grid 模块产出）。
    零尺度、子组数不足都是显式失败，绝不产生一个"看似可用"的方案。
    """
    if plan.frozen:
        fail('spm_plan_frozen', 'Plan is already frozen; create a new plan to refit')
    valid = [group for group in baseline_subgroups if group.status == 'valid']
    if len(valid) < plan.min_baseline_subgroups:
        raise PipelineError(
            'spm_baseline_insufficient',
            'Fewer valid baseline subgroups than the configured minimum',
            valid_subgroups=len(valid),
            required=plan.min_baseline_subgroups,
        )
    all_values = [value for group in valid for value in group.values]
    center = numeric.mean(all_values)
    scale = numeric.pooled_within_sd([group.values for group in valid])
    if numeric.is_zero_scale(scale):
        raise PipelineError(
            'spm_zero_scale',
            'Baseline has zero within-subgroup scale; monitoring cannot be frozen',
            plan_id=plan.plan_id,
            hint='widen the baseline or check the metric for a constant signal',
        )
    charts = tuple(
        ChartSpec(spec.kind, spec.params, _control_limits(spec.kind, spec.params, scale))
        for spec in plan.charts
    )
    return Plan(
        plan_id=plan.plan_id, aircraft=plan.aircraft, component=plan.component,
        metric=plan.metric, grid_seconds=plan.grid_seconds, grid_origin=plan.grid_origin,
        min_points_per_subgroup=plan.min_points_per_subgroup,
        min_baseline_subgroups=plan.min_baseline_subgroups,
        reset_after_maintenance=plan.reset_after_maintenance, charts=charts,
        baseline_start=plan.baseline_start, baseline_end=plan.baseline_end,
        frozen=True, center=numeric.round_stat(center), scale=numeric.round_stat(scale),
        baseline_subgroup_count=len(valid),
        baseline_subgroup_ids=tuple(group.subgroup_id for group in valid),
        frozen_at=instant(at), input_fingerprint=input_fingerprint,
    )


def from_dict(data):
    """从持久化 JSON 重建方案（frozen dataclass）。"""
    charts = tuple(
        ChartSpec(kind=item['kind'], params=dict(item['params']),
                  control_limits=dict(item['control_limits']))
        for item in data['charts']
    )
    unknown = set(data) - set(Plan.__dataclass_fields__)
    if unknown:
        fail('spm_invalid_plan', 'Stored plan contains unknown fields', unknown=sorted(unknown))
    return Plan(
        plan_id=data['plan_id'], aircraft=data['aircraft'], component=data['component'],
        metric=data['metric'], grid_seconds=data['grid_seconds'],
        grid_origin=data['grid_origin'],
        min_points_per_subgroup=data['min_points_per_subgroup'],
        min_baseline_subgroups=data['min_baseline_subgroups'],
        reset_after_maintenance=data['reset_after_maintenance'], charts=charts,
        baseline_start=data['baseline_start'], baseline_end=data['baseline_end'],
        frozen=data['frozen'], center=data['center'], scale=data['scale'],
        baseline_subgroup_count=data['baseline_subgroup_count'],
        baseline_subgroup_ids=tuple(data['baseline_subgroup_ids']),
        frozen_at=data['frozen_at'], input_fingerprint=data['input_fingerprint'],
    )
