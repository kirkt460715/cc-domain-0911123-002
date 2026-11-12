"""固定网格子组聚合。

输入是已经过现有接入管线（别名解析、校准、质量规则、维修窗口）处理的读数，
每条读数附带一个"可纳入判定"。本模块只做三件确定性的事：

1. 按方案固定网格把读数归入桶，计算每架飞机的有效子组（子组均值等）；
2. 对空网格、不足最小点数、跨越空桶的不等间隔给出明确状态，绝不插值；
3. 逐条记录排除理由，保证每个未进子组的读数都可解释。

网格桶定义为半开区间 ``[origin + k*g, origin + (k+1)*g)``，桶索引可负，
因此不要求读数晚于网格原点。
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field, asdict

from hangar_ingest.types import canonical, digest, fail, instant, micros, from_micros
from hangar_ingest.spm.models import SubgroupStatus, ExclusionCode
from hangar_ingest.spm import numeric


@dataclass(frozen=True)
class Reading:
    """进入 SPM 的一条读数（已通过现有管线，值为规范后的 float）。"""

    observation_id: str
    aircraft: str
    event_time: str          # 校准时钟修正后的事件时间
    value: float
    eligible: bool           # 现有规则是否允许纳入统计
    exclusion_codes: tuple   # tuple[str]，eligible=True 时为空
    maintenance_order_id: str | None = None
    calibration_id: str | None = None

    def to_dict(self):
        return asdict(self)


@dataclass
class Subgroup:
    subgroup_id: str         # 全局确定：plan/aircraft/网格索引
    aircraft: str
    bucket_index: int
    window_start: str
    window_end: str
    status: str
    values: list = field(default_factory=list)
    observation_ids: list = field(default_factory=list)
    mean: float | None = None
    within_sd: float | None = None
    # 与上一个有效子组之间缺失的网格桶数量；0 表示相邻。
    missing_buckets_before: int = 0
    # 有效子组但跨越了空网格（不等间隔）：照常参与统计，同时显式标注。
    irregular_gap: bool = False
    # 落在桶内但被排除的读数及理由（可解释性）。
    excluded: list = field(default_factory=list)

    def to_dict(self):
        data = asdict(self)
        data['mean'] = numeric.round_stat(self.mean)
        data['within_sd'] = numeric.round_stat(self.within_sd)
        return data


def bucket_index(event_time_us, origin_us, grid_seconds):
    # 整除向负无穷取整，负索引也有唯一定义。
    return (event_time_us - origin_us) // (grid_seconds * 1_000_000)


def fingerprint(readings):
    """冻结输入摘要：纳入的读数身份、时间与值，重放一致性检查的基础。"""
    payload = [
        {'id': r.observation_id, 't': r.event_time, 'v': r.value,
         'eligible': r.eligible, 'excluded': list(r.exclusion_codes)}
        for r in sorted(readings, key=lambda r: (r.event_time, r.observation_id))
    ]
    return digest(canonical(payload))


def build_subgroups(plan, readings, *, window_start=None, window_end=None):
    """把读数聚合成固定网格子组序列。

    ``window_start`` / ``window_end`` 限定要物化的网格范围（UTC ISO，半开）。
    缺省时覆盖读数实际跨度，保证每个空桶都被显式物化出来。
    """
    origin_us = micros(plan.grid_origin)
    grid_us = plan.grid_seconds * 1_000_000
    window_start = instant(window_start) if window_start else None
    window_end = instant(window_end) if window_end else None

    by_aircraft = {}
    outside = []
    for reading in readings:
        if reading.aircraft != plan.aircraft:
            fail('spm_aircraft_mismatch', 'Reading does not belong to the plan aircraft',
                 plan=plan.aircraft, reading=reading.observation_id)
        time_us = micros(reading.event_time)
        if window_start is not None and not (micros(window_start) <= time_us < micros(window_end)):
            outside.append({
                'observation_id': reading.observation_id,
                'reason': ExclusionCode.OUTSIDE_GRID.value,
                'event_time': reading.event_time,
            })
            continue
        index = bucket_index(time_us, origin_us, plan.grid_seconds)
        by_aircraft.setdefault(index, []).append(reading)

    if not by_aircraft:
        return [], outside

    lowest = min(by_aircraft)
    highest = max(by_aircraft)
    if window_start is not None:
        lowest = bucket_index(micros(window_start), origin_us, plan.grid_seconds)
        highest = bucket_index(micros(window_end) - 1, origin_us, plan.grid_seconds)

    subgroups = []
    last_valid_index = None
    for index in range(lowest, highest + 1):
        bucket_readings = sorted(by_aircraft.get(index, []),
                                 key=lambda r: (r.event_time, r.observation_id))
        start_us = origin_us + index * grid_us
        subgroup = Subgroup(
            subgroup_id=f'{plan.plan_id}/{plan.aircraft}/bucket/{index}',
            aircraft=plan.aircraft, bucket_index=index,
            window_start=from_micros(start_us), window_end=from_micros(start_us + grid_us),
            status=SubgroupStatus.EMPTY.value,
        )
        eligible_readings = []
        for reading in bucket_readings:
            if reading.eligible:
                eligible_readings.append(reading)
            else:
                subgroup.excluded.append({
                    'observation_id': reading.observation_id,
                    'event_time': reading.event_time,
                    'reasons': list(reading.exclusion_codes),
                })
        if not bucket_readings or not eligible_readings:
            subgroup.status = SubgroupStatus.EMPTY.value
        elif len(eligible_readings) < plan.min_points_per_subgroup:
            subgroup.status = SubgroupStatus.INSUFFICIENT.value
            subgroup.observation_ids = [r.observation_id for r in eligible_readings]
            subgroup.values = [r.value for r in eligible_readings]
            subgroup.excluded.append({
                'observation_ids': subgroup.observation_ids,
                'reason': 'fewer_than_min_points',
                'count': len(eligible_readings),
                'required': plan.min_points_per_subgroup,
            })
        else:
            subgroup.status = SubgroupStatus.VALID.value
            subgroup.observation_ids = [r.observation_id for r in eligible_readings]
            subgroup.values = [r.value for r in eligible_readings]
            subgroup.mean = numeric.mean(subgroup.values)
            subgroup.within_sd = (
                math.sqrt(numeric.sample_variance(subgroup.values))
                if len(subgroup.values) >= 2 else 0.0
            )
            if last_valid_index is not None:
                gap = index - last_valid_index - 1
                if gap > 0:
                    subgroup.missing_buckets_before = gap
                    subgroup.irregular_gap = True
            last_valid_index = index
        subgroups.append(subgroup)
    return subgroups, outside
