"""复用现有业务规则的纳入口判定。

维修窗口、别名有效期、校准（缺失/过期修订）与质量排除全部沿用现有规则，本模块
不重新定义阈值，只把现有表与规则的判定结果折叠成 SPM 的 ``Reading.eligible`` 与
排除理由。别名在接入时已经解析，这里做的是有效期复核；质量排除分两级：

* 致命质量错误（超 min/max、邻接 delta）的读数根本不会进入 observations 表
  （在隔离区），因此 SPM 看不到；仍在表内但带 quality 警告的读数默认排除，
  可由方案显式选择纳入警告级读数。
"""
from __future__ import annotations

import json

from hangar_ingest.types import micros
from hangar_ingest.spm.models import ExclusionCode
from hangar_ingest.spm.grid import Reading


def maintenance_order_at(db, aircraft, component, event_time):
    """返回覆盖该时刻的最小维修工单（沿用 windows.match_orders 的排序口径）。"""
    rows = db.all(
        "SELECT order_id,opened_at,closed_at FROM work_orders WHERE aircraft=? AND component=? "
        "AND state!='cancelled' AND opened_at<=? AND closed_at>? ORDER BY opened_at,order_id",
        (aircraft, component, event_time, event_time),
    )
    if not rows:
        return None
    ranked = sorted(rows, key=lambda r: (micros(r['closed_at']) - micros(r['opened_at']),
                                         r['order_id']))
    return ranked[0]['order_id']


def alias_valid_at(db, source, external_id, observed_at):
    """复核别名在观测时刻是否恰好有效（沿用 alias.resolve 的唯一性口径）。"""
    rows = db.all(
        'SELECT 1 FROM aliases WHERE source=? AND external_id=? AND valid_from<=? AND valid_until>?',
        (source, external_id, observed_at, observed_at),
    )
    return len(rows) == 1


def classify_observation(db, row, device, *, plan, source, external_id, include_warnings):
    """把一条 observations 行分类为 SPM Reading。

    判定顺序固定，排除理由按优先级全部记录（可解释性要求"每个排除都有理由"）。
    """
    reasons = []
    order_id = maintenance_order_at(db, device['aircraft'], device['component'], row['event_time'])
    if order_id is not None:
        reasons.append(ExclusionCode.MAINTENANCE_WINDOW.value)

    if not alias_valid_at(db, source, external_id, row['raw_time']):
        reasons.append(ExclusionCode.ALIAS_NOT_VALID.value)

    if row['calibration_id'] is None:
        if _required_calibration(db, device['device_id']):
            reasons.append(ExclusionCode.CALIBRATION_MISSING.value)
    else:
        calibration = db.one('SELECT revision FROM calibrations WHERE calibration_id=?',
                             (row['calibration_id'],))
        if calibration is None or calibration['revision'] != row['calibration_revision']:
            reasons.append(ExclusionCode.CALIBRATION_STALE.value)

    warnings = json.loads(row['quality']) if isinstance(row['quality'], str) else row['quality']
    if warnings:
        reasons.append(ExclusionCode.QUALITY_WARNING.value)

    eligible = not reasons or (
        include_warnings
        and reasons == [ExclusionCode.QUALITY_WARNING.value]
        and order_id is None
    )
    # 维修窗口内的读数即便只有警告也不纳入；上面的 OR 已保证 order_id 为空。
    return Reading(
        observation_id=row['observation_id'], aircraft=device['aircraft'],
        event_time=row['event_time'], value=float(row['value']),
        eligible=eligible, exclusion_codes=tuple(reasons),
        maintenance_order_id=order_id, calibration_id=row['calibration_id'],
    )


def _required_calibration(db, device_id):
    rule = db.one('SELECT required_calibration FROM quality_rules WHERE device_id=?',
                  (device_id,))
    return bool(rule['required_calibration']) if rule else True


def load_readings(db, plan, *, source, external_id, include_warnings=False,
                  window_start=None, window_end=None):
    """从现有 observations 表加载读数并完成纳入口判定。

    隔离区（致命质量错误）读数不在 observations 中，按现有规则天然排除，
    调用方可通过 quarantine 清单单独向报告补充该排除理由。
    """
    device = db.one(
        'SELECT * FROM devices WHERE aircraft=? AND component=? AND metric=? ORDER BY device_id',
        (plan.aircraft, plan.component, plan.metric),
    )
    if device is None:
        return [], None
    clauses = ['device_id=?']
    args = [device['device_id']]
    if window_start is not None:
        clauses.append('event_time>=?')
        args.append(window_start)
    if window_end is not None:
        clauses.append('event_time<?')
        args.append(window_end)
    rows = db.all(
        f'SELECT * FROM observations WHERE {" AND ".join(clauses)} '
        'ORDER BY event_time,observation_id', args,
    )
    readings = [
        classify_observation(
            db, row, device, plan=plan, source=source, external_id=external_id,
            include_warnings=include_warnings,
        )
        for row in rows
    ]
    return readings, device
