"""SPM 本地演示：稳定段、单点尖峰、持续偏移与缺口。

在仓库根目录运行：

    python -m examples.spm_demo

演示完全使用合成读数，只输出统计意义上的异常提示（EWMA / CUSUM 控制图状态），
不输出任何故障诊断结论。流程：

1. 用已确认正常的基线区间冻结中心、尺度与双侧 EWMA / CUSUM 控制限；
2. 逐固定网格子组推进，展示每个有效子组、空网格/不足子组、排除理由；
3. 展示首次触发、持续、人工确认、恢复与再次触发各自对应的子组和累积统计量；
4. 校验离线一次处理与逐批处理在同一冻结输入下序列完全相同；
5. 补录更早读数后从最早受影响子组重放，保留旧版本并解释触发的延后/撤回，
   且同一告警身份不会收到第二遍通知。
"""
from __future__ import annotations

import json
import random
import tempfile
from pathlib import Path

from hangar_ingest.api import Application
from hangar_ingest.runs.database import Database
from hangar_ingest.spm.service import MonitoringService

GRID_SECONDS = 600          # 每个子组 10 分钟
ORIGIN = '2026-02-01T00:00:00Z'
BASELINE_BUCKETS = 12       # 00:00 - 02:00
CENTER = 10.0
BASELINE_SIGMA = 0.4


def stamp(bucket, point):
    minute = bucket * 10 + point * 3
    return f'2026-02-01T{minute // 60:02d}:{minute % 60:02d}:00Z'


def make_baseline():
    rng = random.Random(20260201)
    rows, event = [], 0
    for bucket in range(BASELINE_BUCKETS):
        for point in range(3):
            event += 1
            rows.append((f'base-{event}', stamp(bucket, point),
                         CENTER + rng.gauss(0, BASELINE_SIGMA)))
    return rows


def make_monitoring():
    """返回监测窗口读数与每段的人工说明（说明不参与任何计算）。"""
    rng = random.Random(99)
    rows, event = [], 0
    segments = []
    for bucket in range(12, 34):
        if bucket in (24, 25):
            segments.append((bucket, '空网格：该桶无任何可用读数'))
            continue
        for point in range(3):
            event += 1
            if bucket == 16 and point == 1:
                value = CENTER + 3.5            # 单点尖峰（同桶其余两点正常）
            elif 20 <= bucket <= 23:
                value = CENTER + 1.7 + rng.gauss(0, BASELINE_SIGMA)   # 持续上偏移
            elif 27 <= bucket <= 29:
                value = CENTER - 1.6 + rng.gauss(0, BASELINE_SIGMA)   # 持续下偏移
            else:
                value = CENTER + rng.gauss(0, BASELINE_SIGMA)         # 稳定段
            rows.append((f'mon-{event}', stamp(bucket, point), value))
    return rows, segments


def ingest(app, rows, batch_id):
    content = ''.join(
        json.dumps({'device': 'sensor1', 'event_id': eid,
                    'observed_at': when, 'unit': 'm/s2',
                    'value': f'{value:.6f}'}) + '\n'
        for eid, when, value in rows)
    app.call('batch.stage',
             {'batch_id': batch_id, 'source': 'feed1', 'content': content})
    app.call('batch.drain', {'batch_id': batch_id})


def plan_config():
    return {
        'plan_id': 'vibration-demo', 'aircraft': 'AC1', 'component': 'engine',
        'metric': 'vibration', 'grid_seconds': GRID_SECONDS, 'grid_origin': ORIGIN,
        'min_points_per_subgroup': 3, 'min_baseline_subgroups': 8,
        'reset_after_maintenance': True,
        'baseline_start': '2026-02-01T00:00:00Z',
        'baseline_end': '2026-02-01T02:00:00Z',
        'charts': {
            # 双侧 EWMA：lambda=0.25，控制限倍数 l=3（稳态约 ±1.13 sigma）。
            'ewma': {'lambda': 0.25, 'l': 3.0},
            # 双侧 CUSUM：参考偏移 k=0.5 sigma，决策区间 h=4 sigma。
            'cusum': {'k': 0.5, 'h': 4.0},
        },
    }


def _line(title):
    return f'\n=== {title} ' + '=' * max(0, 72 - len(title))


def render_frozen(plan):
    print(_line('1. 冻结的监测方案（中心/尺度/控制限不可变）'))
    print(f"方案 {plan['plan_id']}  机型={plan['aircraft']} 部件={plan['component']} "
          f"指标={plan['metric']}")
    print(f"网格={plan['grid_seconds']}s  每子组最少点数={plan['min_points_per_subgroup']} "
          f"最少基线子组={plan['min_baseline_subgroups']}  "
          f"维修后重置={plan['reset_after_maintenance']}")
    print(f"基线有效子组数={plan['baseline_subgroup_count']} "
          f"冻结中心 center={plan['center']:.4f}  组内尺度 sigma={plan['scale']:.4f}")
    for chart in plan['charts']:
        print(f"  - {chart['kind']}: 参数={json.dumps(chart['params'], ensure_ascii=False)}")
        print(f"    冻结控制限={json.dumps(chart['control_limits'], ensure_ascii=False)}")


def render_subgroups(result):
    print(_line('2. 每个网格子组：有效值 / 空网格 / 不足 / 排除理由'))
    for group in result['subgroups']:
        idx = group['bucket_index']
        if group['status'] == 'valid':
            flag = ' [不等间隔:跨越空网格]' if group['irregular_gap'] else ''
            print(f"桶 {idx:>2} 有效  n={len(group['values'])} 均值={group['mean']:.4f}"
                  f" 窗口 {group['window_start'][11:16]}-{group['window_end'][11:16]}{flag}")
        else:
            reasons = [item.get('reason') or ','.join(item.get('reasons', []))
                       for item in group['excluded']]
            print(f"桶 {idx:>2} {group['status']:<12} 不参与统计 排除/原因={reasons}")
    for item in result['nonevaluable']:
        if item['reason'] == 'empty':
            continue
        print(f"  非有效子组登记: 桶 {item['subgroup']['bucket_index']} -> {item['reason']}")


def render_evaluations(result):
    print(_line('3. 逐子组累积统计量与告警状态转换（首次/持续/恢复/再次）'))
    for evaluation in result['evaluations']:
        idx = evaluation['subgroup']['bucket_index']
        z = evaluation['standardized_mean']
        ewma = evaluation['chart_statistics']['ewma']
        cusum = evaluation['chart_statistics']['cusum']
        print(f"桶 {idx:>2} z={z:+.3f} | EWMA 上/下={ewma['upper']:+.3f}/"
              f"{ewma['lower']:+.3f} | CUSUM 上/下={cusum['upper']:+.3f}/"
              f"{cusum['lower']:+.3f} | 超限={json.dumps(evaluation['breaches'])}")
        for transition in evaluation['transitions']:
            print(f"      -> {transition['chart']}/{transition['side']} "
                  f"{transition['transition']} 状态={transition['state']} "
                  f"统计量={transition['statistic']:+.3f} 控制限="
                  f"{transition['control_limit']:+.3f} 依据子组={transition['bucket_index']}")
        for note in evaluation['notifications']:
            if note['decision'] != 'notify':
                continue
            print(f"      !! 通知: {note['alert_id']} ({note['transition']})")


def render_ack_and_report(svc):
    print(_line('4. 人工确认（不重置统计量）与当前告警快照'))
    report = svc.report('vibration-demo')
    upper = next((a for a in report['alert_history']
                  if a['chart'] == 'cusum' and a['side'] == 'upper'
                  and a['closed_at'] is None), None)
    if upper is not None:
        svc.acknowledge('vibration-demo', 'cusum', 'upper',
                        note='值班人员已知悉该统计异常提示，继续观察',
                        at='2026-02-01T04:00:00Z')
        print(f"已确认 {upper['alert_id']}，确认仅改标记，累积统计量保持不变。")
    report = svc.report('vibration-demo')
    print('当前未恢复告警:')
    for alert in report['active_alerts']:
        ack = ' [已确认]' if alert['acknowledged'] else ''
        print(f"  - {alert['alert_id']} 状态={alert['state']}{ack} "
              f"始于桶 {alert['opened_bucket_index']}")
    print(f"全部告警 episode 数={len(report['alert_history'])}；"
          f"已通知身份数={len(report['notified_alert_ids'])}（去重后）")


def assert_equivalence(db_rows, state_dir):
    print(_line('5. 确定性校验：离线一次处理 == 逐批处理（同一冻结输入）'))
    offline_dir = Path(state_dir) / 'offline'
    batched_dir = Path(state_dir) / 'batched'
    results = []
    for directory, batch_size in ((offline_dir, None), (batched_dir, 2)):
        db = Database(':memory:')
        app = Application(db)
        _provision(app)
        ingest(app, db_rows['baseline'], 'base')
        svc = MonitoringService(db, directory, clock=lambda: '2026-03-01T00:00:00Z')
        svc.define_plan(plan_config())
        ingest(app, db_rows['monitoring'], 'mon')
        results.append(svc.run(
            'vibration-demo', window_start='2026-02-01T02:00:00Z',
            window_end='2026-02-01T06:00:00Z', batch_size=batch_size))
        db.close()
    same = (
        json.dumps(results[0]['evaluations'], sort_keys=True)
        == json.dumps(results[1]['evaluations'], sort_keys=True)
        and json.dumps(results[0]['nonevaluable'], sort_keys=True)
        == json.dumps(results[1]['nonevaluable'], sort_keys=True)
    )
    print(f"离线评估数={len(results[0]['evaluations'])} "
          f"逐批评估数={len(results[1]['evaluations'])} 序列一致={same}")
    print(f"冻结输入摘要一致={results[0]['input_fingerprint'] == results[1]['input_fingerprint']}")
    if not same:
        raise AssertionError('offline and batched SPM sequences diverged')


def render_replay(app, svc, monitoring_rows):
    print(_line('6. 补录后重放：保留旧版本，解释延后/撤回且不重复通知'))
    # 向最早的上偏移桶 20 补入三个低值读数，削弱并延后上侧触发。
    backfill = [(f'backfill-{i}', stamp(20, i).replace(':00Z', ':05Z'), CENTER - 0.5)
                for i in range(3)]
    ingest(app, backfill, 'backfill')
    replay = svc.replay(
        'vibration-demo',
        window_start='2026-02-01T00:00:00Z', window_end='2026-02-01T06:00:00Z',
        label='backfill-bucket-20')
    print('版本差异（旧版本保留在版本日志，不被覆盖）:')
    for diff in replay['differences']:
        print(f"  - {diff['alert_id']}: {diff['outcome']} "
              f"桶 {diff.get('old_bucket_index')} -> {diff.get('new_bucket_index')} "
              f"（{diff['explanation']}）")
    notify = [n for n in replay['notifications'] if n['decision'] == 'notify']
    print(f"重放后实际通知条数={len(notify)}（同一身份不重复通知）；"
          f"决策集合={sorted({n['decision'] for n in replay['notifications']})}")
    versions = [line for line in Path(replay['version_log']).read_text().splitlines() if line]
    print(f"版本日志条目数={len(versions)}（含 before/after 两个完整快照）")


def render_calibration_withdrawal(app, svc):
    print(_line('7. 校准更正后重放：触发被撤回（旧版本保留，不重发通知）'))
    before_alert_ids = {a['alert_id'] for a in svc.report('vibration-demo')['alert_history']}
    # 校准更正（offset 平移），随后按现有校准重处理流程改写历史观测值。
    current = app.call('calibration.plan', {'calibration_id': 'c1'})
    next_revision = current['calibration_revision'] + 1
    app.call('calibration.correct', {
        'calibration_id': 'c1', 'revision': current['calibration_revision'],
        'scale': '1', 'offset': '-1.7', 'clock_offset_us': 0})
    app.call('calibration.apply',
             {'calibration_id': 'c1', 'expected_revision': next_revision})
    replay = svc.replay(
        'vibration-demo',
        window_start='2026-02-01T00:00:00Z', window_end='2026-02-01T06:00:00Z',
        label='calibration-correction')
    withdrawn = [d for d in replay['differences'] if d['outcome'] == 'withdrawn']
    print(f"重放前告警身份数={len(before_alert_ids)}；本次被撤回的触发数={len(withdrawn)}")
    for diff in withdrawn:
        print(f"  - 撤回 {diff['alert_id']}：原触发于桶 "
              f"{diff['old_bucket_index']}，{diff['explanation']}")
    duplicates = [n for n in replay['notifications'] if n['decision'] == 'notify']
    print(f"撤回/变更后通知条数={len(duplicates)}（撤回不产生新通知）；"
          f"决策集合={sorted({n['decision'] for n in replay['notifications']})}")
    if not withdrawn:
        raise AssertionError('expected at least one withdrawn trigger after correction')


def _provision(app):
    app.call('device.register', {
        'device_id': 'v1', 'aircraft': 'AC1', 'component': 'engine',
        'metric': 'vibration', 'canonical_unit': 'm/s2'})
    app.call('alias.add', {
        'source': 'feed1', 'external_id': 'sensor1', 'device_id': 'v1',
        'valid_from': '2026-01-01T00:00:00Z',
        'valid_until': '2027-01-01T00:00:00Z'})
    app.call('calibration.add', {
        'calibration_id': 'c1', 'device_id': 'v1',
        'valid_from': '2026-01-01T00:00:00Z',
        'valid_until': '2027-01-01T00:00:00Z'})
    app.call('quality.configure', {
        'device_id': 'v1', 'minimum': '-50', 'maximum': '50',
        'max_gap_seconds': 3600})


def run():
    state_root = tempfile.mkdtemp(prefix='hangar-spm-demo-')
    db = Database(':memory:')
    app = Application(db)
    _provision(app)
    baseline = make_baseline()
    monitoring, segments = make_monitoring()
    ingest(app, baseline, 'baseline')

    svc = MonitoringService(db, Path(state_root) / 'state',
                            clock=lambda: '2026-03-01T00:00:00Z')
    defined = svc.define_plan(plan_config())
    render_frozen(defined['plan'])

    ingest(app, monitoring, 'monitoring')
    result = svc.run(
        'vibration-demo', window_start='2026-02-01T02:00:00Z',
        window_end='2026-02-01T06:00:00Z')
    render_subgroups(result)
    print('\n场景说明（仅描述数据构造，不是诊断结论）:')
    for bucket, text in segments:
        print(f'  桶 {bucket:>2}: {text}')
    render_evaluations(result)
    render_ack_and_report(svc)

    assert_equivalence({'baseline': baseline, 'monitoring': monitoring},
                       Path(state_root) / 'equivalence')
    render_replay(app, svc, monitoring)
    render_calibration_withdrawal(app, svc)

    print('\n说明：以上全部为统计过程监测的异常提示，不构成任何故障诊断结论。')
    print(f'演示状态目录（可检查检查点与版本日志）: {state_root}')
    db.close()
    return {'ok': True, 'state_directory': state_root}


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False, sort_keys=True))
