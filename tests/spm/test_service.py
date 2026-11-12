"""SPM 服务级测试：对接真实 SQLite 与现有接入规则。"""
import json
import os
import random
import tempfile
import unittest
from pathlib import Path

from hangar_ingest.api import Application
from hangar_ingest.runs.database import Database
from hangar_ingest.types import PipelineError
from hangar_ingest.spm.service import MonitoringService

GRID = 600  # 10 分钟一个子组
ORIGIN = '2026-02-01T00:00:00Z'


def ts(bucket, point=0):
    minute = bucket * 10 + point * 3
    return f'2026-02-01T{minute // 60:02d}:{minute % 60:02d}:00Z'


class SPMSupport(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.dir.name) / 'spm-state'
        self.clock_value = '2026-03-01T00:00:00Z'
        self.db = Database(':memory:', clock=lambda: self.clock_value)
        self.app = Application(self.db)
        self.app.call('device.register', {
            'device_id': 'v1', 'aircraft': 'AC1', 'component': 'engine',
            'metric': 'vibration', 'canonical_unit': 'm/s2'})
        self.app.call('alias.add', {
            'source': 'feed1', 'external_id': 'sensor1', 'device_id': 'v1',
            'valid_from': '2026-01-01T00:00:00Z',
            'valid_until': '2027-01-01T00:00:00Z'})
        self.app.call('calibration.add', {
            'calibration_id': 'c1', 'device_id': 'v1',
            'valid_from': '2026-01-01T00:00:00Z',
            'valid_until': '2027-01-01T00:00:00Z'})
        self.app.call('quality.configure', {
            'device_id': 'v1', 'minimum': '-50', 'maximum': '50',
            'max_gap_seconds': 3600})

    def tearDown(self):
        self.db.close()
        self.dir.cleanup()

    def service(self):
        return MonitoringService(self.db, self.state_dir, clock=lambda: self.clock_value)

    def ingest(self, rows, batch_id):
        lines = ''.join(
            '{"device":"sensor1","event_id":"%s","observed_at":"%s","unit":"m/s2","value":"%.6f"}\n'
            % (eid, when, value) for eid, when, value in rows)
        self.app.call('batch.stage',
                      {'batch_id': batch_id, 'source': 'feed1', 'content': lines})
        self.app.call('batch.drain', {'batch_id': batch_id})

    def plan_config(self, *, reset=True, min_baseline=6):
        return {
            'plan_id': 'plan1', 'aircraft': 'AC1', 'component': 'engine',
            'metric': 'vibration', 'grid_seconds': GRID, 'grid_origin': ORIGIN,
            'min_points_per_subgroup': 3, 'min_baseline_subgroups': min_baseline,
            'reset_after_maintenance': reset,
            'baseline_start': '2026-02-01T00:00:00Z',
            'baseline_end': '2026-02-01T02:00:00Z',
            'charts': {'ewma': {'lambda': 0.25, 'l': 3.0},
                       'cusum': {'k': 0.5, 'h': 4.0}},
        }

    def baseline_rows(self, n_buckets=12, seed=1):
        rng = random.Random(seed)
        rows = []
        eid = 0
        for b in range(n_buckets):
            for p in range(3):
                eid += 1
                rows.append((f'e{eid}', ts(b, p), 10.0 + rng.gauss(0, 0.4)))
        return rows

    def define(self, **kwargs):
        svc = self.service()
        return svc, svc.define_plan(self.plan_config(**kwargs))


class ExclusionTests(SPMSupport):
    def test_maintenance_window_readings_are_excluded_with_reason(self):
        self.ingest(self.baseline_rows(), 'base')
        svc, _ = self.define()
        # 维修工单覆盖桶 14。
        self.app.call('order.upsert', {'value': {
            'order_id': 'wo1', 'revision': 1, 'aircraft': 'AC1', 'component': 'engine',
            'opened_at': ts(14), 'closed_at': ts(15),
            'issue': 'scheduled', 'resolution': 'done', 'state': 'closed'}})
        rows = [(f'm{i}', ts(14, i), 10.0) for i in range(3)]
        rows += [(f'n{i}', ts(15, i), 10.1) for i in range(3)]
        self.ingest(rows, 'maint')
        result = svc.run('plan1', window_start=ts(12), window_end=ts(16))
        bucket14 = next(g for g in result['subgroups'] if g['bucket_index'] == 14)
        self.assertEqual(bucket14['status'], 'empty')
        self.assertEqual(bucket14['excluded'][0]['reasons'], ['maintenance_window'])

    def test_alias_expired_at_ingestion_is_quarantined_by_existing_rule(self):
        # 别名有效期继续沿用现有接入规则：失效时读数在接入阶段即进入隔离区，
        # SPM 不会把它当成正常值（表内根本不存在该读数）。
        self.ingest(self.baseline_rows(), 'base')
        self.db.execute(
            "UPDATE aliases SET valid_until=? WHERE external_id='sensor1'",
            ('2026-02-01T02:00:00Z',))
        self.ingest([(f'x{i}', ts(13, i), 10.0) for i in range(3)], 'late')
        quarantine = self.db.all(
            "SELECT json_extract(reasons,'$.code') AS code FROM quarantine")
        self.assertTrue(all(row['code'] == 'alias_unresolved' for row in quarantine))
        in_window = self.db.scalar(
            'SELECT COUNT(*) FROM observations WHERE event_time>=? AND event_time<?',
            (ts(13), ts(14)))
        self.assertEqual(in_window, 0)

    def test_stale_calibration_is_flagged_not_silently_accepted(self):
        self.ingest(self.baseline_rows(), 'base')
        svc, _ = self.define()
        self.ingest([(f'x{i}', ts(13, i), 10.0) for i in range(3)], 'late')
        # 校准被更正：历史观测的 calibration_revision 落后于当前修订。
        self.app.call('calibration.correct', {
            'calibration_id': 'c1', 'revision': 1,
            'scale': '1', 'offset': '0.1', 'clock_offset_us': 0})
        result = svc.run('plan1', window_start=ts(12), window_end=ts(15))
        bucket = next(g for g in result['subgroups'] if g['bucket_index'] == 13)
        self.assertTrue(
            any('calibration_stale' in item['reasons'] for item in bucket['excluded']))

    def test_quality_rejected_observation_never_enters_monitoring(self):
        self.ingest(self.baseline_rows(), 'base')
        svc, _ = self.define()
        # 超过质量规则 max=50 的读数进入隔离区，不进入 observations，SPM 看不到。
        self.ingest([('bad0', ts(13, 0), 99.0),
                     ('ok1', ts(13, 1), 10.0),
                     ('ok2', ts(13, 2), 10.1)], 'bad')
        result = svc.run('plan1', window_start=ts(12), window_end=ts(14))
        bucket = next(g for g in result['subgroups'] if g['bucket_index'] == 13)
        self.assertEqual(bucket['status'], 'insufficient')
        quarantine = self.db.all("SELECT status FROM quarantine")
        self.assertEqual(len(quarantine), 1)


class MonitoringBehaviorTests(SPMSupport):
    def _scenario_rows(self):
        """稳定段、单点尖峰、持续偏移、缺口的组合数据。"""
        rng = random.Random(7)
        rows, eid = [], 0
        for b in range(12, 30):
            for p in range(3):
                eid += 1
                if b in (24, 25):
                    continue  # 两个空网格
                if b == 16 and p == 1:
                    value = 13.5  # 单点尖峰（同桶其余点正常）
                elif 20 <= b <= 23 or 26 <= b <= 28:
                    value = 11.6 + rng.gauss(0, 0.4)  # 持续偏移
                else:
                    value = 10.0 + rng.gauss(0, 0.4)
                rows.append((f's{eid}', ts(b, p), value))
        return rows

    def test_single_point_spike_does_not_alert_persistent_shift_does(self):
        self.ingest(self.baseline_rows(), 'base')
        svc, _ = self.define()
        self.ingest(self._scenario_rows(), 'mon')
        result = svc.run('plan1', window_start=ts(12), window_end=ts(30))
        triggered = {
            (t['chart'], t['side']): t['bucket_index']
            for ev in result['evaluations']
            for t in ev['transitions'] if t['transition'] == 'first_trigger'}
        # 尖峰桶 16 不触发；持续偏移在桶 20 之后触发。
        self.assertNotIn(16, triggered.values())
        self.assertGreaterEqual(min(triggered.values()), 20)
        # 空桶显式存在，且恢复后的首个有效桶带不等间隔标志。
        empties = {n['subgroup']['bucket_index']
                   for n in result['nonevaluable'] if n['reason'] == 'empty'}
        self.assertIn(24, empties)
        self.assertIn(25, empties)
        gap_after = next(ev for ev in result['evaluations']
                         if ev['subgroup']['bucket_index'] == 26)
        self.assertTrue(gap_after['irregular_gap'])

    def test_acknowledgment_does_not_reset_statistics(self):
        self.ingest(self.baseline_rows(), 'base')
        svc, _ = self.define()
        self.ingest(self._scenario_rows(), 'mon')
        svc.run('plan1', window_start=ts(12), window_end=ts(30))
        before = svc.report('plan1')
        statistic_before = before['alert_history'][0]['events'][0]['statistic']
        acked = svc.acknowledge('plan1', 'cusum', 'upper', note='值班确认')
        self.assertTrue(acked['acknowledged'])
        after = svc.report('plan1')
        # 确认后累积统计量原值保留，没有任何 episode 被关闭或重置。
        self.assertEqual(
            after['alert_history'][0]['events'][0]['statistic'], statistic_before)
        active_ids = {a['alert_id'] for a in after['active_alerts']}
        self.assertIn(acked['alert_id'], active_ids)

    def test_maintenance_reset_governed_by_plan(self):
        self.ingest(self.baseline_rows(), 'base')
        # reset_after_maintenance=True：维修窗口结束桶触发重置。
        svc, _ = self.define(reset=True)
        self.app.call('order.upsert', {'value': {
            'order_id': 'wo1', 'revision': 1, 'aircraft': 'AC1', 'component': 'engine',
            'opened_at': ts(18), 'closed_at': ts(19),
            'issue': 'scheduled', 'resolution': 'done', 'state': 'closed'}})
        self.ingest(self._scenario_rows(), 'mon')
        result = svc.run('plan1', window_start=ts(12), window_end=ts(30))
        reset_evals = [ev for ev in result['evaluations'] if ev['maintenance_reset']]
        self.assertTrue(reset_evals)
        self.assertEqual(reset_evals[0]['subgroup']['bucket_index'], 19)

    def test_no_maintenance_reset_when_plan_disables_it(self):
        self.ingest(self.baseline_rows(), 'base')
        svc, _ = self.define(reset=False)
        self.app.call('order.upsert', {'value': {
            'order_id': 'wo1', 'revision': 1, 'aircraft': 'AC1', 'component': 'engine',
            'opened_at': ts(18), 'closed_at': ts(19),
            'issue': 'scheduled', 'resolution': 'done', 'state': 'closed'}})
        self.ingest(self._scenario_rows(), 'mon')
        result = svc.run('plan1', window_start=ts(12), window_end=ts(30))
        self.assertFalse(any(ev['maintenance_reset'] for ev in result['evaluations']))


class DeterminismAndReplayTests(SPMSupport):
    def _build(self, state_dir):
        state_dir = Path(state_dir)
        state_dir.mkdir(parents=True, exist_ok=True)
        db = Database(state_dir / 'db.sqlite3', clock=lambda: self.clock_value)
        app = Application(db)
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

        def ingest(rows, batch_id):
            lines = ''.join(
                '{"device":"sensor1","event_id":"%s","observed_at":"%s","unit":"m/s2","value":"%.6f"}\n'
                % (eid, when, value) for eid, when, value in rows)
            app.call('batch.stage',
                     {'batch_id': batch_id, 'source': 'feed1', 'content': lines})
            app.call('batch.drain', {'batch_id': batch_id})

        rng = random.Random(11)
        base, eid = [], 0
        for b in range(12):
            for p in range(3):
                eid += 1
                base.append((f'e{eid}', ts(b, p), 10.0 + rng.gauss(0, 0.4)))
        mon = []
        for b in range(12, 28):
            for p in range(3):
                eid += 1
                shift = 1.6 if 20 <= b <= 25 else 0.0
                mon.append((f'e{eid}', ts(b, p), 10.0 + shift + rng.gauss(0, 0.4)))
        ingest(base, 'base')
        svc = MonitoringService(db, Path(state_dir) / 'spm',
                                clock=lambda: self.clock_value)
        svc.define_plan({
            'plan_id': 'plan1', 'aircraft': 'AC1', 'component': 'engine',
            'metric': 'vibration', 'grid_seconds': GRID, 'grid_origin': ORIGIN,
            'min_points_per_subgroup': 3, 'min_baseline_subgroups': 6,
            'reset_after_maintenance': True,
            'baseline_start': '2026-02-01T00:00:00Z',
            'baseline_end': '2026-02-01T02:00:00Z',
            'charts': {'ewma': {'lambda': 0.25, 'l': 3.0},
                       'cusum': {'k': 0.5, 'h': 4.0}}})
        ingest(mon, 'mon')
        return db, svc

    def test_offline_and_batched_produce_identical_sequences(self):
        offline_dir = Path(self.dir.name) / 'offline'
        batched_dir = Path(self.dir.name) / 'batched'
        _, svc_offline = self._build(offline_dir)
        db_batched, svc_batched = self._build(batched_dir)
        window = {'window_start': ts(12), 'window_end': ts(28)}
        r1 = svc_offline.run('plan1', **window)
        r2 = svc_batched.run('plan1', batch_size=3, **window)
        self.assertEqual(
            json.dumps(r1['evaluations'], sort_keys=True),
            json.dumps(r2['evaluations'], sort_keys=True))
        self.assertEqual(
            json.dumps(r1['nonevaluable'], sort_keys=True),
            json.dumps(r2['nonevaluable'], sort_keys=True))
        self.assertEqual(r1['input_fingerprint'], r2['input_fingerprint'])
        db_batched.close()

    def test_restart_resumes_from_checkpoint_without_duplicate_notification(self):
        run_dir = Path(self.dir.name) / 'restart'
        db, svc = self._build(run_dir)
        svc.run('plan1', window_start=ts(12), window_end=ts(20), batch_size=2)
        notified = set(svc.report('plan1')['notified_alert_ids'])
        db.close()
        # 新进程：重新打开同一个数据库与状态目录，接着处理剩余窗口。
        db2 = Database(run_dir / 'db.sqlite3', clock=lambda: self.clock_value)
        svc2 = MonitoringService(db2, run_dir / 'spm',
                                 clock=lambda: self.clock_value)
        result = svc2.run('plan1', window_start=ts(12), window_end=ts(28), batch_size=2)
        report = svc2.report('plan1')
        # 旧身份不重复通知，通知集合只增不改。
        self.assertTrue(notified.issubset(set(report['notified_alert_ids'])))
        duplicate = [n for ev in result['evaluations'] for n in ev['notifications']
                     if n['decision'] == 'notify' and n['alert_id'] in notified]
        self.assertFalse(duplicate)
        db2.close()

    def test_replay_explains_withdrawal_delay_and_keeps_versions(self):
        run_dir = Path(self.dir.name) / 'replay'
        db, svc = self._build(run_dir)
        svc.run('plan1', window_start=ts(12), window_end=ts(28))
        # 先确认一个告警，验证确认标记跨重放保留。
        svc.acknowledge('plan1', 'cusum', 'upper', note='人工确认')
        # 补录：向最早受影响的偏移桶 20 补入低值读数，削弱并延后触发。
        backfill = [(f'bf{i}', ts(20, i).replace(':00Z', ':05Z'), 8.8) for i in range(3)]
        lines = ''.join(
            '{"device":"sensor1","event_id":"%s","observed_at":"%s","unit":"m/s2","value":"%.6f"}\n'
            % row for row in backfill)
        self_app = Application(db)
        self_app.call('batch.stage',
                      {'batch_id': 'backfill', 'source': 'feed1', 'content': lines})
        self_app.call('batch.drain', {'batch_id': 'backfill'})
        replay = svc.replay(
            'plan1', window_start='2026-02-01T00:00:00Z',
            window_end=ts(28), label='backfill-bucket-20')
        outcomes = {d['alert_id']: d['outcome'] for d in replay['differences']}
        self.assertTrue(any(v in {'delayed', 'withdrawn', 'advanced', 'new'}
                            for v in outcomes.values()))
        # 通知去重：不存在对同一旧身份的第二次 notify。
        for item in replay['notifications']:
            if item['outcome'] in {'unchanged', 'delayed', 'advanced'}:
                self.assertNotEqual(item['decision'], 'notify')
        # 旧版本完整保留在版本日志中。
        log_lines = [line for line in Path(replay['version_log']).read_text().splitlines()
                     if line.strip()]
        self.assertGreaterEqual(len(log_lines), 2)
        # 确认标记被带到新版本。
        report = svc.report('plan1')
        acked = [a for a in report['alert_history']
                 if a['chart'] == 'cusum' and a['side'] == 'upper']
        self.assertTrue(any(a['acknowledged'] for a in acked))
        db.close()

    def test_replay_withdrawal_when_shift_disappears(self):
        run_dir = Path(self.dir.name) / 'withdraw'
        db, svc = self._build(run_dir)
        svc.run('plan1', window_start=ts(12), window_end=ts(28))
        before = {a['alert_id'] for a in svc.report('plan1')['alert_history']}
        self.assertTrue(before)
        # 校准更正把偏移段读数整体拉回基线水平（offset 为负）。
        self.app_cal = Application(db)
        self.app_cal.call('calibration.correct', {
            'calibration_id': 'c1', 'revision': 1,
            'scale': '1', 'offset': '-1.6', 'clock_offset_us': 0})
        # 重新物化观测值：直接用校准计划应用更正。
        plan = self.app_cal.call('calibration.plan', {'calibration_id': 'c1'})
        self.app_cal.call('calibration.apply',
                          {'calibration_id': 'c1', 'expected_revision': 2})
        replay = svc.replay(
            'plan1', window_start='2026-02-01T00:00:00Z',
            window_end=ts(28), label='calibration-correction')
        withdrawn = [d for d in replay['differences'] if d['outcome'] == 'withdrawn']
        self.assertTrue(withdrawn)
        # 撤回的身份不允许再发通知。
        self.assertTrue(all(
            n['decision'] != 'notify' for n in replay['notifications']
            if n['outcome'] == 'withdrawn'))
        db.close()


class PlanFreezeEdgeTests(SPMSupport):
    def test_zero_scale_baseline_fails_explicitly_at_service(self):
        # 完全恒定的基线 -> 组内尺度为零，必须显式失败而不是产出可用方案。
        rows = [(f'c-{b}-{p}', ts(b, p), 10.0) for b in range(12) for p in range(3)]
        self.ingest(rows, 'constant')
        svc = self.service()
        with self.assertRaises(PipelineError) as ctx:
            svc.define_plan(self.plan_config())
        self.assertEqual(ctx.exception.code, 'spm_zero_scale')

    def test_duplicate_plan_identifier_rejected(self):
        self.ingest(self.baseline_rows(), 'base')
        svc = self.service()
        svc.define_plan(self.plan_config())
        with self.assertRaises(PipelineError) as ctx:
            svc.define_plan(self.plan_config())
        self.assertEqual(ctx.exception.code, 'spm_plan_exists')


class ReplayNewTriggerTests(SPMSupport):
    def test_new_trigger_notifies_once_and_then_is_suppressed(self):
        # 基线 + 一段完全正常的监测窗口（无告警）。
        self.ingest(self.baseline_rows(), 'base')
        svc, _ = self.define()
        calm = [(f'k-{b}-{p}', ts(b, p), 10.0 + 0.05 * p)
                for b in range(12, 20) for p in range(3)]
        self.ingest(calm, 'calm')
        svc.run('plan1', window_start=ts(12), window_end=ts(20))
        self.assertEqual(svc.report('plan1')['alert_history'], [])
        # 补录一个强持续上偏移的新桶序列，制造新版本里的全新触发。
        shifted = [(f'up-{b}-{p}', ts(b, p), 13.0)
                   for b in range(20, 24) for p in range(3)]
        self.ingest(shifted, 'shifted')
        replay = svc.replay(
            'plan1', window_start='2026-02-01T00:00:00Z',
            window_end=ts(24), label='new-shift')
        new_items = [n for n in replay['notifications'] if n['outcome'] == 'new']
        self.assertTrue(new_items)
        # 全新身份允许且仅允许通知一次。
        self.assertTrue(all(n['decision'] == 'notify' for n in new_items))
        notified = set(svc.report('plan1')['notified_alert_ids'])
        self.assertTrue(all(n['alert_id'] in notified for n in new_items))
        # 再重放一次：同样的新身份这次必须被抑制，不能发第二遍。
        second = svc.replay(
            'plan1', window_start='2026-02-01T00:00:00Z',
            window_end=ts(24), label='new-shift-repeat')
        again = [n for n in second['notifications']
                 if n['alert_id'] in {x['alert_id'] for x in new_items}
                 and n['decision'] == 'notify']
        self.assertEqual(again, [])


if __name__ == '__main__':
    unittest.main()
