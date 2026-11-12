"""SPM 纯函数层测试：方案冻结、网格聚合、控制图与告警状态机。"""
import math
import unittest

from hangar_ingest.types import PipelineError
from hangar_ingest.spm import charts, grid, numeric, plans, alerts
from hangar_ingest.spm.models import AlertState, ChartKind, Side, SubgroupStatus
from hangar_ingest.spm.plans import Plan, ChartSpec


def plan_config(**overrides):
    config = {
        'plan_id': 'p1', 'aircraft': 'AC1', 'component': 'engine', 'metric': 'vibration',
        'grid_seconds': 60, 'grid_origin': '2026-01-01T00:00:00Z',
        'min_points_per_subgroup': 2, 'min_baseline_subgroups': 3,
        'reset_after_maintenance': False,
        'baseline_start': '2026-01-01T00:00:00Z', 'baseline_end': '2026-01-01T01:00:00Z',
        'charts': {'ewma': {'lambda': 0.2, 'l': 3.0}, 'cusum': {'k': 0.5, 'h': 4.0}},
    }
    config.update(overrides)
    return config


def reading(oid, minute, value, *, aircraft='AC1', eligible=True, reasons=()):
    return grid.Reading(
        observation_id=oid, aircraft=aircraft,
        event_time=f'2026-01-01T00:{minute:02d}:30Z',
        value=float(value), eligible=eligible, exclusion_codes=tuple(reasons),
    )


class PlanValidationTests(unittest.TestCase):
    def test_rejects_out_of_range_parameters(self):
        for field_name, bad in [
            ('grid_seconds', 0), ('min_points_per_subgroup', 0),
            ('min_baseline_subgroups', 1),
        ]:
            with self.assertRaises(PipelineError) as ctx:
                plans.build_plan(plan_config(**{field_name: bad}))
            self.assertEqual(ctx.exception.code, 'invalid_integer')

    def test_rejects_bad_chart_params(self):
        with self.assertRaises(PipelineError) as ctx:
            plans.build_plan(plan_config(charts={'ewma': {'lambda': 0.0, 'l': 3.0}}))
        self.assertEqual(ctx.exception.code, 'spm_invalid_parameter')
        with self.assertRaises(PipelineError) as ctx:
            plans.build_plan(plan_config(charts={'cusum': {'k': 0.5}}))
        self.assertEqual(ctx.exception.code, 'spm_invalid_chart')
        with self.assertRaises(PipelineError) as ctx:
            plans.build_plan(plan_config(charts={'shewhart': {}}))
        self.assertEqual(ctx.exception.code, 'spm_invalid_chart')

    def test_rejects_empty_or_reversed_baseline(self):
        with self.assertRaises(PipelineError):
            plans.build_plan(plan_config(
                baseline_end='2026-01-01T00:00:00Z',
                baseline_start='2026-01-01T01:00:00Z'))

    def test_freeze_requires_minimum_baseline_subgroups(self):
        plan = plans.build_plan(plan_config(min_baseline_subgroups=4))
        # 每桶 2 个点，共 3 个有效子组，少于要求的 4 个。
        readings = [reading(f'a{i}', i // 2, 10 + i) for i in range(6)]
        groups, _ = grid.build_subgroups(
            plan, readings,
            window_start='2026-01-01T00:00:00Z', window_end='2026-01-01T00:04:00Z',
        )
        self.assertEqual(sum(g.status == 'valid' for g in groups), 3)
        with self.assertRaises(PipelineError) as ctx:
            plans.freeze(plan, groups, at='2026-02-01T00:00:00Z',
                         input_fingerprint='x')
        self.assertEqual(ctx.exception.code, 'spm_baseline_insufficient')

    def test_freeze_rejects_zero_scale_and_is_explicit(self):
        plan = plans.build_plan(plan_config())
        # 每桶两个完全相同的点 -> 组内标准差为零。
        readings = [reading(f'a{i}', i // 2, 10.0) for i in range(8)]
        groups, _ = grid.build_subgroups(
            plan, readings,
            window_start='2026-01-01T00:00:00Z', window_end='2026-01-01T01:00:00Z',
        )
        with self.assertRaises(PipelineError) as ctx:
            plans.freeze(plan, groups, at='2026-02-01T00:00:00Z',
                         input_fingerprint='x')
        self.assertEqual(ctx.exception.code, 'spm_zero_scale')

    def test_freeze_round_trip_keeps_limits_and_is_immutable(self):
        plan = plans.build_plan(plan_config())
        readings = [reading(f'a{i}', i // 2, 10 + (i % 3) * 0.7) for i in range(12)]
        groups, _ = grid.build_subgroups(
            plan, readings,
            window_start='2026-01-01T00:00:00Z', window_end='2026-01-01T00:06:00Z',
        )
        frozen = plans.freeze(plan, groups, at='2026-02-01T00:00:00Z',
                              input_fingerprint='fp')
        self.assertTrue(frozen.frozen)
        with self.assertRaises(PipelineError) as ctx:
            plans.freeze(frozen, groups, at='2026-02-02T00:00:00Z',
                         input_fingerprint='fp')
        self.assertEqual(ctx.exception.code, 'spm_plan_frozen')
        restored = plans.from_dict(frozen.to_dict())
        self.assertEqual(restored.center, frozen.center)
        self.assertEqual(restored.scale, frozen.scale)
        self.assertEqual(restored.to_dict(), frozen.to_dict())


class GridTests(unittest.TestCase):
    def setUp(self):
        self.plan = plans.build_plan(plan_config())

    def test_empty_grid_is_explicit_not_imputed(self):
        # 桶 0 和桶 2 有数据，桶 1 为空，必须显式物化。
        readings = [
            reading('a1', 0, 10), reading('a2', 0, 11),
            reading('b1', 2, 10), reading('b2', 2, 11),
        ]
        groups, _ = grid.build_subgroups(
            self.plan, readings,
            window_start='2026-01-01T00:00:00Z', window_end='2026-01-01T00:04:00Z',
        )
        statuses = {g.bucket_index: g.status for g in groups}
        self.assertEqual(statuses[0], SubgroupStatus.VALID.value)
        self.assertEqual(statuses[1], SubgroupStatus.EMPTY.value)
        self.assertEqual(statuses[2], SubgroupStatus.VALID.value)
        # 桶 2 跨越空桶，标注不等间隔但仍是有效子组。
        self.assertTrue(groups[2].irregular_gap)
        self.assertEqual(groups[2].missing_buckets_before, 1)
        self.assertIsNone(groups[1].mean)

    def test_insufficient_subgroup_keeps_exclusion_reason(self):
        groups, _ = grid.build_subgroups(
            self.plan, [reading('a1', 0, 10)],
            window_start='2026-01-01T00:00:00Z', window_end='2026-01-01T00:01:00Z',
        )
        self.assertEqual(groups[0].status, SubgroupStatus.INSUFFICIENT.value)
        self.assertEqual(groups[0].excluded[0]['reason'], 'fewer_than_min_points')

    def test_excluded_readings_never_become_normal_values(self):
        readings = [
            reading('a1', 0, 10, eligible=False, reasons=('maintenance_window',)),
            reading('a2', 0, 11, eligible=False, reasons=('maintenance_window',)),
        ]
        groups, _ = grid.build_subgroups(
            self.plan, readings,
            window_start='2026-01-01T00:00:00Z', window_end='2026-01-01T00:01:00Z',
        )
        self.assertEqual(groups[0].status, SubgroupStatus.EMPTY.value)
        self.assertEqual(groups[0].values, [])
        self.assertEqual(groups[0].excluded[0]['reasons'], ['maintenance_window'])

    def test_bucket_index_is_stable_for_negative_buckets(self):
        origin = grid.micros('2026-01-01T00:00:00Z')
        idx = grid.bucket_index(origin - 30 * 1_000_000, origin, 60)
        self.assertEqual(idx, -1)

    def test_fingerprint_deterministic_and_order_independent(self):
        a = [reading('a', 1, 10), reading('b', 2, 12)]
        b = list(reversed(a))
        self.assertEqual(grid.fingerprint(a), grid.fingerprint(b))


class ChartTests(unittest.TestCase):
    def test_ewma_smoothing_and_two_sided_breach(self):
        state = charts.initial_state(ChartKind.EWMA.value)
        lam = 0.2
        ewma = 0.0
        breaches = set()
        for _ in range(12):
            state, stats, breaches = charts.update(
                ChartKind.EWMA.value, state, 1.5, {'lambda': lam, 'l': 3.0})
            ewma = lam * 1.5 + (1 - lam) * ewma
            self.assertAlmostEqual(stats[Side.UPPER.value], numeric.round_stat(ewma),
                                   places=10)
        self.assertIn(Side.UPPER.value, breaches)
        self.assertNotIn(Side.LOWER.value, breaches)
        # 下侧独立累积：换一个全新状态验证负向超限。
        lower_state = charts.initial_state(ChartKind.EWMA.value)
        for _ in range(8):
            lower_state, _, lower_breaches = charts.update(
                ChartKind.EWMA.value, lower_state, -1.5, {'lambda': lam, 'l': 3.0})
        self.assertIn(Side.LOWER.value, lower_breaches)

    def test_cusum_accumulates_then_resets_to_zero(self):
        state = charts.initial_state(ChartKind.CUSUM.value)
        params = {'k': 0.5, 'h': 4.0}
        for z in [3.0, 3.0]:
            state, _, breaches = charts.update(ChartKind.CUSUM.value, state, z, params)
        self.assertIn(Side.UPPER.value, breaches)
        # 负向大偏移先把上侧累积压回零，再累积下侧。
        for z in [-3.0, -3.0]:
            state, stats, breaches = charts.update(ChartKind.CUSUM.value, state, z, params)
        self.assertEqual(state['cusum_upper'], 0.0)
        self.assertIn(Side.LOWER.value, breaches)

    def test_single_spike_does_not_trigger_but_persistent_shift_does(self):
        # 与控制限一致的 EWMA 参数：l=3, lambda=0.2 -> 稳态限 ≈ 1.0 sigma。
        state = charts.initial_state(ChartKind.EWMA.value)
        params = {'lambda': 0.2, 'l': 3.0}
        _, _, spike_breach = charts.update(ChartKind.EWMA.value, state, 4.0, params)
        self.assertEqual(spike_breach, set())
        for z in [2.0] * 6:
            state, _, breaches = charts.update(ChartKind.EWMA.value, state, z, params)
        self.assertIn(Side.UPPER.value, breaches)


class AlertStateMachineTests(unittest.TestCase):
    def setUp(self):
        self.plan = plans.build_plan(plan_config())
        readings = [reading(f'a{i}', i // 2, 10 + (i % 2)) for i in range(12)]
        groups, _ = grid.build_subgroups(
            self.plan, readings,
            window_start='2026-01-01T00:00:00Z', window_end='2026-01-01T00:06:00Z',
        )
        self.subgroups = [g for g in groups if g.status == 'valid']

    def _sg(self, index):
        return next(g for g in self.subgroups if g.bucket_index == index)

    def test_full_lifecycle_trigger_persist_ack_recover_retrigger(self):
        episodes = []
        sg0, sg1, sg2, sg3, sg4 = (self._sg(i) for i in range(5))
        ep, ev = alerts.evaluate('p1', 'AC1', 'ewma', 'upper', episodes, sg0,
                                 breached=True, statistic=1.5, control_limit=1.0,
                                 standardized_mean=2.0)
        self.assertEqual(ev.transition, 'first_trigger')
        self.assertEqual(ep.state, AlertState.TRIGGERED.value)
        alerts.evaluate('p1', 'AC1', 'ewma', 'upper', episodes, sg1,
                        breached=True, statistic=1.6, control_limit=1.0,
                        standardized_mean=2.1)
        self.assertEqual(episodes[-1].state, AlertState.PERSISTING.value)
        alerts.acknowledge(episodes, at='2026-01-02T00:00:00Z', note='seen')
        # 确认不改变状态机对后续超限的处理，且确认后仍超限保持 ACKNOWLEDGED。
        alerts.evaluate('p1', 'AC1', 'ewma', 'upper', episodes, sg2,
                        breached=True, statistic=1.7, control_limit=1.0,
                        standardized_mean=2.2)
        self.assertEqual(episodes[-1].state, AlertState.ACKNOWLEDGED.value)
        alerts.evaluate('p1', 'AC1', 'ewma', 'upper', episodes, sg3,
                        breached=False, statistic=0.2, control_limit=1.0,
                        standardized_mean=0.2)
        self.assertEqual(episodes[-1].state, AlertState.RECOVERED.value)
        self.assertEqual(episodes[-1].close_reason, 'recovered')
        ep2, ev2 = alerts.evaluate('p1', 'AC1', 'ewma', 'upper', episodes, sg4,
                                   breached=True, statistic=1.4, control_limit=1.0,
                                   standardized_mean=1.9)
        self.assertEqual(ev2.transition, 'retrigger')
        self.assertEqual(ep2.state, AlertState.RETRIGGERED.value)
        self.assertEqual(ep2.episode_no, 2)

    def test_acknowledge_requires_active_episode(self):
        with self.assertRaises(PipelineError) as ctx:
            alerts.acknowledge([], at='2026-01-02T00:00:00Z', note='x')
        self.assertEqual(ctx.exception.code, 'spm_no_active_alert')

    def test_maintenance_close_is_distinct_from_recovery(self):
        episodes = []
        sg0, sg1 = self._sg(0), self._sg(1)
        alerts.evaluate('p1', 'AC1', 'cusum', 'upper', episodes, sg0,
                        breached=True, statistic=5.0, control_limit=4.0,
                        standardized_mean=3.0)
        alerts.close_for_maintenance(episodes, subgroup=sg1)
        self.assertEqual(episodes[0].close_reason, 'maintenance_reset')
        self.assertNotEqual(episodes[0].state, AlertState.ACKNOWLEDGED.value)


if __name__ == '__main__':
    unittest.main()
