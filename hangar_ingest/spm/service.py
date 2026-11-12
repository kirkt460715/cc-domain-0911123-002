"""SPM 应用服务：对接现有数据库，编排冻结、监测、确认、重放。

服务层不引入新的业务规则，只负责：

* 从现有 ``observations`` / ``work_orders`` / ``aliases`` / ``calibrations`` /
  ``quality_rules`` 取数并交给 ``exclusions`` 做纳入口判定；
* 调用纯函数引擎完成逐子组处理，并通过 ``StateStore`` 原子落检查点；
* 重放时保留旧版本、计算版本差异与通知去重决策。
"""
from __future__ import annotations

import os

from hangar_ingest.types import fail, instant, micros
from hangar_ingest.spm import engine, exclusions, grid, plans
from hangar_ingest.spm.engine import MonitorState, StateStore


class MonitoringService:
    def __init__(self, db, state_directory, *, clock=None):
        self.db = db
        self.store = StateStore(state_directory)
        self.clock = clock or _utc_now

    # ------------------------------------------------------------------ #
    # 方案配置与基线冻结
    # ------------------------------------------------------------------ #
    def define_plan(self, config):
        plan = plans.build_plan(config)
        if self._plan_file_exists(plan.plan_id):
            fail('spm_plan_exists', 'Plan identifier is already in use', plan_id=plan.plan_id)
        readings, device = self._readings_for(plan, plan.baseline_start, plan.baseline_end)
        subgroups, _ = grid.build_subgroups(
            plan, readings,
            window_start=plan.baseline_start, window_end=plan.baseline_end,
        )
        frozen = plans.freeze(
            plan, subgroups, at=self.clock(),
            input_fingerprint=grid.fingerprint(readings),
        )
        state = MonitorState(frozen)
        state.input_fingerprint = frozen.input_fingerprint
        self.store.save(state)
        return {'plan': frozen.to_dict(),
                'baseline_subgroups': [group.to_dict() for group in subgroups]}

    def get_plan(self, plan_id):
        state = self.store.load(plan_id)
        if state is None:
            fail('spm_unknown_plan', 'Monitoring plan does not exist', plan_id=plan_id)
        return state.plan.to_dict()

    # ------------------------------------------------------------------ #
    # 监测处理（离线一次跑与逐批跑共用）
    # ------------------------------------------------------------------ #
    def run(self, plan_id, *, window_start, window_end, batch_size=None,
            include_warnings=False, source='feed1', external_id=None):
        state = self._require_state(plan_id)
        plan = state.plan
        window_start = instant(window_start)
        window_end = instant(window_end)
        if window_start >= window_end:
            fail('spm_window', 'Monitoring window must be nonempty')
        readings, device = self._readings_for(
            plan, window_start, window_end, include_warnings=include_warnings,
            source=source, external_id=external_id,
        )
        fingerprint = grid.fingerprint(readings)
        subgroups, outside = grid.build_subgroups(
            plan, readings, window_start=window_start, window_end=window_end,
        )
        maintenance_buckets = self._maintenance_buckets(plan, subgroups)
        if batch_size is None:
            batches = [subgroups]
        else:
            if batch_size < 1:
                fail('spm_batch_size', 'batch_size must be positive')
            # 按网格顺序切分（含空/不足子组），离线与逐批的处理序列完全一致。
            batches = [subgroups[i:i + batch_size]
                       for i in range(0, len(subgroups), batch_size)]
        for chunk in batches:
            engine.process_subgroups(state, chunk, maintenance_buckets=maintenance_buckets)
            self.store.save(state)
        state.input_fingerprint = fingerprint
        saved = self.store.save(state)
        return {
            'plan_id': plan_id,
            'input_fingerprint': fingerprint,
            'subgroups': [group.to_dict() for group in subgroups],
            'outside_window': outside,
            'evaluations': state.evaluations,
            'nonevaluable': state.nonevaluable,
            'checkpoint': saved,
        }

    def acknowledge(self, plan_id, chart, side, *, note, at=None):
        state = self._require_state(plan_id)
        if chart not in state.episodes or side not in state.episodes[chart]:
            fail('spm_unknown_chart', 'Unknown chart or side', chart=chart, side=side)
        episode = alerts_acknowledge(state, chart, side, note=note, at=at or self.clock())
        self.store.save(state)
        return episode.to_dict()

    # ------------------------------------------------------------------ #
    # 重放：补录 / 校准更正后从最早受影响子组重新计算
    # ------------------------------------------------------------------ #
    def replay(self, plan_id, *, window_start, window_end, label,
               include_warnings=False, source='feed1', external_id=None):
        old_state = self._require_state(plan_id)
        plan = old_state.plan
        window_start = instant(window_start)
        window_end = instant(window_end)
        # 冻结参数（中心/尺度/控制限）保持不变；重放只重建监测序列。
        fresh = MonitorState(plan)
        readings, _ = self._readings_for(
            plan, window_start, window_end, include_warnings=include_warnings,
            source=source, external_id=external_id,
        )
        fresh.input_fingerprint = grid.fingerprint(readings)
        subgroups, outside = grid.build_subgroups(
            plan, readings, window_start=window_start, window_end=window_end,
        )
        maintenance_buckets = self._maintenance_buckets(plan, subgroups)
        engine.process_subgroups(
            fresh, subgroups, maintenance_buckets=maintenance_buckets,
        )
        differences = engine.diff_versions(old_state, fresh)
        notifications = engine.replay_notifications(old_state, fresh, differences)
        engine.carry_forward(old_state, fresh, notifications)
        self.store.append_version(
            old_state, label=f'{label}:before', at=self.clock(),
        )
        log_path = self.store.append_version(
            fresh, label=f'{label}:after', at=self.clock(),
            differences=differences, notifications=notifications,
        )
        self.store.save(fresh)
        return {
            'plan_id': plan_id,
            'label': label,
            'input_fingerprint': fresh.input_fingerprint,
            'subgroups': [group.to_dict() for group in subgroups],
            'outside_window': outside,
            'differences': differences,
            'notifications': notifications,
            'version_log': log_path,
        }

    # ------------------------------------------------------------------ #
    # 报告视图
    # ------------------------------------------------------------------ #
    def report(self, plan_id):
        state = self._require_state(plan_id)
        active = []
        history = []
        for chart in sorted(state.episodes):
            for side in sorted(state.episodes[chart]):
                for episode in state.episodes[chart][side]:
                    item = episode.to_dict()
                    if episode.closed_at is None:
                        active.append(item)
                    history.append(item)
        return {
            'plan': state.plan.to_dict(),
            'last_processed_bucket': state.last_processed_bucket,
            'input_fingerprint': state.input_fingerprint,
            'active_alerts': active,
            'alert_history': history,
            'evaluations': state.evaluations,
            'nonevaluable': state.nonevaluable,
            'notified_alert_ids': sorted(state.notified_alert_ids),
        }

    # ------------------------------------------------------------------ #
    # 内部辅助
    # ------------------------------------------------------------------ #
    def _require_state(self, plan_id):
        state = self.store.load(plan_id)
        if state is None:
            fail('spm_unknown_plan', 'Monitoring plan does not exist', plan_id=plan_id)
        return state

    def _plan_file_exists(self, plan_id):
        return os.path.exists(self.store._path(plan_id))

    def _readings_for(self, plan, start, end, *, include_warnings=False,
                      source='feed1', external_id=None):
        device = self.db.one(
            'SELECT * FROM devices WHERE aircraft=? AND component=? AND metric=? '
            'ORDER BY device_id LIMIT 1',
            (plan.aircraft, plan.component, plan.metric),
        )
        if device is None:
            fail('spm_unknown_device', 'No device matches the plan dimensions',
                 aircraft=plan.aircraft, component=plan.component, metric=plan.metric)
        if external_id is None:
            alias = self.db.one(
                'SELECT external_id FROM aliases WHERE device_id=? ORDER BY valid_from LIMIT 1',
                (device['device_id'],),
            )
            external_id = alias['external_id'] if alias else device['device_id']
        readings, _ = exclusions.load_readings(
            self.db, plan, source=source, external_id=external_id,
            include_warnings=include_warnings, window_start=start, window_end=end,
        )
        return readings, device

    def _maintenance_buckets(self, plan, subgroups):
        """维修窗口结束（关闭时刻）落在哪个网格桶，就在该桶按方案决定是否重置。"""
        orders = self.db.all(
            "SELECT order_id,closed_at FROM work_orders WHERE aircraft=? AND component=? "
            "AND state!='cancelled' ORDER BY closed_at,order_id",
            (plan.aircraft, plan.component),
        )
        origin_us = micros(plan.grid_origin)
        buckets = set()
        for order in orders:
            index = grid.bucket_index(micros(order['closed_at']), origin_us, plan.grid_seconds)
            buckets.add(index)
        return buckets


def alerts_acknowledge(state, chart, side, *, note, at):
    from hangar_ingest.spm import alerts
    return alerts.acknowledge(state.episodes[chart][side], at=at, note=note)


def _utc_now():
    from hangar_ingest.types import utcnow
    return utcnow()
