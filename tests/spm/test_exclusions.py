"""SPM 纳入口判定（exclusions）直接测试。

接入阶段的硬排除（别名失效、致命质量错误）由现有管线隔离区覆盖；这里专门测试
读数已在 observations 表内、但依据当前规则复核时需要带理由排除的路径。
"""
import json
import unittest

from hangar_ingest.spm import exclusions
from hangar_ingest.spm.models import ExclusionCode
from tests.spm.test_service import SPMSupport, ts


class ExclusionClassificationTests(SPMSupport):
    def _observation_row(self, event_time, *, quality=None, calibration='c1', rev=1):
        return {
            'observation_id': f'obs-{event_time}', 'event_time': event_time,
            'raw_time': event_time, 'value': '10.0',
            'calibration_id': calibration, 'calibration_revision': rev,
            'quality': json.dumps(quality or []),
        }

    def test_maintenance_window_classification(self):
        self.ingest(self.baseline_rows(), 'base')
        svc, _ = self.define()
        self.app.call('order.upsert', {'value': {
            'order_id': 'wo1', 'revision': 1, 'aircraft': 'AC1', 'component': 'engine',
            'opened_at': ts(14), 'closed_at': ts(15),
            'issue': 'scheduled', 'resolution': 'done', 'state': 'closed'}})
        device = self.db.one('SELECT * FROM devices WHERE device_id=?', ('v1',))
        plan = svc._require_state('plan1').plan
        row = self._observation_row(ts(14, 1))
        reading = exclusions.classify_observation(
            self.db, row, device, plan=plan, source='feed1',
            external_id='sensor1', include_warnings=False)
        self.assertFalse(reading.eligible)
        self.assertIn(ExclusionCode.MAINTENANCE_WINDOW.value, reading.exclusion_codes)
        self.assertEqual(reading.maintenance_order_id, 'wo1')

    def test_stale_calibration_revision_classified(self):
        self.ingest(self.baseline_rows(), 'base')
        svc, _ = self.define()
        self.app.call('calibration.correct', {
            'calibration_id': 'c1', 'revision': 1,
            'scale': '1', 'offset': '0.1', 'clock_offset_us': 0})
        device = self.db.one('SELECT * FROM devices WHERE device_id=?', ('v1',))
        plan = svc._require_state('plan1').plan
        # 观测记录仍指向修订 1，但当前校准已经是修订 2。
        row = self._observation_row(ts(13, 1), rev=1)
        reading = exclusions.classify_observation(
            self.db, row, device, plan=plan, source='feed1',
            external_id='sensor1', include_warnings=False)
        self.assertIn(ExclusionCode.CALIBRATION_STALE.value, reading.exclusion_codes)
        self.assertFalse(reading.eligible)

    def test_warning_level_reading_excluded_by_default_included_when_allowed(self):
        self.ingest(self.baseline_rows(), 'base')
        svc, _ = self.define()
        device = self.db.one('SELECT * FROM devices WHERE device_id=?', ('v1',))
        plan = svc._require_state('plan1').plan
        row = self._observation_row(
            ts(13, 1), quality=[{'code': 'sampling_gap', 'neighbor': 'previous'}])
        strict = exclusions.classify_observation(
            self.db, row, device, plan=plan, source='feed1',
            external_id='sensor1', include_warnings=False)
        self.assertFalse(strict.eligible)
        permissive = exclusions.classify_observation(
            self.db, row, device, plan=plan, source='feed1',
            external_id='sensor1', include_warnings=True)
        # 无维修窗口等其它排除理由时，警告级读数可被方案显式纳入。
        self.assertTrue(permissive.eligible)
        self.assertEqual(permissive.exclusion_codes,
                         (ExclusionCode.QUALITY_WARNING.value,))

    def test_missing_calibration_when_required(self):
        self.ingest(self.baseline_rows(), 'base')
        svc, _ = self.define()
        device = self.db.one('SELECT * FROM devices WHERE device_id=?', ('v1',))
        plan = svc._require_state('plan1').plan
        row = self._observation_row(ts(13, 1), calibration=None)
        reading = exclusions.classify_observation(
            self.db, row, device, plan=plan, source='feed1',
            external_id='sensor1', include_warnings=False)
        self.assertIn(ExclusionCode.CALIBRATION_MISSING.value, reading.exclusion_codes)
        self.assertFalse(reading.eligible)


if __name__ == '__main__':
    unittest.main()
