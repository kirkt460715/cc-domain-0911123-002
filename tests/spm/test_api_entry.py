"""SPM 通过统一 Application 入口调用的集成测试。"""
import tempfile
import unittest
from pathlib import Path

from hangar_ingest.api import Application
from hangar_ingest.runs.database import Database
from hangar_ingest.types import PipelineError
from tests.spm.test_service import SPMSupport, ts


class SPMApplicationEntryTests(SPMSupport):
    def _app_with_spm_dir(self):
        state_dir = Path(self.dir.name) / 'via-api'
        return Application(self.db, spm_state_directory=state_dir), state_dir

    def test_full_flow_through_application_call(self):
        app, _ = self._app_with_spm_dir()
        self.ingest(self.baseline_rows(), 'base')
        defined = app.call('spm.plan.define', {'config': self.plan_config()})
        self.assertTrue(defined['plan']['frozen'])
        self.ingest([(f'm{i}', ts(13, i), 10.0) for i in range(3)], 'mon')
        result = app.call('spm.run', {
            'plan_id': 'plan1', 'window_start': ts(12), 'window_end': ts(15)})
        self.assertTrue(result['subgroups'])
        # 未知方案必须显式报错。
        with self.assertRaises(PipelineError) as ctx:
            app.call('spm.report', {'plan_id': 'missing'})
        self.assertEqual(ctx.exception.code, 'spm_unknown_plan')

    def test_unknown_spm_operation_rejected(self):
        app, _ = self._app_with_spm_dir()
        with self.assertRaises(PipelineError) as ctx:
            app.call('spm.diagnose', {'plan_id': 'plan1'})
        self.assertEqual(ctx.exception.code, 'unknown_operation')

    def test_invalid_plan_config_surfaces_validation_error(self):
        app, _ = self._app_with_spm_dir()
        self.ingest(self.baseline_rows(), 'base')
        bad = self.plan_config()
        bad['grid_seconds'] = 0
        with self.assertRaises(PipelineError):
            app.call('spm.plan.define', {'config': bad})

    def test_commands_list_includes_spm_operations(self):
        app, _ = self._app_with_spm_dir()
        commands = app.commands()
        for operation in ('spm.plan.define', 'spm.run', 'spm.acknowledge',
                          'spm.replay', 'spm.report'):
            self.assertIn(operation, commands)


if __name__ == '__main__':
    unittest.main()
