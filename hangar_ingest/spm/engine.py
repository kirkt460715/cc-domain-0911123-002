"""SPM 监测引擎：逐子组处理、检查点、通知去重与重放版本管理。

引擎状态是纯数据（可 JSON 序列化），处理函数是确定性纯函数，持久化只负责原子
落盘。由此得到三条可验证性质：

1. 同一冻结输入（``grid.fingerprint`` 相同）下，离线一次处理与任意切分的逐批
   处理产出完全相同的子组序列、统计量序列与告警序列；
2. 每个子组处理完立即落检查点，进程重启后从最后检查点继续，不重复通知；
3. 补录/校准更正后从最早受影响子组重放：旧告警版本整体保留，新版本与旧版本
   做身份匹配并给出撤回/延后/提前/新增解释，同一告警身份只通知一次。
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass

from hangar_ingest.types import canonical, digest, fail, instant
from hangar_ingest.spm.models import (
    ChartKind, Side, AlertState, Transition, NotificationDecision, ReplayOutcome,
)
from hangar_ingest.spm import charts, alerts, numeric
from hangar_ingest.spm.plans import from_dict


STATE_VERSION = 1


def _limit_for_side(chart_spec, side):
    limits = chart_spec.control_limits
    if chart_spec.kind == ChartKind.EWMA.value:
        key = 'center_offset_upper' if side == Side.UPPER.value else 'center_offset_lower'
        return limits[key]
    return limits['h_distance']


@dataclass
class SubgroupEvaluation:
    """一个有效子组的完整逐点解释记录。"""

    subgroup: dict
    z: float | None
    chart_statistics: dict          # chart -> side -> statistic
    breaches: dict                  # chart -> [sides]
    transitions: list               # 本次产生的告警事件（含方向与依据）
    notifications: list             # 去重后的通知决策
    maintenance_reset: bool
    irregular_gap: bool

    def to_dict(self):
        return {
            'subgroup': self.subgroup,
            'standardized_mean': numeric.round_stat(self.z),
            'chart_statistics': self.chart_statistics,
            'breaches': self.breaches,
            'transitions': self.transitions,
            'notifications': self.notifications,
            'maintenance_reset': self.maintenance_reset,
            'irregular_gap': self.irregular_gap,
        }


class MonitorState:
    """一架飞机在一个方案下的全部可变状态。"""

    def __init__(self, plan):
        self.plan = plan
        # chart -> 图表累积状态
        self.chart_states = {spec.kind: charts.initial_state(spec.kind) for spec in plan.charts}
        # chart -> side -> list[Episode]
        self.episodes = {
            spec.kind: {Side.UPPER.value: [], Side.LOWER.value: []}
            for spec in plan.charts
        }
        # 已通知过的告警身份集合（跨重启持久化，去重的根本依据）。
        self.notified_alert_ids = set()
        # 已处理到的最后一个有效子组 bucket_index（None 表示尚未开始）。
        self.last_processed_bucket = None
        # 按处理顺序留存的评估记录（重放比对与报告都用它）。
        self.evaluations = []
        # 非有效子组的显式记录（空网格/不足点数），同样逐桶可解释。
        self.nonevaluable = []
        self.input_fingerprint = None

    def to_dict(self):
        return {
            'state_version': STATE_VERSION,
            'plan': self.plan.to_dict(),
            'chart_states': self.chart_states,
            'episodes': {
                chart: {side: [episode.to_dict() for episode in side_episodes]
                        for side, side_episodes in by_side.items()}
                for chart, by_side in self.episodes.items()
            },
            'notified_alert_ids': sorted(self.notified_alert_ids),
            'last_processed_bucket': self.last_processed_bucket,
            'evaluations': self.evaluations,
            'nonevaluable': self.nonevaluable,
            'input_fingerprint': self.input_fingerprint,
        }

    @classmethod
    def from_dict(cls, data):
        if data.get('state_version') != STATE_VERSION:
            fail('spm_state_version', 'Unsupported monitor state version',
                 version=data.get('state_version'))
        plan = from_dict(data['plan'])
        state = cls(plan)
        state.chart_states = data['chart_states']
        state.episodes = {
            chart: {side: [alerts.episode_from_dict(item) for item in episodes]
                    for side, episodes in by_side.items()}
            for chart, by_side in data['episodes'].items()
        }
        state.notified_alert_ids = set(data['notified_alert_ids'])
        state.last_processed_bucket = data['last_processed_bucket']
        state.evaluations = data['evaluations']
        state.nonevaluable = data['nonevaluable']
        state.input_fingerprint = data['input_fingerprint']
        return state


def _decision_for_transition(transition):
    """把状态转换映射到通知语义（确认/持续/恢复本身不产生新通知）。"""
    if transition in (Transition.FIRST_TRIGGER.value, Transition.RETRIGGER.value):
        return NotificationDecision.NOTIFY.value
    return NotificationDecision.SUPPRESSED_DUPLICATE.value


def evaluate_subgroup(state, subgroup, *, maintenance_reset=False):
    """对一个有效子组推进全部图表与告警状态机，返回完整解释记录。

    维修重置先于统计更新执行：若方案要求重置，图表累积量归零、未恢复 episode
    以 maintenance_reset 关闭，然后该子组按全新起点参与统计。
    """
    plan = state.plan
    reset = bool(maintenance_reset and plan.reset_after_maintenance)
    transitions = []
    notifications = []
    all_statistics = {}
    all_breaches = {}

    if reset:
        for chart in state.chart_states:
            state.chart_states[chart] = charts.reset_state(state.chart_states[chart])
            for side in (Side.UPPER.value, Side.LOWER.value):
                alerts.close_for_maintenance(
                    state.episodes[chart][side], subgroup=subgroup,
                )

    z = charts.standardized(subgroup.mean, plan)
    for spec in plan.charts:
        chart = spec.kind
        new_state, statistics, breaches = charts.update(
            chart, state.chart_states[chart], z, spec.params,
        )
        state.chart_states[chart] = new_state
        all_statistics[chart] = statistics
        all_breaches[chart] = sorted(breaches)
        for side in (Side.UPPER.value, Side.LOWER.value):
            episode, event = alerts.evaluate(
                plan.plan_id, plan.aircraft, chart, side,
                state.episodes[chart][side], subgroup,
                breached=side in breaches, statistic=statistics[side],
                control_limit=_limit_for_side(spec, side),
                standardized_mean=numeric.round_stat(z),
            )
            if event is None:
                continue
            transitions.append({
                'alert_id': episode.alert_id, 'chart': chart, 'side': side,
                'transition': event.transition, 'state': episode.state,
                'subgroup_id': subgroup.subgroup_id,
                'bucket_index': subgroup.bucket_index,
                'statistic': event.statistic, 'control_limit': event.control_limit,
                'standardized_mean': event.standardized_mean,
            })
            decision = _decision_for_transition(event.transition)
            if decision == NotificationDecision.NOTIFY.value:
                if episode.alert_id in state.notified_alert_ids:
                    decision = NotificationDecision.SUPPRESSED_DUPLICATE.value
                else:
                    state.notified_alert_ids.add(episode.alert_id)
            notifications.append({
                'alert_id': episode.alert_id, 'chart': chart, 'side': side,
                'subgroup_id': subgroup.subgroup_id, 'decision': decision,
                'transition': event.transition,
            })

    record = SubgroupEvaluation(
        subgroup=subgroup.to_dict(), z=numeric.round_stat(z),
        chart_statistics=all_statistics, breaches=all_breaches,
        transitions=transitions, notifications=notifications,
        maintenance_reset=reset, irregular_gap=subgroup.irregular_gap,
    )
    state.last_processed_bucket = subgroup.bucket_index
    state.evaluations.append(record.to_dict())
    return record


def process_subgroups(state, subgroups, *, maintenance_buckets=None):
    """按网格顺序处理一组子组（离线/逐批共用同一入口）。

    ``maintenance_buckets`` 为发生维修窗口结束的桶索引集合，是否真正重置由
    方案 ``reset_after_maintenance`` 决定。非有效子组只登记不推进统计量。
    """
    maintenance_buckets = set(maintenance_buckets or ())
    recorded_nonevaluable = {
        item['subgroup']['bucket_index'] for item in state.nonevaluable
    }
    results = []
    for subgroup in sorted(subgroups, key=lambda group: group.bucket_index):
        if (state.last_processed_bucket is not None
                and subgroup.bucket_index <= state.last_processed_bucket
                and subgroup.status == 'valid'):
            # 重启续跑保护：已处理的有效子组不重复推进。
            continue
        if subgroup.status != 'valid':
            if subgroup.bucket_index in recorded_nonevaluable:
                continue
            recorded_nonevaluable.add(subgroup.bucket_index)
            state.nonevaluable.append({
                'subgroup': subgroup.to_dict(),
                'reason': subgroup.status,
                'excluded': subgroup.excluded,
            })
            continue
        record = evaluate_subgroup(
            state, subgroup,
            maintenance_reset=subgroup.bucket_index in maintenance_buckets,
        )
        results.append(record)
    return results


# --------------------------------------------------------------------------- #
# 重放与版本管理
# ---------------------------------------------------------------------------

def _episode_index(state):
    index = {}
    for chart, by_side in state.episodes.items():
        for side, episodes in by_side.items():
            for episode in episodes:
                index[episode.alert_id] = episode
    return index


def diff_versions(old_state, new_state):
    """对比两个版本的告警 episode，按稳定身份匹配并解释差异。

    身份为 ``plan/aircraft/chart/side/episode_no``：同一方向上第 N 轮 episode
    在两个版本里互相比对。
    """
    old_index = _episode_index(old_state)
    new_index = _episode_index(new_state)
    differences = []
    for alert_id in sorted(set(old_index) | set(new_index)):
        old = old_index.get(alert_id)
        new = new_index.get(alert_id)
        if old is not None and new is None:
            differences.append({
                'alert_id': alert_id, 'outcome': ReplayOutcome.WITHDRAWN.value,
                'old_trigger_subgroup': old.opened_at_subgroup,
                'new_trigger_subgroup': None,
                'old_bucket_index': old.opened_bucket_index,
                'new_bucket_index': None,
                'explanation': '触发条件在重放后不再成立，撤回该告警',
            })
            continue
        if old is None and new is not None:
            differences.append({
                'alert_id': alert_id, 'outcome': ReplayOutcome.NEW.value,
                'old_trigger_subgroup': None,
                'new_trigger_subgroup': new.opened_at_subgroup,
                'old_bucket_index': None,
                'new_bucket_index': new.opened_bucket_index,
                'explanation': '重放后新增的触发',
            })
            continue
        if old.opened_bucket_index == new.opened_bucket_index:
            outcome = ReplayOutcome.UNCHANGED.value
            explanation = '触发子组与依据未变化'
        elif new.opened_bucket_index > old.opened_bucket_index:
            outcome = ReplayOutcome.DELAYED.value
            explanation = '同一告警的触发子组延后'
        else:
            outcome = ReplayOutcome.ADVANCED.value
            explanation = '同一告警的触发子组提前'
        differences.append({
            'alert_id': alert_id, 'outcome': outcome,
            'old_trigger_subgroup': old.opened_at_subgroup,
            'new_trigger_subgroup': new.opened_at_subgroup,
            'old_bucket_index': old.opened_bucket_index,
            'new_bucket_index': new.opened_bucket_index,
            'explanation': explanation,
        })
    return differences


def replay_notifications(old_state, new_state, differences):
    """重放后的通知决策：只对真正的新情况通知，同一身份绝不发第二遍。"""
    decisions = []
    for diff in differences:
        alert_id = diff['alert_id']
        if diff['outcome'] == ReplayOutcome.UNCHANGED.value:
            decision = NotificationDecision.SUPPRESSED_DUPLICATE.value
        elif diff['outcome'] == ReplayOutcome.NEW.value:
            decision = (
                NotificationDecision.SUPPRESSED_DUPLICATE.value
                if alert_id in old_state.notified_alert_ids
                else NotificationDecision.NOTIFY.value
            )
        elif diff['outcome'] == ReplayOutcome.WITHDRAWN.value:
            decision = NotificationDecision.WITHDRAWN.value
        else:
            decision = NotificationDecision.DELAYED.value
        decisions.append({'alert_id': alert_id, 'decision': decision, **diff})
    return decisions


def carry_forward(old_state, new_state, notifications):
    """把人工决策与已通知身份带到重放后的新状态。

    * 已通知身份全部继承，本次真正需要通知的新身份再加入集合；
    * 人工确认是人的决策而非统计量：同一告警身份在新版本仍存在时，确认标记、
      时间与备注原样继承（不改变任何累积统计量）。
    """
    new_state.notified_alert_ids = set(old_state.notified_alert_ids)
    for item in notifications:
        if item['decision'] == NotificationDecision.NOTIFY.value:
            new_state.notified_alert_ids.add(item['alert_id'])

    old_index = _episode_index(old_state)
    new_index = _episode_index(new_state)
    for alert_id, episode in new_index.items():
        previous = old_index.get(alert_id)
        if previous is not None and previous.acknowledged:
            episode.acknowledged = True
            episode.acknowledged_at = previous.acknowledged_at
            episode.acknowledgment_note = previous.acknowledgment_note
            if episode.closed_at is None:
                episode.state = AlertState.ACKNOWLEDGED.value


# --------------------------------------------------------------------------- #
# 持久化（原子写、跨重启）
# ---------------------------------------------------------------------------

class StateStore:
    """文件系统检查点存储；目录内每个方案一个 JSON 文件与一个版本日志。"""

    def __init__(self, directory):
        self.directory = directory
        os.makedirs(directory, exist_ok=True)

    def _path(self, plan_id):
        safe = ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in plan_id)
        return os.path.join(self.directory, f'monitor-{safe}.json')

    def load(self, plan_id):
        path = self._path(plan_id)
        if not os.path.exists(path):
            return None
        with open(path, 'r', encoding='utf-8') as handle:
            return MonitorState.from_dict(json.load(handle))

    def save(self, state):
        path = self._path(state.plan.plan_id)
        payload = json.dumps(state.to_dict(), ensure_ascii=False, sort_keys=True,
                             allow_nan=False)
        directory, name = os.path.split(path)
        fd, tmp = tempfile.mkstemp(prefix=name + '.', suffix='.tmp', dir=directory)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise
        return {'path': path, 'digest': digest(payload)}

    def append_version(self, state, label, *, at, differences=None, notifications=None):
        """把一次（重放）版本追加到不可变版本日志，旧告警版本始终保留。"""
        log_path = os.path.join(self.directory,
                                f'versions-{_safe(state.plan.plan_id)}.jsonl')
        entry = {
            'label': label,
            'at': instant(at),
            'input_fingerprint': state.input_fingerprint,
            'last_processed_bucket': state.last_processed_bucket,
            'snapshot': state.to_dict(),
            'differences': differences or [],
            'notifications': notifications or [],
        }
        with open(log_path, 'a', encoding='utf-8') as handle:
            handle.write(canonical(entry) + '\n')
        return log_path


def _safe(plan_id):
    return ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in plan_id)
