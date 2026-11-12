"""告警状态机。

每个 ``(机型, 图表, 方向)`` 维护一条独立告警序列（episode）。状态转换完全由
子组评估结果驱动，人工确认只改变标记、绝不重置任何统计量：

    无 episode
      └─ 超限 ──> TRIGGERED (first_trigger)
    TRIGGERED / RETRIGGERED / ACKNOWLEDGED
      ├─ 继续超限 ──> PERSISTING（ACKNOWLEDGED 保持 ACKNOWLEDGED，记 persist）
      ├─ 人工确认 ──> ACKNOWLEDGED（acknowledge，不动统计量）
      └─ 回到限内 ──> RECOVERED (recover)，episode 关闭
    RECOVERED（已关闭）
      └─ 再次超限 ──> 新 episode，TRIGGERED 并标 RETRIGGERED (retrigger)

维修重置由引擎层决定：若方案要求重置，图表统计量归零，当前未恢复的 episode 以
``maintenance_reset`` 原因关闭（不是恢复，也不是确认）。
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict

from hangar_ingest.types import fail, instant, text
from hangar_ingest.spm.models import AlertState, Transition


@dataclass
class AlertEvent:
    transition: str
    subgroup_id: str
    bucket_index: int
    event_time: str
    statistic: float                 # 触发/转换时该方向的累积统计量
    control_limit: float             # 当时的控制限（sigma 单位，冻结值）
    standardized_mean: float
    note: str | None = None

    def to_dict(self):
        return asdict(self)


@dataclass
class Episode:
    episode_no: int                 # 同一 (图表, 方向) 的第几轮，从 1 开始
    alert_id: str                   # 稳定身份：plan/aircraft/chart/side/episode
    chart: str
    side: str
    state: str
    opened_at_subgroup: str
    opened_bucket_index: int
    opened_at: str
    closed_at_subgroup: str | None = None
    closed_bucket_index: int | None = None
    closed_at: str | None = None
    close_reason: str | None = None  # recovered | maintenance_reset | withdrawn(重放)
    events: list = field(default_factory=list)
    acknowledged: bool = False
    acknowledged_at: str | None = None
    acknowledgment_note: str | None = None

    def to_dict(self):
        data = asdict(self)
        data['events'] = [event.to_dict() for event in self.events]
        return data


def make_alert_id(plan_id, aircraft, chart, side, episode_no):
    return f'{plan_id}/{aircraft}/{chart}/{side}/episode/{episode_no}'


def open_episode(plan_id, aircraft, chart, side, episode_no, subgroup, event, *, retriggered):
    state = AlertState.RETRIGGERED.value if retriggered else AlertState.TRIGGERED.value
    episode = Episode(
        episode_no=episode_no,
        alert_id=make_alert_id(plan_id, aircraft, chart, side, episode_no),
        chart=chart, side=side, state=state,
        opened_at_subgroup=subgroup.subgroup_id,
        opened_bucket_index=subgroup.bucket_index,
        opened_at=subgroup.window_start,
    )
    episode.events.append(event)
    return episode


def evaluate(plan_id, aircraft, chart, side, episodes, subgroup, *, breached,
             statistic, control_limit, standardized_mean):
    """处理一个有效子组对单个 (图表, 方向) 的判定，返回 (episode 或 None, 事件或 None)。

    ``episodes`` 为该方向的历史 episode 列表（原地更新最后一个）。
    """
    now = subgroup.window_start
    active = episodes[-1] if episodes and episodes[-1].closed_at is None else None
    if breached:
        if active is None:
            episode_no = (episodes[-1].episode_no + 1) if episodes else 1
            transition = (
                Transition.RETRIGGER.value if episodes else Transition.FIRST_TRIGGER.value
            )
            event = AlertEvent(
                transition=transition, subgroup_id=subgroup.subgroup_id,
                bucket_index=subgroup.bucket_index, event_time=now,
                statistic=statistic, control_limit=control_limit,
                standardized_mean=standardized_mean,
            )
            episode = open_episode(
                plan_id, aircraft, chart, side, episode_no, subgroup, event,
                retriggered=bool(episodes),
            )
            episodes.append(episode)
            return episode, event
        # 已打开且继续超限：持续。确认过的 episode 维持 ACKNOWLEDGED。
        prior_state = active.state
        active.state = (
            AlertState.ACKNOWLEDGED.value if active.acknowledged else AlertState.PERSISTING.value
        )
        event = AlertEvent(
            transition=Transition.PERSIST.value, subgroup_id=subgroup.subgroup_id,
            bucket_index=subgroup.bucket_index, event_time=now,
            statistic=statistic, control_limit=control_limit,
            standardized_mean=standardized_mean,
            note=prior_state,
        )
        active.events.append(event)
        return active, event
    if active is not None:
        active.state = AlertState.RECOVERED.value
        active.closed_at_subgroup = subgroup.subgroup_id
        active.closed_bucket_index = subgroup.bucket_index
        active.closed_at = now
        active.close_reason = 'recovered'
        event = AlertEvent(
            transition=Transition.RECOVER.value, subgroup_id=subgroup.subgroup_id,
            bucket_index=subgroup.bucket_index, event_time=now,
            statistic=statistic, control_limit=control_limit,
            standardized_mean=standardized_mean,
        )
        active.events.append(event)
        return active, event
    return None, None


def acknowledge(episodes, *, at, note):
    """人工确认当前未恢复 episode；不触碰任何图表统计量。"""
    active = episodes[-1] if episodes and episodes[-1].closed_at is None else None
    if active is None:
        fail('spm_no_active_alert', 'There is no active alert to acknowledge')
    at = instant(at)
    note = text(note, 'note', 2000)
    active.acknowledged = True
    active.acknowledged_at = at
    active.acknowledgment_note = note
    active.state = AlertState.ACKNOWLEDGED.value
    active.events.append(AlertEvent(
        transition=Transition.ACKNOWLEDGE.value,
        subgroup_id=active.opened_at_subgroup,
        bucket_index=active.opened_bucket_index,
        event_time=at, statistic=active.events[-1].statistic,
        control_limit=active.events[-1].control_limit,
        standardized_mean=active.events[-1].standardized_mean,
        note=note,
    ))
    return active


def close_for_maintenance(episodes, *, subgroup, reason='maintenance_reset'):
    """维修重置时关闭仍打开的 episode（区别于恢复与确认）。"""
    closed = []
    active = episodes[-1] if episodes and episodes[-1].closed_at is None else None
    if active is not None:
        active.closed_at_subgroup = subgroup.subgroup_id
        active.closed_bucket_index = subgroup.bucket_index
        active.closed_at = subgroup.window_start
        active.close_reason = reason
        closed.append(active)
    return closed


def episode_from_dict(data):
    episode = Episode(
        episode_no=data['episode_no'], alert_id=data['alert_id'], chart=data['chart'],
        side=data['side'], state=data['state'],
        opened_at_subgroup=data['opened_at_subgroup'],
        opened_bucket_index=data['opened_bucket_index'], opened_at=data['opened_at'],
        closed_at_subgroup=data.get('closed_at_subgroup'),
        closed_bucket_index=data.get('closed_bucket_index'),
        closed_at=data.get('closed_at'), close_reason=data.get('close_reason'),
        acknowledged=data.get('acknowledged', False),
        acknowledged_at=data.get('acknowledged_at'),
        acknowledgment_note=data.get('acknowledgment_note'),
    )
    episode.events = [AlertEvent(**item) for item in data.get('events', [])]
    return episode
