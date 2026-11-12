"""SPM 领域枚举与数据结构。

设计约束：

* 所有"不正常"的输入都有显式状态，绝不插值、补零或当成正常值；
* 数据结构均可 JSON 序列化，冻结后的方案与状态可直接落盘做检查点；
* 统计量统一使用 Python ``float``（标准 IEEE-754），输入读数先经现有
  Decimal 管线校验，进入 SPM 时才转换，保证同一输入在任何机器上逐位一致。
"""
from __future__ import annotations

import enum


class SubgroupStatus(str, enum.Enum):
    """固定网格子组的明确状态。"""

    VALID = 'valid'
    # 网格桶存在，但桶内没有任何未被排除的读数：明确为空，不补值。
    EMPTY = 'empty'
    # 与上一个有效子组之间跨越了一个以上的网格步长（存在空网格）。
    IRREGULAR_GAP = 'irregular_gap'
    # 桶内有效读数少于方案要求的最小点数，无法形成有效子组。
    INSUFFICIENT = 'insufficient'


class ExclusionCode(str, enum.Enum):
    """读数被排除出子组的理由，全部来自现有业务规则的判定结果。"""

    MAINTENANCE_WINDOW = 'maintenance_window'
    ALIAS_NOT_VALID = 'alias_not_valid'
    CALIBRATION_MISSING = 'calibration_missing'
    CALIBRATION_STALE = 'calibration_stale'
    QUALITY_REJECTED = 'quality_rejected'
    QUALITY_WARNING = 'quality_warning'
    OUTSIDE_GRID = 'outside_grid'
    DUPLICATE = 'duplicate'


class ChartKind(str, enum.Enum):
    EWMA = 'ewma'
    CUSUM = 'cusum'


class Side(str, enum.Enum):
    """双侧控制图的方向。"""

    UPPER = 'upper'
    LOWER = 'lower'


class AlertState(str, enum.Enum):
    """告警生命周期状态（状态机，见 ``alerts`` 模块）。"""

    TRIGGERED = 'triggered'        # 首次触发
    PERSISTING = 'persisting'      # 持续超限
    ACKNOWLEDGED = 'acknowledged'  # 人工确认（不重置任何统计量）
    RECOVERED = 'recovered'        # 统计量回到控制限内
    RETRIGGERED = 'retriggered'    # 恢复后再次触发（新一轮）
    WITHDRAWN = 'withdrawn'        # 重放后该触发不再成立（仅历史版本）


class Transition(str, enum.Enum):
    """驱动告警状态转换的事件类型。"""

    FIRST_TRIGGER = 'first_trigger'
    PERSIST = 'persist'
    ACKNOWLEDGE = 'acknowledge'
    RECOVER = 'recover'
    RETRIGGER = 'retrigger'
    # 重放产生的版本级动作，不会出现在实时状态机里。
    WITHDRAW = 'withdraw'
    DELAY = 'delay'


class NotificationDecision(str, enum.Enum):
    """通知去重决策。同一告警身份的同一触发只允许通知一次。"""

    NOTIFY = 'notify'
    SUPPRESSED_DUPLICATE = 'suppressed_duplicate'
    WITHDRAWN = 'withdrawn'
    DELAYED = 'delayed'


class ReplayOutcome(str, enum.Enum):
    """重放版本差异中，单个告警条目的处置。"""

    NEW = 'new'                    # 新版本新增的触发
    UNCHANGED = 'unchanged'        # 两个版本完全一致
    DELAYED = 'delayed'            # 同一告警身份，触发子组延后
    ADVANCED = 'advanced'          # 同一告警身份，触发子组提前
    WITHDRAWN = 'withdrawn'        # 旧版本存在、新版本不再成立
