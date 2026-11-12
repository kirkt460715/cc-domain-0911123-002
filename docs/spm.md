# 统计过程监测（SPM）口径说明

本模块只输出**统计过程意义上的异常提示**，不输出故障诊断结论。所有处理基于
Python 标准库，数值为确定性的 IEEE-754 浮点运算，输出级统计量统一按 12 位有效
数字舍入，保证同一冻结输入在离线、逐批和重启续跑下逐位一致。

## 1. 方案与冻结

工程师通过 `spm.plan.define` 提供：

| 字段 | 含义 | 校验 |
| --- | --- | --- |
| `aircraft` / `component` / `metric` | 监测维度，对应已注册设备 | 标识符规则 |
| `grid_seconds` / `grid_origin` | 固定网格步长与对齐原点（UTC） | 1 秒到 7 天 |
| `min_points_per_subgroup` | 单个有效子组所需最少读数 | 1–10000 |
| `min_baseline_subgroups` | 冻结所需最少基线有效子组 | 2–100000 |
| `reset_after_maintenance` | 维修窗口结束后是否重置累积统计量 | 布尔 |
| `baseline_start` / `baseline_end` | 已确认正常的基线区间 | 非空区间 |
| `charts` | `ewma` 与/或 `cusum` 的参数 | 见下 |

图表参数范围（越界即 `spm_invalid_parameter`，不静默夹紧）：

- EWMA：`lambda ∈ [0.05, 1]`，控制限倍数 `l ∈ [2, 5]`；
- CUSUM：参考偏移 `k ∈ [0.25, 2]`（sigma 单位），决策区间 `h ∈ [1, 10]`。

冻结时用基线**有效子组**计算：中心为全部基线读数均值，尺度为按自由度
`(n_i-1)` 加权的合并组内标准差。有效基线子组少于最小值报
`spm_baseline_insufficient`；组内尺度为零或非有限报 `spm_zero_scale`。
冻结后中心、尺度、控制限不可变，重新拟合必须新建方案。

## 2. 固定网格与子组状态

网格桶为半开区间 `[origin + k*g, origin + (k+1)*g)`，桶索引可负。每个桶有
明确状态，绝不插值：

- `valid`：桶内未被排除的读数达到 `min_points_per_subgroup`，参与统计；
- `empty`：桶内无任何未排除读数（含"桶存在但读数全被排除"）；
- `insufficient`：有效读数少于最小点数，登记原因 `fewer_than_min_points`；
- 有效桶若与上一个有效桶之间存在空网格，额外置 `irregular_gap=true` 并记录
  `missing_buckets_before`，但仍是有效子组、照常参与统计。

## 3. 纳入口判定（复用现有规则）

读数先经现有接入管线（别名解析、单位/校准、质量规则、维修工单）。SPM 复核后
把每条读数折叠为 `eligible` 与排除理由：

- `maintenance_window`：落在未取消维修工单覆盖区间（现有工单排序口径）；
- `alias_not_valid`：别名在观测时刻不能唯一有效（接入时失效的读数已在隔离区）；
- `calibration_missing` / `calibration_stale`：缺校准或观测的校准修订已过期；
- `quality_warning`：带质量警告，默认排除，方案可显式纳入警告级读数；
- 致命质量错误（超 min/max、邻接 delta）的读数在接入阶段即进入隔离区，
  不会出现在 `observations` 表，SPM 看不到，也无法被补成正常值。

## 4. 控制图与告警状态机

子组均值标准化为 `z = (x_bar - center)/scale`：

- EWMA：`e_t = lambda*z_t + (1-lambda)*e_{t-1}`，稳态控制限
  `±l*sqrt(lambda/(2-lambda))`（冻结时换算为实际量纲）；
- 双侧 CUSUM：`C+ = max(0, z-k+C+_prev)`、`C- = max(0, -k-z+C-_prev)`，
  任一方向超过 `h` 即超限。严格大于控制限才判定超限。

每个 `(图表, 方向)` 维护独立 episode，状态为：首次触发 `triggered` →
持续 `persisting` →（人工确认 `acknowledged`，不重置统计量）→ 回到限内
`recovered` 关闭 → 再次超限开启新 episode 并标 `retriggered`。维修重置（仅当
方案允许）把图表累积量归零，并以 `maintenance_reset` 关闭未恢复 episode，
区别于恢复与确认。

## 5. 确定性、检查点与通知去重

- `MonitorState` 为纯数据，可 JSON 序列化；`StateStore.save` 经临时文件 +
  `os.replace` 原子落盘，每个子组批次处理完即写检查点。
- 离线一次处理与任意切分的逐批处理共用同一纯函数入口，输出序列一致；
  重启后已处理的有效子组不重复推进，已通知的告警身份集合跨重启保留。
- 通知只在 `first_trigger` / `retrigger` 且该 `alert_id` 未通知过时发出，
  持续、确认、恢复一律 `suppressed_duplicate`。

## 6. 重放与版本

补录读数或校准更正后调用 `spm.replay`：以冻结参数从最早受影响子组重建整条
序列，再按稳定身份 `plan/aircraft/chart/side/episode_no` 与旧版本比对：

- `withdrawn`：旧触发在新版本不再成立（撤回）；
- `delayed` / `advanced`：同一身份触发子组延后或提前；
- `new` / `unchanged`：新增触发或无变化。

重放把 before/after 两个完整快照追加到不可变 JSONL 版本日志（旧版本不覆盖），
通知决策对撤回/延后/未变化一律不重发，仅对全新身份通知；人工确认标记与已通知
身份集合按身份继承到新版本。
