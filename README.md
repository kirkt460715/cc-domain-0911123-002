# 维修遥测接入

处理设备读数和维修工单，经过别名、单位、校准及质量规则后物化分区，生成带水位和输入血缘的发布。

Python 3.11 标准库与 SQLite 即可运行。数据、时钟和文件均在本地，不需要服务凭据。示例只使用合成记录。

## 运行

在仓库根目录执行：

```sh
python -m hangar_ingest --db :memory: demo --fixture fixtures/demo.json
python -m examples.context
python -m examples.spm_demo
python -m unittest discover -s tests -v
python -m compileall -q hangar_ingest examples
```

示例使用内存或临时目录，不会改动持久台账。`examples/context.json` 记录一组可重复导入的业务输入，字段与时间口径见 `examples/数据说明.md`。`examples.context` 检查数据并调用现有产品入口；它只展示原有业务数据，不计算分析结论。

`tests` 覆盖现有业务生命周期、非法输入、版本和完整性边界，另有示例导入回归。完整测试和上述演示可在一次短终端会话中执行。

## 统计过程监测（SPM）

`hangar_ingest/spm` 在不新增原始读数阈值的前提下，提供逐点可解释的统计过程监测，仅输出**统计异常提示**，不输出任何故障诊断结论：

- 工程师按机型、部件、指标和已确认正常的基线区间定义方案，按固定网格把每架飞机的读数汇成有效子组，并冻结中心、尺度与双侧 EWMA / CUSUM 控制限；参数、控制限与最少基线子组数均做范围校验，基线不足或零尺度显式失败，不产生"看似可用"的方案。
- 不等间隔（跨越空网格）、空网格、不足最小点数都有明确状态，绝不插值或补成正常值；维修窗口、别名有效期、校准缺失/过期修订与质量排除继续沿用现有规则，并逐条保留排除理由。
- 告警区分首次触发、持续、人工确认、恢复与再次触发，记录驱动每次状态转换的子组与累积统计量。人工确认只改标记，不重置统计量；维修后是否重置由方案 `reset_after_maintenance` 决定。
- 处理函数是确定性纯函数，状态可 JSON 序列化并原子落检查点：同一冻结输入下离线一次处理与逐批处理得到完全相同的序列，进程重启从最后检查点继续且不重复通知。
- 补录更早读数或校准更正后，从最早受影响子组重放，旧告警版本整体保留在版本日志中，并对每个告警身份解释撤回、延后、提前或新增；同一身份不会收到第二遍通知。

`python -m examples.spm_demo` 用合成数据完整演示稳定段、单点尖峰、持续偏移与缺口，并打印每个有效子组、排除理由、触发依据、离线/逐批等价性校验、重放版本差异与通知去重结果。SPM 检查点与版本日志默认写入本地 `spm-state/`（可用环境变量 `HANGAR_SPM_STATE_DIR` 覆盖），也可通过 `Application(db, spm_state_directory=...)` 注入。统一操作入口为 `spm.plan.define`、`spm.plan.get`、`spm.run`、`spm.acknowledge`、`spm.replay`、`spm.report`。

## 容器

```sh
docker build -t local/hangar_ingest .
docker run --rm --network none local/hangar_ingest
docker run --rm --network none local/hangar_ingest python -m examples.context
```

镜像构建完成后运行和测试不下载文件。持久化使用各自命令的本地路径参数；具体操作及参数可通过 `python -m hangar_ingest --help` 查询。第三方依赖清单为空。
