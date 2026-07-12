# HypeEdge 实施计划

版本：v0.2
日期：2026-06-01  
状态：当前计划以“先可靠采集，后安全交易”为主线。任何阶段未满足门禁条件，不进入下一阶段。

## 0. 当前修复基线

本轮已将以下设计约束落到代码层：

- 风控默认 fail-closed：未接入真实账户、持仓、额度数据前，默认拒绝订单。
- EventBus 分层：行情事件允许丢旧保新；交易、风控、账户、kill switch、对账事件不允许静默丢弃。
- WebSocket 订阅按 Hyperliquid channel schema 构造：`allMids` 全局订阅，`candle` 带 `interval`，`l2Book` / `trades` / `activeAssetCtx` 按币种订阅。
- 配置优先级修正为：环境变量 / `.env` > YAML > Settings 默认值。
- REST 回填分页和限频估算更保守，按 K 线周期和 funding 小时粒度分页。
- ClickHouse 写入循环修复为长期挂起的 queue reader，避免每轮创建未取消的 `queue.get()` 任务。

## 1. Phase 1A：行情采集可用

目标：稳定采集并落地公开行情数据，不启动任何真实交易能力。

范围：

- WS：`l2Book`、`trades`、`candle`、`allMids`、`activeAssetCtx`。
- REST：`candleSnapshot`、`fundingHistory`、`meta` / `metaAndAssetCtxs`。
- ClickHouse：盘口、成交、K 线、funding 表写入。
- Metrics：WS 状态、事件丢弃数、ClickHouse 写入错误、REST 限频剩余额度估算。

门禁：

- `uv run pytest tests/unit/ -q` 通过。
- `uv run ruff check src/ tests/` 通过。
- `uv run mypy src/` 通过。
- 本地连续采集 24 小时无任务泄漏、无未处理异常。
- WS 重连后自动重订阅，ClickHouse flush 错误可见且不静默丢数据。

## 2. Phase 1B：数据完整性与回填

目标：让采集的数据可用于趋势/网格类策略研究。

范围：

- 增加回填作业状态表或 checkpoint 文件，记录每个 `coin + interval + endpoint` 的最后成功时间。
- 对历史回填增加幂等写入策略，避免重跑产生重复数据。
- 增加数据质量检查：时间断点、重复率、盘口 bid/ask 反转、异常价差、K 线缺口。
- 增加 DuckDB 可选导出路径，仅用于本地研究，不替代 ClickHouse 长期存储。

门禁：

- 任一币种任一 interval 可从 checkpoint 恢复回填。
- 数据质量报告能明确列出缺口和重复。
- 回填限频按 IP weight 保守执行，不与实时采集抢占全部预算。

## 3. Phase 2A：交易前置安全层

目标：先完成“不下错单”的基础设施，再接策略。

范围：

- Postgres 使用 Alembic 管理 schema，不再由 ORM 在启动时隐式 `create_all`。
- 订单存储改为 Decimal/Numeric，新增 append-only `order_events` 表。
- AccountTracker 接入 `clearinghouseState` 和用户 WS 事件，维护账户权益、峰值权益、持仓和 PnL。
- Reconciler 在启动、WS 重连、周期任务中校正本地订单与持仓。
- App 启动门禁：`trading_enabled=false`，只有对账成功、账户状态新鲜、kill switch 未触发时才允许策略提交订单。
- 启动时钟校验：检查本地时钟与 Hyperliquid API 服务器时间偏差，超过 1 秒告警并拒绝启动（design doc §16.2）。

门禁：

- 进程启动必须先完成对账，再启动任何策略。
- 对账失败触发 kill switch 或保持只撤不下模式。
- 账户状态过期时，风控拒绝开仓订单，仅允许撤单 / reduce-only。

## 4. Phase 2B：执行引擎

目标：完成可在 testnet 运行的执行闭环。

范围：

- ExecutionEngine 实现 `ExecutionClient`，作为唯一签名和下单出口。
- NonceManager 接入 Hyperliquid SDK 真实签名，所有 exchange action 经单队列串行。
- 所有订单必须有 cloid，提交超时后先 `orderStatus` 查询，不盲目重发。
- 下单、撤单、改单、批量操作均同时检查 IP weight 和地址动作额度。
- Kill switch 支持撤全单，可配置是否主动平仓。

门禁：

- testnet 下单 / 撤单 / 查询 / 重试 / 对账集成测试通过。
- 同一 cloid 重试不产生重复订单。
- 连续执行失败触发策略暂停，而不是无限重试。

## 5. Phase 2C：第一个实盘策略

目标：只上线中低频趋势跟随策略，不启用网格和做市。

范围：

- 趋势策略接口：信号生成、仓位目标、止损、退出条件。
- 风控实现：单币仓位、策略亏损、总回撤、最大杠杆、动作额度低水位。
- Paper trading 至少运行 2 周，使用真实行情、模拟执行或 testnet 执行。
- 小资金 mainnet 灰度上线，初始仅单币种、低杠杆、严格日亏损上限。
- 策略参数热更新：monitor YAML 变更 → 通知策略重新加载参数（不重启进程），每次变更记录审计日志（design doc §15.2）。

门禁：

- Paper trading 期间无重复下单、无未对账订单、无风控绕过。
- mainnet 启动必须显式配置 agent wallet，主钱包私钥不可进入进程。
- 手动 kill switch 路径实测可用。

## 6. Phase 3：网格与做市

目标：只有在额度、对账和执行稳定后，才验证更高动作频率策略。盘口做市的权威设计与完整执行计划见
`docs/market_making_design.md` 和 `docs/market_making_implementation_plan.md`。

范围：

- 动态网格先实现 regime 识别和区间击穿强平，不满足 regime 时不挂网格。
- 做市先修复盘口 freshness、可靠事件隔离、Decimal 交易类型和 instrument normalization。
- 建立统一 `TradingCommandService`、QuoteCoordinator、batch durable execution 和 ActionBudgetController。
- 第一版仅单币单档，报价由净 edge、库存、毒性和动作影子成本驱动，允许 `NO_QUOTE`。
- `reserveRequestWeight` 默认关闭，仅允许 emergency policy 使用，并增加单次/日/月成本上限和告警。
- 完成 Postgres 配置/状态/quote command schema、ClickHouse 高频分析表、API 和前端做市控制面重构。
- 热路径达到 design §18.6 的可测瓶颈后，再评估 PyO3/Rust，不提前拆进程。

门禁：

- stale/gap/user-stream overflow/重启/数据库故障下无重复单、漏撤单和风控绕过。
- 地址动作模型覆盖挂单、撤单、改单、批量、失败、重试、成交 earned rate 和紧急保留量。
- QuoteCoordinator/批量状态机/做市风控核心测试覆盖率至少 90%，UNKNOWN 均能权威恢复。
- 至少 14 天 mainnet shadow、testnet 故障验证和单 symbol 极小资金 canary；不以 L2 回测收益作为上线依据。
- 扩容前 trailing `USDC/action >= 1.25`，硬库存越限、重复订单和关键未解决对账差异均为 0。

## 7. 长期工程约束

- 新增事件必须注册到 `ALL_EVENT_TYPES`，并明确 lossy / reliable 策略。
- 新增交易能力默认关闭，通过配置和启动门禁显式启用。
- 新增配置项必须同步更新三个 YAML 环境和配置测试。
- 修改 ClickHouse schema 必须同步更新 `docs/design.md` 和 DDL。
- 修改 Postgres schema 必须通过 Alembic migration。
- 每个 bug fix 和新功能必须有单元测试；交易路径必须有 testnet 集成测试。

## 8. V2 重构执行计划

V2 使用旁路迁移，严禁旧执行链与新执行链同时向交易所发送订单。

1. **安全隔离与契约冻结**：默认禁用交易；完成 system state、订单状态机、API v1、SSE 事件和数据库模型设计。
2. **数据库扩展**：Alembic 创建 V2 投影、事实表、execution command、inbox/outbox 和审计表。
3. **安全内核**：实现 SafetyController、RiskGate、风险预占、规范 cloid 和持久化订单命令。
4. **执行与恢复**：实现 SignedActionExecutor、Hyperliquid adapter、UNKNOWN 查询恢复和统一动作额度预算。
5. **账户事实链**：接入 user order/fill stream，事务更新订单、仓位、账本并通过 outbox 发布。
6. **Fail-closed 对账**：启动、重连和周期对账必须完整成功后才启用策略。
7. **API v1 与认证**：所有 mutation 强制认证、权限、CSRF、幂等和审计。
8. **可靠实时通道**：控制面 SSE 由 Postgres outbox sequence 驱动，支持租约恢复、durable replay、
   retention gap 全量 resync 与慢客户端隔离；行情使用后端 WebSocket snapshot/sequence/gap 恢复。
9. **前端切换**：单例 AppShell/SSE、SWR 事实状态、全局 Kill Switch/stale 告警、精度与错误契约。
10. **回测修复**：复用成交/仓位账本，修复 realized PnL、mark-to-market、funding 和 look-ahead。
11. **testnet 验证**：故障注入、重启恢复、重复/乱序成交、Kill Switch、低动作额度和 14 天 soak。
12. **删除旧链路**：移除旧 NonceManager、内存订单事实源、EventBus 后写 Postgres 和旧 API。

切换开关：`durable_ledger_v2`、`execution_v2`、`user_stream_v2`、`reconciliation_v2`、`api_v1`、
`strategy_runner_v2`。所有开关默认关闭，按 dev shadow -> testnet -> mainnet 顺序启用。

实现约束：dev/testnet 配置可显式启用完整 V2 链用于验证；mainnet YAML 保持所有 V2 开关关闭。应用只有在
五个安全关键交易开关（durable ledger、execution、user stream、reconciliation、strategy runner）全部开启时
才初始化交易组件；不存在旧链路自动回退。API v1 采用 viewer/operator/admin RBAC，所有精确数值以 decimal
string 传输并按 instrument meta 做 tick/lot/min-size 校验。
