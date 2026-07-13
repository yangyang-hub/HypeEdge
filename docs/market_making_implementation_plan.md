# HypeEdge 做市系统实施计划

> 对应权威设计：`docs/design.md` §18 与 `docs/market_making_design.md`。
> 原则：先修安全事实链，再实现报价；先 shadow，再 testnet，再 mainnet canary；每次只扩大一个风险维度。

## 1. 范围与完成定义

本计划覆盖后端、Postgres、ClickHouse、API 和 Next.js 控制面重构。第一版完成定义：

- 单个 market-maker 实例可在独立 sub-account 上运行单 symbol、每侧单档 ALO 报价。
- stale market/user stream、低额度、UNKNOWN、重启、数据库故障和 Kill Switch 下可自动撤单并 fail closed。
- desired quote 与 authoritative live orders 通过 QuoteCoordinator 最小差异协调。
- batch 命令、风险预占、网络尝试和恢复事实持久化，不产生重复订单。
- 公平价、库存 skew、spread、size、毒性和动作影子成本均可解释、版本化、可 shadow。
- 前端可查看实时报价、库存、PnL 分解、动作 runway、配置 diff 和全部安全状态。

> 实现状态（2026-07-11）：P0–P7 代码、迁移、控制面与本地验证已完成；P8 故障注入和 P9
> canary/扩容门禁已实现为可执行检查。真实 14 天 shadow、14 天 clean testnet 与 30 天扩容观察属于
> 外部时间证据，当前发布门禁保持 `false`，不得据此宣称 mainnet 已获准。
- 通过 replay、故障注入、mainnet shadow、testnet 和极小资金 canary 门禁。

不在第一版范围：多交易所对冲、多档网格、黑箱 ML、公平价跨进程服务、自动购买额度维持日常做市、先行 Rust 重写。

## 2. 关键依赖顺序

```text
P0 行情/事件安全修复
  -> P1 Decimal + instrument normalization
  -> P2 统一交易命令与数据库扩展
  -> P3 batch execution + QuoteCoordinator
  -> P4 ActionBudget + 做市风控
  -> P5 策略模型 + shadow
  -> P6 研究/遥测/绩效归因
  -> P7 API + 前端控制面
  -> P8 testnet 故障与恢复验证
  -> P9 mainnet canary 与扩容
```

P5–P7 可在 P3/P4 接口冻结后并行，但任何真实 placement 必须等待 P0–P4 全部完成。

## 3. P0：安全前置修复

### 目标

消除现有行情新鲜度和可靠事件丢失风险，使系统具备“断流立即撤报价”的基础。

### 后端工作

1. 重构 `OrderBook/L2BookSnapshot`：update 时保存不可变 `exchange_ts`、`received_at`、`version`、connection id。
2. `get_snapshot()` 只返回当前事实，不生成新的本地时间。
3. 新增 `FeedHealth` 和 `DataHealthGate`：WS 断连、stale、gap、crossed/empty book、时间倒退、event-loop lag。
4. 应用消费 market/user-stream health；异常时停止 placement，权威撤全部 maker orders，进入 `CANCEL_ONLY`。
5. 重构 `StrategyRunner`：只订阅声明事件；L2 使用 latest-value/coalescing mailbox；订单/fill/safety 使用可靠独立队列。
6. authenticated stream 溢出、cursor 停滞、历史补缺失败时立即安全降级。
7. 定义 `AccountHealthProvider`，区分 inventory stream、2–5 秒可调 clearinghouse-state poll 和低频完整 reconciliation；
   equity/margin/liquidation 任一 stale 时禁止增加风险，user stream 断流不等待 REST poll 即撤报价。
8. 定义 `EmergencyCancelExecutor`：Postgres 不可用时进入 CANCEL_ONLY/HALTING，查询权威挂单，只经唯一签名出口撤单，
   并写 fsync append-only emergency WAL；数据库恢复后补录事实和完整对账。

### 测试

- 读取旧盘口不会刷新 `received_at`。
- WS 断连、空盘口、crossed book、时间倒退均触发撤报价。
- L2 洪峰不能丢 fill/order/cancel/safety 事件。
- user stream 溢出后不允许恢复 placement，直到补缺和对账完成。
- event-loop 延迟 watchdog 的阈值与恢复 hysteresis 测试。
- Postgres 故障、撤单中进程崩溃、emergency WAL 回放和 Kill Switch authoritative cancel-all 测试。
- inventory/account/user-stream/reconciliation 分层 freshness 测试。

### 退出门禁

- 故障注入下 stale book 不可能通过执行前新鲜度校验。
- reliable 事件零静默丢失；积压只允许显式降级。
- P0 相关核心逻辑覆盖率 >= 90%。

## 4. P1：Decimal 领域类型与合约规格归一化

### 目标

消除 float 进入交易边界造成的价格/数量舍入和数据库双口径。

### 后端工作

1. 将 `Price/Size/Usd/Fee/PnL` 迁移为基于 `Decimal` 的值对象或严格语义封装。
2. 新增 `InstrumentSpec`：tick、size decimals、有效价格位数、lot、minimum size/notional、max leverage。
3. 新增 `OrderNormalizer`，统一执行：解析 decimal string、量化、边界检查、ALO 不跨价校验。
4. Postgres NUMERIC 到领域模型保持 Decimal；仅研究计算/图表边界显式转 float。
5. 清理 legacy Float PnL/position snapshot 使用路径，账本成为唯一 PnL 口径。

### 迁移方式

- 先为新接口增加 Decimal 类型并提供兼容适配器。
- 按 market data -> core models -> risk -> execution -> account -> API 顺序迁移。
- 新旧类型不得同时进入签名路径；通过 feature flag 做单向切换。

### 测试与门禁

- tick/lot/minimum、边界舍入、极小数、极大数、负数和科学计数法测试。
- Python -> Postgres -> API -> Python round-trip 精确相等。
- 所有下单入口必须经过同一个 normalizer，策略和 API 无旁路。

## 5. P2：统一交易入口、策略注册与 Postgres 扩展

### 目标

把策略、API 和运维交易命令统一到持久化、可恢复、可审计的入口，并建立多策略实例和配置版本模型。

### 后端工作

1. 定义并实现 `TradingCommandService`：Safety -> DataHealth -> Risk -> ActionBudget -> Normalize -> durable transaction。
2. 策略不再注入 `ExecutionEngine`，改为 `QuoteCommandClient/TradingCommandClient`。
3. 实现 `StrategyRegistry/StrategySupervisor`，替代 `app.py` 硬编码单一 `TrendFollowStrategy`。
   多策略类型插件、前端可创建非做市实例、以及 Trend 迁入控制面的分期（P0–P2）见
   `docs/strategy_control_plane.md` §10 与 `docs/design.md` §19。
4. 支持 `STOPPED/WARMING/SHADOW/RUNNING/PAUSED/DRAINING/FAULTED` 生命周期。
   非做市类型按 capabilities 使用子集（如 trend：stopped/running/paused），不强制对外 shadow。
5. YAML 只保留环境安全默认和上限；实例参数以 Postgres 不可变配置版本为主，避免双主。

### Postgres migration 007+

采用 expand -> backfill -> read comparison -> fenced cutover -> contract：

1. 新增 `strategy_instances`、`strategy_allocations`、`strategy_config_versions`、`market_maker_config_versions`。
2. 新增 `strategy_runtime_state`、`strategy_state_events`、`market_making_sessions`。
3. 新增 `quote_plans`、`quote_plan_items`、`quote_slots`、`execution_command_items`、`execution_actions`。
4. 新增 `action_budget_scopes`、`action_budget_allocations`、`action_budget_events`；scope 使用真实 quota owner address。
5. 扩展 `risk_reservations` 为逐 command item/risk owner 预占，移除单 command 唯一限制。
6. 为现有 orders/fills/risk/ledger 的 `strategy_id` 建引用表并回填孤儿。
7. FK 先 `NOT VALID`，后台校验后再 validate；所有 FK 列显式建索引。
8. 为 runtime 热表设置合理 fillfactor 和 optimistic revision。
9. quote plan item 单向引用内部 `orders.order_id`，不增加反向循环 FK；cloid 仅作冗余审计。
10. 淘汰 legacy Float `position_snapshots/pnl` 读取路径。

迁移期间旧结构在 cutover 前仍是唯一写源。新表只能在同一 Postgres 事务中由旧命令派生写，禁止独立异步双写；
read comparison 只观测不影响控制。切换使用 feature flag、schema compatibility、单写者 lease/fence，旧/新 supervisor
和执行链绝不允许同时向交易所发单。

### 测试与门禁

- Alembic upgrade/downgrade、空库和现有数据回填测试。
- 配置版本不可变、hash 稳定、desired/effective config 语义、并发激活 revision 冲突测试。
- allocation 获取/释放并发测试；同一 `(sub_account,symbol)` 只能有一个活跃策略租约。
- 策略启停/恢复幂等；进程重启能恢复 actual/desired state。
- Postgres 不可用时任何策略 placement 数为零，撤单路径仍可运行。

## 6. P3：QuoteCoordinator 与 batch durable execution

### 目标

安全地把完整 desired quote set 变成最少的 place/cancel/modify 动作。

### 核心模型

- `QuoteSlot(strategy_id, symbol, side, level)`。
- `DesiredQuoteSet`、`DesiredQuote`、`NoQuoteReason`。
- `QuotePlan/QuoteRevision`。
- `QuoteDiffAction`: KEEP、PLACE、CANCEL、MODIFY、BLOCKED_UNKNOWN。
- `BatchExecutionCommand` 及 child action outcomes。

### 后端工作

1. QuoteCoordinator 从交易所权威订单投影构建 live/inflight/UNKNOWN slot 状态。
2. 实现 price/size hysteresis、min lifetime、cooldown、max quote age 和 revision fencing。
3. partial fill 后基于剩余 size 和库存重算，不机械补回。
4. 实现 batch place/cancel/modify adapter；不能 batch 时仍使用同一父命令和子结果模型。
5. durable worker 支持父命令租约、子动作结果、部分成功和逐子项 UNKNOWN 恢复。
6. 执行顺序按风险排序：先撤危险侧；旧撤单未确认前不得释放风险预占。
7. `execution_actions` 记录每次实际网络尝试、延迟、请求 hash、结果和额度估计。
8. Kill Switch/strategy pause 通过 QuoteCoordinator 权威撤销策略所属全部订单。
9. 每个 placement/modify child 发送前执行 dispatch-time guard，重新校验 deadline、connection/market revision、
   freshness、Safety/lifecycle、risk reservation、ALO；失败标记 superseded/expired/blocked，cancel 始终可发送。
10. 对迟到旧 revision、对账 orphan 和 modify UNKNOWN 建 `ORPHANED_LIVE/RECOVERY_REQUIRED`，计入最坏风险并优先撤销。
11. 若 modify 语义无法可靠恢复，第一版只实现 CANCEL_THEN_PLACE。

### 测试矩阵

- desired 不变 -> 0 action。
- 同 tick/小 size 差 -> KEEP。
- 单侧/双侧 replace，cancel-first 与 place-first 风险场景。
- batch 部分成功、timeout、迟到 ACK、乱序 child result、重复 result。
- cancel unknown 阻止同 slot 补挂。
- old revision response 不能覆盖新 slot。
- 重启时恢复父/子命令且不重复下单。
- queue 延迟过 deadline、WS 重连代次变化、盘口跳价、配置切换和 Kill Switch 的 dispatch guard 竞争测试。
- orphan 重启恢复、迟到 ACK、modify UNKNOWN 和 cancel/place 所有部分成功组合。
- 1000 次随机状态机/property 测试保持“每 slot 至多一个 current desired owner，所有可能 live risk owner 均被跟踪”。

### 退出门禁

- QuoteCoordinator 和 batch 状态机覆盖率 >= 95%。
- 故障注入下重复订单、漏撤单、风险预占提前释放均为 0。

## 7. P4：动作预算与做市专属风控

### ActionBudgetController

1. 分开管理 address actions、cancel headroom 和 IP weight，不能用一个 remaining 数值混算。
2. 按真实 quota owner address 自适应轮询 `userRateLimit`，维护远端快照新鲜度。
3. 每个实际到达网络边界的 child attempt shadow debit 一次；timeout/reject 不重复扣减，远端差分校正。
4. 统计 burn/earn、actions/fill、marginal USDC/action、1h/6h/24h runway；初始 grant/付费额度不算 earned volume。
5. 按策略/symbol 分配预算，在 scope 层只保留一次动态 cancel/close/IP emergency reserve。
6. 实现 NORMAL/CONSERVE/CRITICAL/CANCEL_ONLY/EXHAUSTED。
7. paid reserve 默认关闭；只支持 emergency policy，带单次/日/月硬上限和审计。
8. 当前投影只在命令 debit、模式/阈值变化和低频 checkpoint 更新；每次远端轮询样本写 ClickHouse。
9. 重启时从最后远端快照和其后 durable execution actions 重建 shadow remaining；无法解释差异时 CANCEL_ONLY。

### MarketMakerRiskGate

- inventory soft/hard/emergency bands。
- 全部 live/inflight/UNKNOWN/new quotes 的最坏成交场景。
- 每侧 notional、gross exposure、max order count。
- margin/liquidation buffer、leverage。
- market/account/user stream/action budget freshness。
- volatility/jump/toxicity circuit breaker。
- daily loss、drawdown、negative markout、forced flatten 成本。
- max quote age、reject/unknown、ACK/cancel latency、event-loop lag。
- funding settlement inventory policy。
- `AccountStatePoller` 正常 2–5 秒、接近 margin/inventory 阈值时 0.5–2 秒自适应轮询，并与 backfill IP 预算隔离。
- 逐 child/risk-owner reservation：分别计算 `worst_long=current+possible_buys` 和
  `worst_short=current-possible_sells`；父 batch 完成不整体释放 reservation。

### 测试与门禁

- 已撤未确认的旧单始终计入最坏风险。
- actions 估计偏差、远端额度跳变、低水位和 exhausted 测试。
- batch child 数与 IP request weight 分离、timeout 不重复 debit、cancel headroom 提前降级测试。
- cancel 永不被额度 gate 拒绝。
- paid reserve 超预算无法执行且触发告警。
- 所有风险维度使用已知边界输入/输出覆盖。

## 8. P5：做市策略与 shadow engine

### 纯计算组件

1. `MarketFeatureEngine`：microprice、L1/L5 OFI、trade flow、EWMA vol、toxicity、latency。
2. `FairValueModel`：可解释线性/分段模型，预测偏移硬封顶。
3. `InventoryController`：reservation price、soft/hard bands、reduce direction。
4. `SpreadModel`：markout、latency vol 和 signed fee 生成候选价格；`min_expected_pnl_usdc` 只在统一效用门槛应用一次。
5. `SizeModel`：inventory headroom、depth participation、vol/toxicity/budget scaling。
6. `MarketMakerPolicy`：以 USDC 为统一量纲，联合枚举 bid/ask/KEEP/NO_QUOTE 候选并输出 DesiredQuoteSet。
7. `ShadowDecisionSink`：记录候选、NO_QUOTE、touch 和 markout，不声称精确 fill probability/action burn。
8. `ShadowExecutionSimulator`：维护虚拟 submitted/resting/partial/fill/cancel/UNKNOWN、queue/latency 情景，并通过
   `OrderStateView` 向 QuoteCoordinator 提供可用于 diff 的 shadow 状态；虚拟事实只写研究表。

### 配置

- 新增 `MarketMakingSettings`，只存全局安全上限和 polling/retention 默认。
- 三环境 YAML 同步；mainnet `market_making_enabled=false`。
- 策略实例参数存在 Postgres typed config version，支持 validate/diff/activate/rollback。

### 测试

- 公平价和 skew 符号方向测试。
- 长库存时 bid 更保守、ask 更积极；触硬限额后不增加库存。
- fee/markout/action cost 不足时返回 NO_QUOTE。
- 所有效用项统一为 USDC，size 变化和 fee 正负号符合模型契约，min expected PnL 只扣一次。
- bid/ask 联合选择满足共享库存和动作预算；Coordinator 与 BudgetController 共用同一 child 动作计费函数。
- 相同库存名义比例在不同价格币种产生一致 bps skew，方差/horizon 单位转换一致，shift 受硬封顶。
- tick rounding 后保持 ALO，不跨价。
- 参数扰动与 property-based invariants。
- 同一输入和 config version 产生确定性输出。

## 9. P6：ClickHouse、回放与绩效归因

### ClickHouse DDL

新增 `mm_feature_samples`、`mm_quote_decisions`、`mm_inventory_samples`、`mm_action_credit_samples`、
`mm_fill_markouts`，同步更新 `storage/clickhouse.py` 和 `docs/design.md` §5.2。

### 研究与回放

1. event-time deterministic replay。
2. queue-ahead 乐观/中性/悲观模型。
3. 使用实测 receipt->decision->send->ACK 延迟分布。
4. 建模部分成交、取消、同价新增、fee/rebate、funding、flatten 和 paid action。
5. markout worker 在 1s/5s/30s 生成不可变分析事实。

### PnL 归因

分开输出：

- Accounting PnL：realized trading、unrealized inventory change、net fee/rebate、funding、paid action，与 PG ledger 严格相等。
- Execution Quality：quoted/realized spread、1s/5s/30s markout、queue/fill diagnostics，不再次计入会计 PnL。
- Inventory Episodes：互斥的 fill-to-markout 与 residual/close 归因，必须可重组为 Accounting PnL。

禁止用总账户权益变化替代策略做市收益。财务、风控和 Kill Switch 始终读取 Postgres ledger/fill/funding/paid-action
事实；ClickHouse 只保存可重建分析投影，不得成为控制事实源。

### 门禁

- replay 可完全复现同一输入下的 quote decisions。
- 悲观模型和 shadow 数据都能输出可审计的 edge 分解。
- 每日/session 归因项以 Decimal 精确重组为 ledger PnL；markout 报表变化不改变 Accounting PnL。
- partial fill、跨 funding/UTC 日、持仓未平和 forced flatten 的对账恒等式测试通过。
- ClickHouse 写入失败不阻塞交易热路径，使用现有 spool/retry 语义。

## 10. P7：API 与 Next.js 做市控制面

### API

- 多策略实例 create/query/update-metadata/archive；有交易历史后禁止硬删除。
- start/pause/resume/drain/stop lifecycle actions。
- config version create/list/diff/activate/rollback。
- state/quotes/inventory/performance/action-budget 查询。
- durable SSE：state、config、risk、budget、reconciliation、alert。
- market WebSocket：fair、desired/live quotes、inventory samples，latest-value 丢弃中间状态。

所有 mutation 使用 RBAC、CSRF、Idempotency-Key、If-Match revision 和 api_audit。

### 前端

1. 将 `StrategyData` 改为 `strategy_type` discriminated union。
2. 新增 `/strategy/[id]/market-making`：Overview、Live Quotes、Inventory、PnL、Action Budget、Config、Events。
3. 风控页增加库存带、quote age、unknown/reject、burn/runway。
4. 订单页增加 strategy、quote revision/side/level、maker/taker。
5. dangerous config 二阶段确认；mainnet 明显标识；所有数据展示 last updated/stale。
6. 高频表虚拟滚动；图表降采样；API 错误统一 toast。
7. 首次加载/重连通过 REST 获取 PG 权威 position/runtime/budget；WS inventory 只用于显示增量，revision gap/stale 时 resync。
8. performance 响应显示 `as_of/stale/source`；ClickHouse 不可用时控制功能保持可用。

### 测试

- TypeScript strict、API contract、MSW hook 测试。
- lifecycle/config 权限和幂等交互。
- SSE replay/resync、WS gap/stale、慢客户端隔离。
- kill switch/stale 全局红色横幅和键盘可操作性。

## 11. P8：验证环境与故障注入

### 历史与 shadow

- 历史 replay 完整运行。
- mainnet shadow 至少 14 个完整 UTC 日，覆盖不同波动/流量/funding regime；shadow 是候选淘汰，不是盈利证明。
- 记录 NO_QUOTE 原因、候选报价 markout 和 projected action runway。
- 预注册统计分析计划、inventory episode 定义、regime coverage 和 canary risk envelope。

### Testnet

验证 batch/partial result、nonce、UNKNOWN、重启、WS/user stream 断连、Postgres/ClickHouse 故障、额度低水位、
Kill Switch、pause/drain、配置切换和完整对账。

testnet 必须连续至少 14 天无重复订单、未解释仓位差异或风控绕过，才可进入 mainnet canary。

### 必须通过的故障场景

- submit/cancel response 丢失和迟到。
- worker 在父命令或子动作中途崩溃。
- user fill 重复、乱序、队列溢出、REST 补缺。
- L2 stale/gap/crossed、event-loop lag。
- Postgres transaction/connection failure。
- ClickHouse 不可用和 spool 恢复。
- 动作额度远端值与 shadow ledger 不一致。
- address action/cancel headroom/IP weight 三预算分别不足和恢复。
- Kill Switch 在双侧 live、partial fill、UNKNOWN 状态触发。
- EmergencyCancelExecutor 在 Postgres 不可用和进程中途崩溃后的 WAL 回放。
- 迟到 revision/orphan/modify UNKNOWN 超 SLA 时进入 CANCEL_ONLY/FAULTED。

任何关键失败均应落 durable risk/reconciliation/execution fact 并产生告警。

## 12. P9：Mainnet canary 与逐级扩容

### Canary 起点

- 一个经 shadow 数据选择的 symbol。
- 一个 isolated sub-account。
- 每侧单档、最小合规 size、1x 或极低杠杆。
- hard inventory notional <= equity 10%–15%。
- paid reserve 与自动 taker flatten 默认关闭。
- 人工在线监控，预设自动 pause/cancel/halt 阈值。
- 激活版本化 `CanaryRiskEnvelope`：部署权益、quote notional、日/累计亏损、日/总动作、最低 credits/cancel headroom、
  forced flatten、UNKNOWN SLA、最长时间/成交量和自动 PAUSED/CANCEL_ONLY/HALTED 条件。

### 扩容顺序

1. 只增加 quote size。
2. 再增加第二 symbol；默认新 strategy instance + 独立 sub-account，共享账户需先单独设计联合库存/额度/nonce 风险。
3. 最后评估第二档。
4. Rust 优化独立于风险扩容，不得与 size/symbol 同时变更。

每一步至少完成一个独立观察窗口，只允许变更一个维度，并保留一键回退到上一配置版本。

### 扩容门禁

- 同时满足至少 30 个完整 UTC 交易日、预注册最小独立 inventory episode/有效交易日数和 regime coverage。
- 按交易日或 inventory episode 做 block bootstrap，Accounting net edge 的 95% CI 下界 > 0。
- trailing marginal USDC/action >= 1.25，remote balance/cancel headroom 高于动态 reserve，runway 覆盖下一观察窗口。
- hard inventory breach、重复订单、关键 reconciliation diff = 0；UNKNOWN/orphan 均在 SLA 内有审计终态。
- 收益不由少量方向性库存暴露主导。

## 13. 可观测性与告警清单

Prometheus/Grafana 至少包含：

- feed/user stream/account/credit freshness。
- fair/reservation、desired/live quote、quote age 和 quote uptime。
- inventory band、margin、liquidation distance、funding carry。
- action credits、burn/earn、USDC/action、runway、emergency reserve。
- submit/cancel/modify/reject/unknown、batch partial success。
- receipt-to-decision、decision-to-send、ACK/cancel latency、event-loop lag。
- 1s/5s/30s markout 和 PnL decomposition。
- reconciliation diff、strategy state、config version。

P0 告警：stale/gap、user stream loss、hard inventory、margin/liquidation、unknown 超 SLA、撤全单失败、Postgres 不可用、
runway 低于 emergency reserve、Kill Switch。

## 14. 代码落点

建议目录：

```text
src/hypeedge/
  market_data/
    market_state.py
    features.py
  strategy/
    supervisor.py
    registry.py
    market_maker/
      models.py
      fair_value.py
      volatility.py
      toxicity.py
      inventory.py
      spread.py
      sizing.py
      policy.py
  trading/
    commands.py
    command_service.py
    quote_coordinator.py
  risk/
    market_maker.py
    action_budget.py
  execution/
    batch.py
    normalizer.py
    recovery.py
  storage/
    postgres.py
    clickhouse.py
  api/routes/
    strategies.py
    market_making.py

web/
  app/strategy/[id]/market-making/
  components/market-making/
  hooks/use-market-making-*.ts
```

`trading/` 是命令编排边界，不直接签名；`execution/` 仍是唯一签名出口。

## 15. 工期与交付策略

在 V2 安全链路已稳定的前提下，单名熟悉代码库的工程师预计 12–18 周完成到 mainnet canary；多人可并行 P5–P7，
但 P0–P4 的安全依赖不能压缩为并行上线。交付应拆成可回滚的小 PR/commit，每个 PR 只完成一个接口、迁移或安全不变量。

若时间受限，必须缩小 symbol、档位和 UI 范围，不能跳过 P0–P4、shadow 或故障注入。
