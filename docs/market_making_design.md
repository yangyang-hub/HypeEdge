# HypeEdge 盘口做市系统设计

> 状态：权威设计扩展。本文受 `docs/design.md` 约束，并由其中 §18 正式引用。
> 目标场景：Hyperliquid、单所、小资金、地址动作额度受限、Python asyncio 单进程模块化单体。

## 1. 设计结论

HypeEdge 的第一版做市不采用“固定点差 + 每次盘口变化都撤挂”的传统方案，而采用：

- 单币种、独立 isolated sub-account、每侧单档 ALO/post-only 报价。
- 事件驱动但合并行情更新；是否刷新由净预期收益决定，而不是由固定时钟决定。
- 公平价、短窗波动率、毒性、库存和 funding 共同决定报价。
- `NO_QUOTE` 是正常决策；没有足够净边际时宁可不挂单。
- 地址动作额度被视为有价格的稀缺资本，为撤单、UNKNOWN 恢复和紧急减仓保留硬额度。
- 策略只产生 `DesiredQuoteSet`，不直接逐单调用执行引擎。
- `QuoteCoordinator` 负责 desired quotes 与交易所权威挂单的最小差异协调。
- 交易命令、风险预占和恢复状态写入 Postgres；高频模型与报价遥测写入 ClickHouse。
- L2 回放只能淘汰坏方案，不能证明收益；盈利验证必须经过 mainnet shadow 和极小资金 canary。

该设计优化的是“风险和动作成本约束下的长期净收益”，不是刷新速度、成交量或 maker ratio 单项最大化。

## 2. 优化目标与硬约束

报价优化统一使用“一个 quote lifecycle 内的增量预期 USDC PnL”口径。bid、ask、KEEP 和 `NO_QUOTE`
作为完整 QuoteSet 联合求解，不能忽略共享库存与动作预算后逐侧独立最大化：

```text
scenario_pnl_usdc(omega) =
    filled_notional(omega)
    * (side_adjusted_fair_move_rate
       - signed_fee_rate
       - expected_flatten_rate
       - expected_funding_rate)
    - incremental_action_cost_usdc(omega)
    - execution_failure_tail_cost_usdc(omega)

expected_utility_usdc =
    sum(probability(omega) * scenario_pnl_usdc(omega))
    - inventory_variance_penalty_usdc
```

只有 `expected_utility_usdc > min_expected_pnl_usdc` 时才允许产生增加动作或风险的报价；KEEP/NO_QUOTE 的效用必须
参与同一比较。概率、horizon、波动率口径、fee 正负号和金额单位属于版本化模型契约。优化器必须满足：

1. 全部 live、inflight、UNKNOWN 和新报价按最坏方向成交后仍不越硬库存限额。
2. 普通报价只使用 ALO/post-only，不得跨价成为 taker。
3. 价格和数量经过统一 `OrderNormalizer`，满足 tick、有效数字、lot、minimum size/notional。
4. 行情、账户、authenticated user stream、动作额度和 Postgres 任一不新鲜时不得增加风险。
5. 撤单不受 placement gate、动作低水位或 Kill Switch 阻断。
6. 分别为 exchange child action、撤单累计 headroom、UNKNOWN/对账 info 请求和受控减仓预留额度。
7. 参数预测只能有限移动公平价，不得把做市隐式变成无上限方向策略。

动作影子成本至少包含付费额度的显式价格，并加入额度枯竭造成停摆的机会成本。maker rebate
不能单独构成报价理由。

## 3. 总体架构

```text
Hyperliquid WS/REST
        |
        v
VersionedMarketState --------------------------+
  book/trades/context/feed health              |
        | latest-value snapshot                |
        v                                      |
MarketFeatureEngine                            |
  microprice/OFI/flow/volatility/toxicity      |
        |                                      |
        v                                      |
MarketMakerPolicy                              |
  fair value/inventory/spread/size/NO_QUOTE    |
        | DesiredQuoteSet                      |
        v                                      |
QuoteCoordinator <----- authoritative orders -+
  quote slots/minimal diff/revision/watchdog
        | QuoteSetCommand
        v
TradingCommandService
  SafetyController
  DataHealthGate
  MarketMakerRiskGate + risk reservation
  ActionBudgetController
  OrderNormalizer
  durable Postgres transaction
        |
        v
SignedActionExecutor -> batch place/cancel/modify -> Hyperliquid
        |
        v
ExchangeEventIngestor -> Postgres facts/outbox -> reliable EventBus/SSE

High-frequency features/decisions/markouts -> ClickHouse
Metrics/alerts                            -> Prometheus + alerting
```

### 3.1 模块边界

新增接口必须先于实现：

- `VersionedMarketDataProvider`：返回不可变行情版本、交易所时间、接收时间和健康状态。
- `MarketFeatureProvider`：返回同一行情版本上的公平价特征、波动率和毒性。
- `MarketMakerPolicy`：纯计算，输入市场、库存、quote-slot snapshot、额度和配置，输出候选 `DesiredQuoteSet`。
- `QuoteCommandClient`：提交完整报价集合，不暴露 `ExecutionEngine`。
- `QuoteCoordinator`：报价槽所有者，解释实际订单状态、计算真实 diff 动作成本，并在候选 PLACE/REPLACE、KEEP、
  NO_ACTION 中选择增量效用最高的可行计划。
- `ActionBudgetController`：动作 shadow debit、远端校正、预算状态和刷新许可。
- `MarketMakerRiskGate`：按报价集合做最坏场景风控，而不是仅逐单检查。
- `StrategySupervisor`：管理多策略实例、配置版本、生命周期和健康状态。

策略计算必须无网络和数据库 I/O。I/O 只存在于行情、命令、执行、事实写入和遥测写入层。

### 3.2 事件隔离

- L2、trade、feature 更新是 lossy/latest-value：EventBus 只通知“版本已更新”，消费者读取最新快照。
- fill、order、risk、safety、config 和 operator command 是 reliable：使用独立有界队列及 Postgres inbox/outbox。
- 策略不得使用 `subscribe_all()` 消费混合队列，避免 L2 洪峰挤掉成交或撤单事件。
- reliable 队列积压、authenticated stream 溢出或 cursor 停滞时立即进入 `CANCEL_ONLY`，补缺和完整对账成功后才恢复。

## 4. 行情状态与特征

### 4.1 行情新鲜度

每个盘口快照必须在 update 时一次性保存：

- `exchange_ts`：交易所事件时间。
- `received_at`：本进程接收时间。
- `version`：按 symbol 单调递增版本。
- `sequence/gap_state`：若上游提供 sequence，则记录连续性；否则使用重连/快照代次。
- `source_connection_id`：区分重连前后的消息。

读取快照不得改变任何时间字段。以下任一条件触发报价撤销和数据健康降级：

- WS 断连或超过 stale threshold 未收到更新。
- 空盘口、交叉盘口、时间倒退、异常大跳价。
- 交易所时间与本地接收时间差超过阈值。
- authenticated user stream 断连、溢出或补缺失败。
- event-loop lag 或 receipt-to-decision 延迟超过安全阈值。

账户健康分层维护：inventory 由 authenticated fill/order stream 驱动；equity、available margin 和 liquidation 数据由
独立 clearinghouse-state poller 正常 2–5 秒、接近风险阈值时 0.5–2 秒刷新；完整 reconciliation 仍低频执行。
任一关键维度 stale 都禁止增加风险，user stream 断流不能等待 REST poll 才撤报价。

### 4.2 第一版特征模型

```text
mid = (best_bid + best_ask) / 2
microprice = (best_ask * best_bid_size + best_bid * best_ask_size)
             / (best_bid_size + best_ask_size)

fair = mid
     + beta_micro * bounded(microprice - mid)
     + beta_ofi * bounded(normalized_order_flow_imbalance)
     + beta_flow * bounded(short_horizon_trade_flow)
     + beta_return * bounded(short_return)
     + funding_carry_adjustment
```

- OFI 同时使用 L1 和前 3–5 档，对瞬时撤单和深度异常做 winsorize。
- 短期预测对公平价的最大影响限制在可配置的少量 ticks。
- mark/index/oracle 用于异常和风险校验，不直接替代可成交盘口公平价。
- 波动率至少维护 1s、5s、30s、5min EWMA，并同时监测跳价和深度塌陷。
- 毒性综合主动成交方向、OFI、连续扫单、成交后 markout、延迟和价差状态。

第一版坚持可解释的低参数模型。黑箱模型只有在产生稳定样本外增益、可回退且不破坏安全边界时才可作为附加项。

### 4.3 外部参考价

Hyperliquid 本地可成交盘口始终是做市公平价的主锚。Binance 等外部市场只能作为有界的领先价格、异常检测和撤单参考，
不得被称为或用作 Hyperliquid oracle，也不得绕过 Hyperliquid mark/index/oracle、账户健康或本地盘口门禁。股票价格仅在标的
经济上直接相关且交易时段、汇率、公司行为和现货/合约基差均已建模时才可启用；对普通加密永续只能作为低权重风险特征。

外部参考价必须先转换到同一合约、计价币与时间基准，并用稳健 EWMA 校正跨市场基差：

```text
adjusted_external = external_mid * exp(ewma(log(hl_mid / external_mid)))
external_signal = clamp(adjusted_external - hl_microprice, +/- external_signal_cap)
fair = local_fair + effective_external_weight * external_signal
```

- `effective_external_weight = configured_weight * confidence * freshness_decay`，范围硬限制在 `[0, max_external_weight]`。
- confidence 至少考虑 source 连接、bid/ask 完整性、时间倒退、价差、跳价、basis 稳定性和多源一致性；数据 stale 时权重必须
  单调衰减到 0，未知或非法质量直接为 0。
- `external_signal_cap` 使用 ticks/bps 双重上限。即使外部价格显著偏离，也只能有限移动 fair；超过 divergence/basis 阈值时应
  扩点差、停止增加风险或撤单，而不是追随外部价格穿越本地盘口。
- 任何外部 source 故障都不得阻塞本地行情更新、可靠交易事实或撤单。未配置、断流、stale、基差失稳时退回纯 HL 本地模型；
  若版本化配置明确声明外部参考为 placement 必需，则 fail closed 为 `NO_QUOTE/CANCEL_ONLY`。
- 每次决策记录 source、raw/adjusted price、basis bps、confidence、configured/effective weight、age、quality、cap 是否命中和
  本地/外部时间戳；精确价格继续使用 Decimal，Prometheus 浮点投影不得回流交易计算。

是否利用外部市场的领先关系必须由 shadow/replay 的 lead-lag 分布决定，而不是按交易所名义假设。外部信号的有效半衰期必须
显著大于端到端 `external receipt -> decision -> durable command -> sign -> Hyperliquid ACK` 的 p99；仅做风险锚时
100--500ms 仍可能有用，改善秒级 fair 通常要求约 20--100ms，依赖短期领先关系通常要求低于 10--30ms。未达到该门槛时
只允许把外部价用于降权、扩点差和撤单，不得提高方向性报价权重。

## 5. 库存、报价与数量

### 5.1 库存保留价

```text
signed_inventory_notional = authoritative_position_size * fair
z = clamp(signed_inventory_notional / soft_inventory_notional, -z_cap, z_cap)

inventory_shift_bps =
    k_inventory_bps * z
    + gamma_bps * z * return_variance_per_second * horizon_seconds

reservation_price = fair * (1 - inventory_shift_bps / 10_000)
```

`return_variance_per_second` 使用无量纲 log-return 方差，horizon 单位必须与其一致；shift 受
`max_inventory_shift_bps` 硬封顶。soft limit 非正、fair 非法或账户权益非正时 fail closed。

- 长库存时整体报价下移，使 ask 更积极、bid 更保守；短库存反之。
- 触及 soft limit 后停止增加该方向库存，并提高减仓侧优先级。
- 接近 hard limit 时只允许降低绝对库存的报价或受控 reduce-only 减仓。
- 触及 emergency limit、margin buffer 或 liquidation distance 阈值时撤全单并进入受控减仓/HALTED 流程。

账户级当前仓位仍以 Postgres `positions(sub_account, symbol)` 为唯一事务投影。做市模块不得维护第二套权威当前仓位；
策略库存通过专属 sub-account 与成交/账本事实归因。

### 5.2 半点差

```text
minimum_half_spread = max(
    one_tick,
    expected_adverse_markout
    + latency_volatility_buffer
    + signed_fee_adjustment
)
```

该半点差只用于生成候选价格，不再次包含 `min_expected_pnl_usdc`。最终 bid/ask 叠加库存和方向毒性调整，
向盘口外侧按 tick 量化；QuoteCoordinator 再按真实 PLACE/CANCEL/MODIFY diff 扣除 transition action/failure cost。
自然价差不足以覆盖成本时选择 KEEP/NO_QUOTE，不得为了 quote uptime 强行贴盘。

### 5.3 数量

每侧 size 取下列上限的最小值：

- 单笔/单侧风险上限。
- soft/hard inventory 剩余空间。
- live、inflight、UNKNOWN 和新报价最坏成交后的 headroom。
- 可见前几档深度的低参与比例。
- 波动率反比缩放和毒性缩放。
- 动作预算状态缩放。
- instrument minimum size/notional。

第一阶段只允许单 symbol、每侧一个 quote slot。第二档或第二 symbol 只能在动作可持续性和净 edge 门禁通过后逐项开启。

## 6. Quote lifecycle 与执行

### 6.1 DesiredQuoteSet

`DesiredQuoteSet` 至少包含：

- `strategy_id`、`session_id`、`config_version_id`、`market_version`、`revision`。
- fair/reservation price、inventory、volatility、toxicity、budget mode。
- bid/ask 的 desired price、size、level、gross edge before actions 或 `NO_QUOTE reason`。
- current slot revision、生成时间、candidate validity deadline 和模型版本。

### 6.2 最小差异协调

`QuoteCoordinator` 按 `(strategy_id, symbol, side, level)` 管理 quote slot：

- 同 tick 且 size 差异小于阈值：保持原单。
- 原单仍有正 edge 且未过最大年龄：保持原单。
- 只有旧单 edge 转负、库存跨带、风险状态变化、关键 fill 或参数切换时才替换。
- 同侧存在 `SUBMIT_UNKNOWN` 或 `CANCEL_UNKNOWN` 时禁止补挂。
- partial fill 后按剩余 size、库存和 edge 决定保持、缩量或撤销，不自动恢复到原始数量。
- 风险恶化时先撤增加库存的一侧，再考虑新的减仓报价。
- 不常规使用 `expiresAfter`，避免过期动作惩罚。
- 对每个候选 transition 计算 `estimated_incremental_actions`、action shadow cost、failure tail cost 和
  `net_incremental_utility`；若 REPLACE 不优于 KEEP/NO_ACTION，则保留旧单。
- batch 只降低 IP request weight，不把 N 个 exchange child action 错算成一个地址动作。

刷新抑制由三层组成：

- `min_quote_lifetime`：正常报价的最短存活时间。
- `refresh_cooldown`：合并连续行情事件。
- hysteresis：替换阈值高于保持阈值。

保护性撤单不受以上限制。

### 6.3 Durable batch command

一个 quote revision 以父 `QuoteSetCommand` 和子动作持久化：

1. 在同一 Postgres 事务中写 quote plan、风险决策/预占、execution command、子动作和 outbox。
2. 每个 placement/modify child 在签名发送前执行 dispatch-time guard：command/session/config/revision 仍 active、
   deadline 未过、连接代次和 market lag 合法、行情/账户/user stream/额度/Postgres 新鲜、Safety/策略状态允许、
   风险预占仍有效、量化后仍是 ALO。失败则标记 SUPERSEDED/EXPIRED/BLOCKED，不发送；cancel 不受该 guard 阻断。
3. SignedActionExecutor 优先使用交易所 batch place/cancel/modify 能力；每个子结果单独落事实。
4. 交易所不保证原子时，执行顺序由风险决定：先撤危险侧；旧单未确认撤销时不得假设风险已释放。
5. 网络尝试写入不可变 `execution_actions`，与订单业务状态分离。
6. timeout 进入 UNKNOWN，按 cloid/oid 查询权威状态，禁止盲目重发。
7. 旧 revision 的迟到成功、对账发现的未归属 live order 或 modify UNKNOWN 进入 `ORPHANED_LIVE/RECOVERY_REQUIRED`：
   持久化、计入最坏风险、优先撤销，并在确认前阻止同 symbol/side 新增风险。
8. 第一版若 modify 无法提供足够幂等和可判定语义，则禁用 MODIFY，统一使用可恢复的 CANCEL_THEN_PLACE。

Placement、modify 和增加风险的命令必须先提交 Postgres。Postgres 不可用时，cancel/cancel-all 切换到严格受限的
`EmergencyCancelExecutor`：进入 CANCEL_ONLY/HALTING，查询权威挂单，只允许撤单，经唯一签名出口发送，并先写本地
fsync append-only emergency WAL；Postgres 恢复后补录 execution/order/reconciliation/audit 事实，完整对账前不得恢复。

## 7. 动作额度控制

现有二元低水位判断升级为 `ActionBudgetController`，分别管理不可混用的三类预算：

- `address_action_budget`：place/cancel/modify/close 等 exchange child action。
- `cancel_headroom`：交易所独立撤单累计限制和动态撤全单余量。
- `ip_weight_budget`：userRateLimit、orderStatus、权威挂单查询、UNKNOWN 和对账 info 请求。

- 以 `userRateLimit` 为交易所权威快照，正常 10–30 秒查询，接近阈值时自适应加快。
- 每个实际到达网络边界的 child attempt 保守 shadow debit 一次；rejected/timeout 不重复 debit，不确定是否到达时保持
  debit，直到远端差分校正。batch 的地址额度按 child，IP 权重按请求公式计算。
- 维护 1h/6h/24h burn rate、成交量 earned rate、actions/fill、USDC/action 和预计 runway。
- 动态计算 `required_cancel_reserve = possible_live_orders + retry_buffer`，并分别为受控减仓和 info 恢复保留额度。
- 为每个策略/symbol 分配预算，并向报价优化器提供动态动作影子成本和刷新许可。

预算状态：

```text
NORMAL      -> 双边单档
CONSERVE    -> 扩点差、缩量、延长寿命、仅保留高价值报价
CRITICAL    -> 禁止增加风险，只允许撤单和降低库存
CANCEL_ONLY -> 只撤不下
EXHAUSTED   -> 仅保留交易所仍允许的恢复/紧急路径
```

长期可持续的必要条件是 trailing 成交量覆盖全部 place/cancel/modify/reject/timeout child actions。生产扩容门槛默认要求
边际 `USDC/action >= 1.25`，给规则变化和估算误差保留余量；初始 grant 和付费购买不计 earned volume，阈值由
远端 cap/used 差分校准，并同时满足 cancel headroom 与 IP emergency reserve。

`reserveRequestWeight` 默认关闭，只能用于退出风险或解决 UNKNOWN，不用于维持日常报价；必须配置单次、每日和月度成本上限，
由 admin 显式授权，并计入策略净 PnL。

## 8. 做市专属风控与状态机

### 8.1 策略生命周期

```text
STOPPED -> WARMING -> SHADOW -> RUNNING -> DRAINING -> STOPPED
                       |          |            |
                       +-------> PAUSED <------+
                                  |
                                FAULTED
```

- `WARMING`：等待 instrument meta、行情、账户、user stream、额度和对账全部新鲜。
- `SHADOW`：计算并记录报价，不发送交易命令。
- `RUNNING`：允许通过全部门禁的报价。
- `PAUSED`：撤全部策略订单，保持库存但不新增风险。
- `DRAINING`：撤报价并按限制逐步回到目标库存。
- `FAULTED`：需要人工或完整恢复流程，不自动回到 RUNNING。

系统 `SafetyController` 状态高于策略生命周期，任何系统级 `CANCEL_ONLY/HALTING/HALTED` 都覆盖策略状态。

### 8.2 风控维度

- soft/hard/emergency inventory bands。
- 单侧 quote notional、gross exposure、最大 live/inflight/UNKNOWN 数量。
- 最坏成交风险预占，包括尚未权威确认撤销的旧单。
- margin headroom、liquidation distance、最大杠杆。
- 行情、账户、user stream、action credits 和对账 freshness。
- 短窗波动、跳价、盘口异常和流量毒性熔断。
- 日内亏损、峰值回撤、连续负 markout 和 forced flatten 成本。
- 最大 quote age、ACK/cancel 延迟、reject/unknown 比例、event-loop lag。
- funding 结算前后的方向库存和持有成本限制。

降级顺序固定为：

```text
双边报价
-> 扩点差/缩量/延长寿命
-> 单边仅降低库存
-> CANCEL_ONLY
-> 权威撤全单并对账
-> 必要时受控 reduce-only/IOC 平仓
-> HALTED
```

## 9. 数据库与数据归属

### 9.1 Postgres：事务事实与恢复状态

保留 V2 的 orders、order_events、fills、positions、account_state、system_state、ledger、risk、command、
inbox/outbox 和 reconciliation 表。新增 schema 通过 Alembic expand/backfill/read-comparison/fenced-cutover/contract 迁移，
cutover 前旧链路保持唯一写者，禁止旧/新 supervisor 或执行链同时发单。

推荐新增：

| 表 | 关键字段与约束 | 用途 |
|---|---|---|
| `strategy_instances` | `strategy_id TEXT PK`、type、sub_account、symbol、desired_state、desired_config_version_id、archived_at、created_at | 策略实例注册表；有交易历史后只允许 archive，不硬删除 |
| `strategy_allocations` | strategy FK/UNIQUE、sub_account、symbol、allocated_at；`UNIQUE NULLS NOT DISTINCT(sub_account,symbol)` | 活跃实例的账户/币种排他租约；生命周期事务中获取/释放，禁止跨 runtime 表推导唯一性 |
| `strategy_config_versions` | identity PK、strategy FK、version、config_hash、created_by、created_at；`UNIQUE(strategy_id,version)` | 不可变配置版本元数据 |
| `market_maker_config_versions` | config version PK/FK；库存、spread、size、toxicity、freshness、budget 等 typed columns；`NUMERIC(38,18)` | 做市强类型配置 |
| `strategy_runtime_state` | strategy PK/FK、actual_state、effective_config_version_id、heartbeat、revision、reason；`fillfactor=90` | 可恢复热状态；effective 表示运行进程已在安全点应用的版本 |
| `market_making_sessions` | identity PK、strategy/config FK、mode、started/ended、stop_reason | shadow/testnet/mainnet 会话边界 |
| `quote_plans` | UUID PK、strategy/session/config FK、revision、market_version、fair/reservation、inventory、budget_mode、status、created_at | 只保存产生 durable exchange command、风险预占变化或 UNKNOWN/恢复边界的计划 |
| `quote_plan_items` | identity PK、plan FK、slot key、decision、source/target internal order FK、desired price/size、ordinal；`UNIQUE(plan,ordinal)` | 可表达同 slot 的 cancel old + place new；普通 KEEP/NO_QUOTE 只进 ClickHouse |
| `quote_slots` | identity PK、strategy/symbol/side/level UNIQUE、owner order/plan revision、state、updated_at、revision | 当前 slot 投影；desired owner 与所有可能 live risk owner 分开跟踪 |
| `execution_command_items` | identity PK、command/plan-item FK、ordinal、action type、source/target order、status、resolution/lease/attempt；`UNIQUE(command,ordinal)` | 父 batch 下可恢复的持久子命令 |
| `execution_actions` | identity PK、command item FK、attempt、action_type、request_hash、sent/responded_at、outcome、estimated/reconciled credit cost | 不可变网络尝试事实；`UNIQUE(command_item,attempt)` |
| `action_budget_scopes` | quota owner address PK、remote cap/used/remaining、shadow used、emergency reserve、mode、observed_at、revision | 地址级唯一预算事实，不假定每个 sub-account 独享额度 |
| `action_budget_allocations` | identity PK、quota scope FK、strategy FK、symbol、soft/hard allocation、status | 策略预算分配，emergency reserve 只能在 scope 层保留一次 |
| `action_budget_events` | identity PK、quota scope/strategy/command item FK、event type、estimated delta、remote before/after、created_at | shadow debit、远端校正、paid reserve 和人工调整事实 |
| `strategy_state_events` | identity PK、strategy FK、from/to/reason、actor、created_at | 生命周期审计 |

设计约束：

- 价格、数量、费用、PnL 使用 `NUMERIC(38,18)`；时间使用 `TIMESTAMPTZ`。
- 所有 FK 列显式建索引；核心列 `NOT NULL`，业务状态使用 TEXT + CHECK，避免难迁移的数据库 enum。
- 高频每次模型计算不得写 Postgres；只有命令、状态变化和恢复所需事实进入事务库。
- 普通 KEEP、NO_QUOTE 和未产生风险预占变化的 desired revision 只能写 ClickHouse，不得以“审计”为由落 Postgres。
- `positions` 继续是账户级唯一当前仓位投影，不新增“策略当前仓位”双主表。
- `fills` 保持不可变；mid-at-fill 和 1s/5s/30s markout 写分析事实，不回写 fill。
- 财务、风控和 Kill Switch 使用 Postgres ledger/fill/funding/paid-action 事实；ClickHouse PnL 只是可重建分析投影，
  不得成为控制决策事实源。funding 与 paid action cost 必须先入 Postgres ledger/execution fact，再异步投影到 ClickHouse。
- 当前松散 `strategy_id TEXT` 迁移到 FK 前先建立引用表、回填孤儿值，再 `NOT VALID`/validate constraint。
- 淘汰仍使用 Float 的 legacy `position_snapshots/pnl` 口径，PnL 统一来自 ledger/fills/positions。
- 配置版本自身没有 mutable `is_active`。`strategy_instances.desired_config_version_id` 是操作员期望版本，
  `strategy_runtime_state.effective_config_version_id` 是运行进程在安全点确认应用的版本；二者变化都产生状态事件。
- 地址预算当前投影只在 command shadow debit、预算模式/关键阈值变化或低频 checkpoint 时更新；每次轮询样本不持续
  UPDATE Postgres 热行，而是追加写 ClickHouse；窗口值记录 algorithm version 与 window end。
- 现有 `risk_reservations` 扩展为逐风险所有者/child reservation，移除单个 command 唯一限制；来源区分
  LIVE_ORDER、INFLIGHT_PLACE、UNKNOWN、NEW_QUOTE。父 batch 完成不得整体释放，只有权威 fill/cancel/reject/expire
  才逐项 consume/release。admission 分别计算 worst long 与 worst short，再取最大绝对敞口校验库存、margin 和 leverage。
- ActionBudget 重启后从最后远端快照和其后 durable execution actions 重建保守 shadow remaining，再查询远端校正；
  差异无法解释时进入 CANCEL_ONLY。单个 action 的 exact actual cost 只在可归因时写 reconciled cost。
- 交易事实 FK 使用 `ON DELETE RESTRICT`；策略/配置采用 archive/append-only。配置核心范围使用 CHECK，并增加
  `UNIQUE(strategy_id,config_hash)`、session 时间约束、quote revision 唯一约束、execution attempt 唯一约束和 revision 非负约束。

### 9.2 ClickHouse：高频分析与研究

新增追加型表：

- `mm_feature_samples`：fair、microprice、OFI、flow、volatility、toxicity、latencies。
- `mm_quote_decisions`：包括 `NO_QUOTE`，desired/live quote、edge 分解、inventory、budget 和 reason。
- `mm_inventory_samples`：库存、soft/hard 利用率、margin、funding carry。
- `mm_action_credit_samples`：远端额度、shadow debit、burn/earn rate、runway、策略分配。
- `mm_fill_markouts`：fill、reference price、side convention、1s/5s/30s markout、spread capture、queue/fill diagnostics
  和计算版本；它是执行质量事实，不是会计账本。

按 `(strategy_id, symbol, ts)` 排序，原始高频数据短 TTL，长期指标使用物化视图/聚合表。订单和成交权威事实不得迁入 ClickHouse。

### 9.3 Prometheus

Prometheus 只保存实时健康和告警指标，不作为订单、PnL、额度或配置审计事实源。

## 10. API 与前端控制面

### 10.1 API

重构为多实例、query/command 分离：

- `GET/POST /api/v1/strategies`，已有交易历史的实例只允许 archive，不提供硬删除。
- `GET/PATCH /api/v1/strategies/{id}`，更新使用 `If-Match` revision。
- `POST /api/v1/strategies/{id}/actions/{start|pause|resume|drain|stop}`。
- `GET/POST /api/v1/strategies/{id}/config-versions`。
- `POST /api/v1/strategies/{id}/config-versions/{version}/activate`。
- `GET /api/v1/market-making/{id}/state`
- `GET /api/v1/market-making/{id}/quotes`
- `GET /api/v1/market-making/{id}/inventory`
- `GET /api/v1/market-making/{id}/performance`
- `GET /api/v1/market-making/{id}/action-budget`

`quotes` 快照可增加向后兼容的可选 `external_reference`：`source`、`symbol`、`raw_price`、`adjusted_price`、
`basis_bps`、`divergence_bps`、`configured_weight`、`effective_weight`、`confidence`、`age_ms`、
`quality(healthy|degraded|stale|disabled)` 和 `observed_at`。字段缺失表示未启用或旧服务端，前端不得据此推断故障；
字段存在但 stale/degraded 时必须明确展示，且该 REST/WS 投影不是控制事实源。

所有 mutation 延续 RBAC、CSRF、`Idempotency-Key`、api_audit 和 `application/problem+json`。增加风险的配置只有在无
UNKNOWN、完整对账成功且 operator/admin 二次确认后激活。mainnet 的 paid reserve、扩大硬库存、提高杠杆必须为 admin 权限。

可靠生命周期、配置、预算和告警事件走 durable SSE；高频 quote/fair/inventory 展示走有界 WebSocket，慢客户端只丢高频中间状态。
页面首次加载或 WebSocket 重连必须用 REST 获取 Postgres 权威 position/runtime/action-budget 快照；WS 只做显示优化，消息携带
runtime/market revision 与 observed_at，gap/stale 时丢弃并 resync。performance 查询来自 ClickHouse 时必须返回
`as_of`、`stale` 和 `source`，且分析库故障不得影响控制面。精确数字继续使用 decimal string。

### 10.2 前端

新增 `/strategy/{id}/market-making` 工作台：

- Overview：运行模式、数据新鲜度、quote uptime、PnL、告警，以及外部参考源的 quality/age/effective weight。
- Live Quotes：盘口梯形图、fair/reservation、desired/live/inflight/UNKNOWN quote slots；启用外部参考时展示 raw/adjusted
  price、basis/divergence、confidence 和 cap 后权重，并明确标记“reference only / HL local anchor”。
- Inventory & Skew：库存带、skew、margin、funding carry、减仓状态。
- PnL：分开显示 Accounting PnL、Execution Quality/Markout 和 Inventory Episodes，禁止把 markout 重复计入会计净值。
- Action Budget：credits、burn/earn、USDC/action、runway、emergency reserve 和预算状态。
- Configuration：强类型表单、版本 diff、shadow 预览、审批与回滚。
- Events：生命周期、risk、reconciliation、execution UNKNOWN 和 operator audit。

前端类型使用 `strategy_type` discriminated union，禁止继续用无约束 `Record<string, ...>` 表示做市配置。危险操作使用二阶段确认，
mainnet 显著标识，所有实时卡片显示最后更新时间和 stale 状态。订单页展示 strategy、quote revision/side/level 和 maker/taker；
风控页展示库存、quote age、reject/unknown、动作 burn 和 runway。

## 11. 回放、仿真与验证

L2 快照不能还原真实队列位置和自身订单影响，因此仿真定位为工程和相对方案验证：

1. 保存 exchange timestamp、received_at、盘口、trades 和真实延迟分布。
2. 实现确定性 event-time replay 和故障注入。
3. queue-ahead、取消、同价新增、部分成交采用乐观/中性/悲观三套模型。
4. 使用真实 fee/rebate、funding、taker flatten、paid action 和额度状态。
5. mainnet shadow 分两层：decision shadow 只记录候选/NO_QUOTE/touch/markout；execution shadow 通过
   `ShadowOrderState/ShadowExecutionSimulator` 模拟 ACK、resting、partial fill、cancel、queue 和故障，并让
   QuoteCoordinator 面向统一 `OrderStateView`。shadow fill 只进研究表，不写正式 orders/fills/positions/ledger。
6. testnet 只验证协议、批次、幂等、恢复、对账和风控，不用于证明微观结构盈利。

验证阶梯：

```text
历史 replay + 故障注入
-> 至少 14 个完整 UTC 日 mainnet shadow
-> testnet 完整执行与恢复
-> testnet 连续至少 14 天无重复订单、未解释仓位差异或风控绕过
-> mainnet 单币/单档/最小合规 size canary
-> 只增加 size
-> 再增加第二 symbol
-> 最后才评估第二档
```

每次只扩大一个维度，并预先定义自动回退条件。

## 12. 指标与上线门禁

会计 PnL 必须与 Postgres ledger 严格对账：

```text
realized trading PnL
+ unrealized inventory change
+ net fee/rebate ledger entries
+ funding PnL
- paid action cost
= accounting net PnL
```

1s/5s/30s markout、quoted/captured spread、maker ratio 和 fill proxy 是执行质量诊断，不再次加入会计 PnL。
若做经济归因，fill-to-markout 与 markout-to-close/end residual 必须互斥，且全部归因项可用 Decimal 精确重组为 ledger PnL。

核心指标：1s/5s/30s markout、quoted/captured spread、fill probability、quote uptime、actions/fill、USDC/action、
库存 P50/P95/P99、硬限额次数、forced flatten、reject/unknown、receipt-to-decision、decision-to-send、ACK/cancel latency、
event-loop lag、数据 freshness 和对账差异。启用外部参考时还必须观测 source freshness、raw/adjusted price、basis、
相对 HL 本地主锚的 divergence、confidence/effective weight 和 quality；外部 stale、basis/divergence 超限以及 stale 时权重未归零
均需要告警。

进入 mainnet 极小资金 canary 的硬门槛：

- stale book、断流、user stream overflow、重启和 Postgres 故障注入下无重复单、漏撤单和风控绕过。
- 启动或恢复时保持 `CANCEL_ONLY`，完整对账后才报价。
- 核心风控、QuoteCoordinator、batch state machine 和恢复逻辑覆盖率至少 90%。
- 悲观 replay/shadow 在扣除全部成本后不显示结构性负 edge。
- 预计动作 runway 覆盖验证期和 emergency reserve。

扩大资金/范围的硬门槛：

- 同时满足不少于 30 个完整 UTC 交易日、预注册的最小独立 inventory episode/有效交易日样本量，并覆盖低/中/高
  波动、流动性和 funding regime。
- 按交易日或 inventory episode 做 block bootstrap，会计净 edge 的 95% 置信区间下界大于 0，禁止把相关 fills 当独立样本。
- trailing marginal `USDC/action >= 1.25`，远端余额和 cancel headroom 高于动态 reserve，预计 runway 覆盖下一观察窗口。
- 硬库存越限、重复订单和关键 reconciliation diff 均为 0；UNKNOWN/orphan 不超过 SLA 且全部有审计终态。
- 收益不能主要来自单次方向性库存暴露。

Canary 启动前必须激活版本化 `CanaryRiskEnvelope`：最大部署权益/quote notional、日亏损与累计亏损、每日/总动作、
最低远端额度/cancel headroom、forced flatten 次数/成本、UNKNOWN SLA、最长持续时间/成交量和自动 PAUSED/CANCEL_ONLY/HALTED 条件。
第二 symbol 默认建立新 strategy instance 和独立 sub-account；共享账户需先单独设计联合库存、额度与 nonce 风险。

## 13. Rust 迁移门槛

第一版继续使用 Python asyncio 单进程。只有 profile 证明热路径是瓶颈时迁移：

- p99 receipt-to-send 超过目标 quote lifetime 的 25%。
- p99 event-loop lag 持续超过 5–10ms。
- 单核持续超过 70%，行情合并后仍积压。
- 多 symbol 产生可量化的 stale quote 或漏刷新。
- 需要稳定低于约 50ms 的 receipt-to-send 且 Python 无法满足。

迁移顺序：WS 解码/订单簿 -> feature/quote 纯计算 -> quote diff/batch 编码 -> 签名热路径。优先使用 PyO3 保持单进程和
Python 安全内核；只有网络/执行确实需要独立伸缩时才拆进程。

## 14. 初始安全包络

以下是 canary 起点，不是盈利承诺，必须由 shadow 数据按 symbol 校准：

- 单 symbol、单 sub-account、每侧单档、1x 或极低杠杆。
- hard inventory notional 不超过账户权益的 10%–15%。
- 单次 quote size 为 hard limit 的 10%–25%。
- 正常 `min_quote_lifetime` 从 2–10 秒区间选取。
- paid reserve 默认关闭；自动 taker flatten 默认关闭，只在受控 emergency policy 中启用。
- mainnet `market_making_enabled=false`，通过 shadow/testnet/canary 门禁后显式打开。

任何固定参数都必须被视为可审计的配置版本，而不是代码常量。
