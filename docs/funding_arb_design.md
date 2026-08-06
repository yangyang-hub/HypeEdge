# 资金费套利设计（单所内 delta-neutral）

> 状态：主线策略设计文档。受 `docs/design.md` §7.0 约束；多类型控制面契约见 `docs/strategy_control_plane.md`。
> 当前目标：在 Hyperliquid **testnet** 完成可恢复的真实两腿执行；mainnet 继续硬禁用。
> 安全约束：测试网执行也必须经过现货元数据、持久化订单、成交驱动状态机、补偿交易、现货/永续对账和动作额度门禁。

## 1. 形态与立论

- **形态**：Hyperliquid 永续做空（收取 funding）+ Hyperliquid 现货做多（delta 对冲），同所内两腿。
- **净 delta ≈ 0**：现货腿抵消永续空头的方向暴露，组合不再赌涨跌，仅收取永续资金费。
- **为何不依赖跨所基建**：两腿在同一交易所（HL）完成，无需第二个交易所连接、跨所转账、双爆仓线管理。早期设计（design.md 旧版）否定的是「单腿收 funding = 裸方向暴露」；本形态通过加现货对冲腿消除该风险，故可作为单所阶段的主线策略。
- **与跨所基差套利的区别**：design.md §7.4 的跨所项（HL 永续 vs 他所现货）仍为后期项，与本文独立。

## 2. 收益与成本结构

Hyperliquid funding **每小时结算**（非币安 8h）。

- **收益**：`funding_rate × 永续名义`（每小时）。`funding_rate > 0` 时空头收费，本策略在此条件下入场。
- **成本**：两腿手续费（永续 maker/taker + 现货）+ 滑点 + 现货腿买卖价差 + 资金占用成本。
- **净 edge**：`funding 收益 − 两腿往返手续费 − 价差/滑点 − 资金成本`。回测必须按小时建模 funding、现货价差与手续费，漏任一项都可能使方向反转（design.md §6 回测纪律）。

## 3. 配置参数（typed，落 `funding_arb_config_versions`）

| 字段 | 类型 | 约束 | 默认 | 含义 |
|---|---|---|---|---|
| `entry_funding_rate` | NUMERIC(38,18) | > 0 | 0.0001 | 入场 funding 阈值（绝对小时率，>0 时空永续收 funding） |
| `exit_funding_rate` | NUMERIC(38,18) | ≥ 0 且 < entry | 0 | 平仓阈值（funding 回落至此以下了结） |
| `max_notional_usd` | NUMERIC(38,18) | > 0 | 1000 | 单策略最大对冲名义敞口（USDC） |
| `hedge_ratio` | NUMERIC(38,18) | (0, 1] | 1.0 | 现货腿相对永续腿的对冲比例（1.0 = 完全对冲） |
| `rebalance_threshold_bps` | BigInt | > 0 | 50 | 两腿 delta 偏离触发再平衡的阈值（bps） |
| `leverage` | NUMERIC(38,18) | > 0 | 1 | 永续腿杠杆 |
| `max_slippage_bps` | BigInt | 1–500 | 50 | 每条 IOC 市价保护限价相对参考价的最大滑点 |
| `max_basis_bps` | BigInt | > 0 | 500 | 现货与永续价格偏离的最大入场门槛 |
| `min_expected_edge_bps` | NUMERIC(38,18) | ≥ 0 | 5 | 预期持有期 funding 扣除费用/盘口成本后的最低净 edge |
| `expected_hold_hours` | BigInt | 1–168 | 8 | 入场经济性估算使用的持有小时数，不阻止 funding 反转时提前退出 |
| `round_trip_fee_bps` | NUMERIC(38,18) | ≥ 0 | 20 | 两腿完整往返手续费与保守成本缓冲 |
| `max_unhedged_seconds` | BigInt | 1–60 | 15 | 第一腿成交后允许处于未完全对冲状态的最长时间 |

交易对不属于操作员配置。现有 typed 表中的 `spot_coin` 仅作为旧配置兼容列保留；新 API 在内部写入保留值
`AUTO/USDC`，runtime 不把它当作交易所市场，也不读取 `strategy_instances.symbol` 作为固定永续。自动选择的实际
`perp_symbol`、`spot_symbol`、display/base/quote 均在 cycle 创建时持久化。

自动选择的部署级安全参数位于 `FundingArbSettings`，三环境 YAML 必须同步：

| 字段 | 默认 | 含义 |
|---|---:|---|
| `universe_refresh_seconds` | 30 | 全市场元数据、funding 与 24h 成交量慢刷新间隔 |
| `book_refresh_seconds` | 5 | 入围候选 REST L2 刷新间隔，必须不大于行情 stale 门槛 |
| `max_candidate_markets` | 8 | 每轮最多请求盘口的成交量候选数，限制 IP 权重 |
| `min_spot_24h_volume_usd` | 1000 | 现货 24h 名义成交量硬下限 |
| `min_perp_24h_volume_usd` | 10000 | 永续 24h 名义成交量硬下限 |
| `min_top_book_depth_usd` | 100 | 两腿四侧在滑点带内最小可成交名义深度 |
| `max_combined_spread_bps` | 100 | 现货与永续相对点差之和硬上限 |

## 4. 控制面契约

- **策略类型**：`funding_arb`，注册于 `StrategyRegistry`（`src/hypeedge/strategy/funding_arb/runtime.py::build_funding_arb_plugin`）。
- **Capabilities**：`creatable=True`；`desired_states={stopped, running, paused}`；**无 shadow、无 drain**；`workspace="funding-arb"`（前端工作台 `/strategy/[id]/funding-arb`）。
- **创建**：`POST /api/v1/strategies`，`strategy_type="funding_arb"`；请求不接受 `symbol`、`spot_coin` 或
  `sub_account`，服务端将实例市场作用域固定为 `AUTO`，并从已校验的 `HYPE_EXCHANGE__ACCOUNT_ADDRESS` 注入
  账户范围；`initial_config` 只包含上表策略/风险参数。
- **配置版本**：`funding_arb_config_versions` 子表 + 通用 `strategy_config_versions`；`POST .../config-versions` 追加版本、`.../activate` 热替换（`apply_config` 解码为 `FundingArbParams`）。
- **生命周期**：`start/stop/pause/resume` 经 Supervisor + CapabilityGate；对 `drain` 或目标 `shadow` 返回 409/422。
- **DB 约束**：`strategy_instances.strategy_type` CHECK 由 Alembic 迁移显式加入 `funding_arb`，不得只修改 ORM metadata。
  收紧的资金费阈值/现货标识 CHECK 以 `NOT VALID` 加入：不改写带内容哈希的不可变历史版本，但会约束所有新写入；
  激活配置前由 Strategy Type Plugin 再校验一次，因此旧非法版本不能成为新的 desired config。

## 5. 真实执行架构

### 5.1 环境与账户门禁

- 只有 `HYPE_ENV=testnet`、完整 V2 交易链、`funding_arb_execution_enabled=true`、启动对账成功、Kill Switch 未触发、
  账户健康与动作额度新鲜时可启动真实 runtime。
- `dev` 只运行观察/控制面；`mainnet` 即使误开环境变量也必须拒绝构造真实 runtime。
- Agent wallet 只负责交易签名，不执行资金转账。现货 USDC 必须由操作员事先在 Hyperliquid UI/受控流程中划入；
  运行时禁止持有主钱包私钥或自动调用 `usdClassTransfer`。
- 每策略仍要求明确账户范围；实例 `sub_account` 只能由后端当前配置账户注入，客户端不能指定或伪装为未实际路由的子账户。
  `AUTO` allocation 是账户级通配租约：与同一账户下任意固定 symbol allocation 互斥。

### 5.2 元数据、行情与精度

- `InstrumentMetaCache` 同时加载 perp `meta` 与 `spotMeta`，保存 display name、exchange coin、base/quote token、
  size decimals、最小数量和价格精度规则。下单、成交和持久化统一使用 exchange coin。
- scanner 以 `spotMetaAndAssetCtxs` 的 USDC quote 市场和 `metaAndAssetCtxs` 的同名 perp 做严格 token identity join；
  每个候选都验证 `spot.base_token == perp symbol` 且 `spot.quote_token == USDC`，任一元数据缺失即淘汰。
- universe/context 每 30 秒以内至多请求一次；先按两腿 24h 名义成交量的较小值排序，只为前
  `max_candidate_markets` 个候选获取 REST `l2Book`。REST 盘口同时注入共享 `BookManager`，让执行风控、价格新鲜度与
  策略使用同一快照；不通过订阅全部现货 WS 扩大消息量。
- 入场要求两边盘口新鲜、双边非空、非交叉，四侧滑点带深度同时覆盖部署硬门槛和本次目标名义，组合点差、
  24h 成交量均通过部署硬门槛，且
  `basis_bps <= max_basis_bps`。
- 预期净 edge：`funding_rate × expected_hold_hours × 10000 - round_trip_fee_bps - observable_spread_cost_bps`，
  必须不低于 `min_expected_edge_bps`。
- 多个候选同时通过时，先按预期净 edge、再按两腿较小 24h 成交量、最后按最小盘口深度降序选择。选中后在
  authoritative account refresh 之后只复核同一候选，禁止在两次入场检查之间静默换市场。

### 5.3 Cycle 状态机

```text
IDLE
  -> ENTERING_SPOT
      -> IDLE                         (零成交/明确拒绝)
      -> ENTERING_PERP               (按现货权威成交量计算永续目标)
          -> OPEN                    (共同对冲规模成立)
          -> COMPENSATING_ENTRY      (永续拒绝/部分成交，卖出现货剩余)
              -> OPEN | CLOSED | FAULTED

OPEN
  -> REBALANCING                     (只缩减较大一腿，不主动扩大风险)
      -> OPEN | FAULTED
  -> EXITING_PERP                    (funding 回落、stop 或 operator close)
      -> EXITING_SPOT                (按已平永续规模卖出现货)
          -> CLOSED | FAULTED
```

- 入场先现货、后永续；退出先永续、后现货。
- 每个 leg 使用唯一规范 cloid 和 IOC 市价保护单。UNKNOWN 先查询，不换 cloid 重发。
- 部分成交按 authenticated fill 的累计 `filled_size` 推进；ACK/REST 请求返回只能作为临时信息。
- 第二腿部分成交后只保留 `min(spot_filled / hedge_ratio, perp_filled)` 对应的共同规模，其余现货立即补偿卖出。
- 再平衡第一版只允许缩减敞口：现货不足时 reduce-only 买回部分永续；现货过多时卖出现货。增加仓位留待后续版本。

### 5.4 持久化与恢复

- `orders` / `fills` 增加 `is_spot`；`orders` 同时持久化 `risk_reducing` 和 `max_slippage_bps`，保证重启后的 SDK 请求
  与原业务意图一致。现货订单禁止 `reduce_only=true`。
- 新增 `spot_balances` 权威投影，来源为 `spotClearinghouseState`；现货 fill 不得写入 perp `positions`。
- 新增 `funding_arb_cycles` 当前/历史 cycle 与 `funding_arb_cycle_events` 追加型状态转换事实，记录配置版本、两腿
  target/filled、cloid、funding/basis、错误与 revision。
- 新增 `funding_payments`，通过 `userFunding` inbox/cursor 幂等补缺；归因到当时覆盖该 symbol 的 active cycle。
- 重启时从 cycle 的实际 `perp_symbol` / `spot_symbol` 恢复 instrument binding，先恢复订单和账户事实，再恢复 cycle。
  任何 cycle 状态与现货余额/永续持仓/订单终态不一致时进入
  `FAULTED`，禁止自动新开仓。

### 5.5 风控与补偿

- Spot BUY 必须有足够可用 quote USDC；Spot SELL 不得超过可用 base token。Spot SELL 只有在权威余额证明减少现货
  敞口时才标记 `risk_reducing`。
- Perp 平仓必须 `reduce_only=true`；入场前按配置设置 isolated leverage，失败即不开仓。
- `max_notional_usd` 同时受部署级 `funding_arb.max_notional_usd` 硬上限约束。
- 达到 `max_unhedged_seconds`、补偿 UNKNOWN、账户/行情过期或对账不一致时停止新增风险并 fault；撤单始终允许。
- Kill Switch 会停止信号并撤单。已确认的单腿敞口由专用风险缩减路径处理；该路径只能减少权威敞口，不能借机开仓。

### 5.6 Testnet 验收门禁

1. 预检：官方 testnet URL、专用账户、无既有挂单/目标币仓位、现货 USDC 已预充值、动作额度 ≥100。
2. 最大配置名义不超过 25 USDC；只运行一个自动市场 cycle。验收窗口若无候选同时通过流动性、基差与经济门槛，
   正确结果是明确的 `no_eligible_liquid_market` 且零订单，不得为测试而自动降低门槛。
3. 实测完成：现货买入成交 → 永续空单成交 → 两边权威对账 → reduce-only 平永续 → 卖出现货 → 最终两边归零。
4. 故障注入覆盖：第一腿拒绝、第二腿拒绝、两腿部分成交、UNKNOWN、重启恢复、Kill Switch 和补偿失败。
5. 测试 teardown 必须按交易所权威状态清理挂单、永续持仓和本策略现货余额；无法清理即测试失败并保留告警。
6. 至少连续 14 天 clean soak 后才允许另行设计 mainnet canary；本版本没有 mainnet 解锁路径。

## 6. 风险

- **HL 现货流动性/价差**：现货腿买卖价差可能吃掉 funding 收益；需评估各 coin 现货深度。
- **两腿执行延迟/滑点**：永续与现货非同步成交产生瞬时 delta 暴露；再平衡本身也耗动作额度。
- **funding 反转**：funding 可转负或波动，需 `exit_funding_rate` 与最大持仓时限保护。
- **验证方式**：套利回测可信度高于做市（不强依赖队列位置），但仍须小资金实盘验证现货价差与执行假设；不信纯回测收益。

## 7. 文档关系

| 文档 | 职责 |
|------|------|
| `docs/design.md` §7.0 | 冻结跨模块决策（主线定位、形态、风险） |
| 本文 | 资金费套利算法骨架、配置、控制面契约、执行路线 |
| `docs/strategy_control_plane.md` | 多类型控制面与创建/启停通用契约 |
| `docs/market_making_design.md` | 做市设计（动作额度、风控范式可复用） |
