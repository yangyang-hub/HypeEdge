# 资金费套利设计（单所内 delta-neutral）

> 状态：主线策略设计文档。受 `docs/design.md` §7.0 约束；多类型控制面契约见 `docs/strategy_control_plane.md`。
> 当前实现：**控制面骨架 + 运行时 stub**（可创建/配置/启停，不下单）。真实执行分阶段补全（见 §5）。

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
| `entry_funding_rate` | NUMERIC(38,18) | ≥ 0 | 0.0001 | 入场 funding 阈值（绝对小时率，>0 时空永续收 funding） |
| `exit_funding_rate` | NUMERIC(38,18) | ≥ 0 | 0 | 平仓阈值（funding 回落至此以下了结） |
| `max_notional_usd` | NUMERIC(38,18) | > 0 | 1000 | 单策略最大对冲名义敞口（USDC） |
| `hedge_ratio` | NUMERIC(38,18) | (0, 1] | 1.0 | 现货腿相对永续腿的对冲比例（1.0 = 完全对冲） |
| `rebalance_threshold_bps` | BigInt | > 0 | 50 | 两腿 delta 偏离触发再平衡的阈值（bps） |
| `leverage` | NUMERIC(38,18) | > 0 | 1 | 永续腿杠杆 |

> 现货腿 symbol 由 `strategy_instances.symbol`（永续 coin）派生，不单独版本化；待真实执行接入后再决定是否引入显式 `spot_symbol`。参数集合为骨架初版，随执行逻辑演进调整。

## 4. 控制面契约

- **策略类型**：`funding_arb`，注册于 `StrategyRegistry`（`src/hypeedge/strategy/funding_arb/runtime.py::build_funding_arb_plugin`）。
- **Capabilities**：`creatable=True`；`desired_states={stopped, running, paused}`；**无 shadow、无 drain**；`workspace="funding-arb"`（前端工作台 `/strategy/[id]/funding-arb`）。
- **创建**：`POST /api/v1/strategies`，`strategy_type="funding_arb"`，`initial_config` 为上表字段（判别联合，见 `api/schemas.py`）。
- **配置版本**：`funding_arb_config_versions` 子表 + 通用 `strategy_config_versions`；`POST .../config-versions` 追加版本、`.../activate` 热替换（`apply_config` 解码为 `FundingArbParams`）。
- **生命周期**：`start/stop/pause/resume` 经 Supervisor + CapabilityGate；对 `drain` 或目标 `shadow` 返回 409/422。
- **DB 约束**：`strategy_instances.strategy_type` CHECK 已含 `funding_arb`（`STRATEGY_TYPES`）。

## 5. 运行时与分阶段执行路线

### 5.1 当前（已实现）：运行时 stub

`FundingArbRuntimeHandle`（`src/hypeedge/strategy/funding_arb/runtime.py`）实现 `StrategyRuntimeHandle`：
- `start` / `set_mode(running|paused|stopped|faulted)` / `stop` 仅记录状态并打日志，**不创建下单任务**；`shadow/warming` 跳过（不被该类型支持）。
- `apply_config` 解码配置为 `FundingArbParams` 并存储，不触发交易。

目的：让控制面、配置版本、启停端到端可用，为真实执行提供接入点。

### 5.2 后续阶段（待实现）

1. **现货行情**：HL 现货（HIP-1/HIP-2）L2/trades 订阅与 `MarketDataProvider` 扩展。
2. **现货下单**：execution 引擎支持现货 order type（当前仅永续）；与永续腿共用 NonceManager 串行签名。
3. **对冲再平衡**：按 `rebalance_threshold_bps` 监控两腿 delta，偏离时补腿；受 §3.2 地址动作额度约束（降频、留额度）。
4. **入场/平仓逻辑**：funding 信号（每小时）驱动开/平两腿；funding 数据已具备（`FundingRate` 模型、`EVENT_FUNDING_UPDATE`、ClickHouse `funding` 表）。
5. **PnL 归因**：funding 收益、两腿已实现/未实现 PnL、手续费分离，与 Accounting 对账一致。
6. **回测**：利用已回填的 funding/现货历史，按小时建模两腿与价差。

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
