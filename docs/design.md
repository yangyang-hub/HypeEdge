# HypeEdge 量化交易系统 — 设计文档

- 版本：v0.2
- 日期：2026-06-01
- 目标平台：Hyperliquid（永续 L1 订单簿）
- 范围说明：**资金费率套利本版暂不纳入**（后续可作为独立模块加回；注意其正确做法本质是「带对冲腿的跨所操作」，依赖多交易所基建，详见 §7.4）。

---

## 1. 项目目标

构建一个**个人可维护**的 Hyperliquid 多策略量化系统，遵循「先把数据与执行基建打磨好，再逐步上策略」的顺序，避免过早优化与过度工程。

核心理念：

- **执行基建比策略本身更值钱**——对账、幂等、nonce、kill switch 这套东西做扎实，比多写一个策略更重要。
- **按真实约束设计**——Hyperliquid 的限频、签名、订单规则是一等约束，写进架构而非事后补丁（见 §3）。
- **能简则简**——个人开发，起步用模块化单体 + 最小存储，等真有瓶颈再拆分/换语言。

---

## 2. 关键设计决策（含与初版方案的差异）

| 决策点 | 结论 | 理由 |
|---|---|---|
| 服务拆分 | **先模块化单体**，模块边界保留为未来拆分蓝图 | 6 个微服务对个人是过度设计；风控放下单路径上做网络调用会给每单加延迟 |
| 风控位置 | **内联在下单路径**（同步校验，零网络开销） | 风控必须在每个订单发出前同步通过 |
| 语言 | **第一二阶段 Python；第三阶段把执行热路径重写 Rust** | 低频不需要 Rust 的延迟；研究阶段 Python 迭代快 5–10× |
| 账户隔离 | **每策略一个子账户（sub-account）+ isolated margin** | 避免策略间保证金互相穿仓、nonce 冲突、PnL 无法归因 |
| 签名钱包 | **API/agent wallet（只交易不可提现）** | 主钱包私钥永不进交易进程 |
| 限频模型 | 把**「按地址动作额度」**当成做市/网格/HFT 的一等约束 | 真正卡死交易机器人的不是 IP 权重，而是「成交量换动作额度」（见 §3.2） |
| 资金费率套利 | **本版移除** | 单腿收 funding = 裸方向暴露；正确做法依赖跨所基建 |
| 跨所基差套利 | **后期项**，依赖多所基建 | 单所阶段先不做 |

---

## 3. Hyperliquid 平台约束（基础事实，已核实）

> 这一节是所有上层设计的地基。数字来源见文末「参考来源」。

### 3.1 IP 维度限频（REST，按权重聚合）

| 项目 | 权重 / 限制 | 实际可发频率 |
|---|---|---|
| 聚合上限 | **1200 weight / 分钟 / IP** | 所有 REST 共享 |
| `info` 默认 | 权重 **20** | **~60 次/min** |
| 轻量 `info`（`l2Book` / `allMids` / `clearinghouseState` / `orderStatus` / `spotClearinghouseState` / `exchangeStatus`） | 权重 **2** | ~600 次/min |
| `userRole` | 权重 60 | — |
| `explorer` | 权重 40 | `blockList` 额外限 1 次/block |
| `exchange`（下单/撤单等） | 权重 **1 + floor(batch_length / 40)** | 单笔=1；批量 ≤40 仍只算 1（IP 维度） |
| EVM JSON-RPC | 100 次/min（独立） | `rpc.hyperliquid.xyz/evm` |

**按返回条目数的额外权重**（重要）：部分端点除基础权重外，还会按返回数据量累加权重：

| 端点 | 额外权重 |
|---|---|
| `recentTrades` / `historicalOrders` / `userFills` / `userFillsByTime` / `fundingHistory` / `userFunding` / `nonUserFundingUpdates` / `twapHistory` / `userTwapSliceFills` / `userTwapSliceFillsByTime` 等 | 每 **20 条**返回 +1 权重 |
| `candleSnapshot` | 每 **60 条**返回 +1 权重 |

要点：「1200/min」是**权重**不是请求数。普通 info 每次耗 20，实际只有 ~60 次/min；但账户状态 `clearinghouseState` 只耗 2，所以**用 REST 轮询账户状态没问题**。需限速的是 `fundingHistory`/`candleSnapshot` 这类权重 20 且有按条目数累加的历史回填端点——大批量查询时实际消耗远超 20，回填逻辑必须做分页限速。

### 3.2 地址维度限频（真正的瓶颈）⚠️

- 每地址初始 **10,000 个动作额度**；之后**每累计成交 1 USDC 解锁 1 个动作额度**。
- 批量请求：n 个订单/撤单，**IP 维度算 1 次，地址维度算 n 次**。
- 撤单正常算 1x；但因 `expiresAfter` 过期而触发的撤单算 **5x**。
- **撤单累计上限**：`min(limit + 100000, limit * 2)`——撤单操作还有独立的累计上限，长期运行需关注。
- **被限流保底**：地址被限流后仍允许 **1 请求 / 10 秒**，用于查询状态和紧急操作。
- **付费购买额度**：可通过 `reserveRequestWeight` exchange 动作，支付 **0.0005 USDC / 请求**购买额外动作额度——这是做市策略额度耗尽时的逃生通道，应在风控中作为可选项纳入（见 §8.1）。
- 用 `userRateLimit` info 接口**实时查询剩余额度**。

**这条直接决定做市/网格/HFT 是否可行**。示例：双边挂 5 档、每 2 秒刷新 = 每 2s 10 挂 + 10 撤 = 600 动作/min = 3.6 万/小时。若几乎无成交，**10,000 初始额度约 17 分钟烧光**，之后需每小时打出 ~3.6 万 USDC 成交量才能维持该刷新率。→ 小资金做市必须**降频、减档、放宽点差**，并把额度退避写进风控。额度紧急耗尽时可考虑 `reserveRequestWeight` 付费续命。

### 3.3 订单与挂单限制

- 默认挂单上限 **1000**，每 5M USDC 成交量 +1，**封顶 5000**。
- **当已有 ≥1000 个挂单时，reduce-only 单和触发单（止损单）会被直接拒绝** → 网格/做市挂太多档时，止损可能挂不上去，必须留额度。

### 3.4 WebSocket 限制

| 项目 | 限制 |
|---|---|
| 连接数 | 10 / IP |
| 新建连接 | ≤30 / min |
| 订阅数 | 1000 |
| 发送消息 | ≤2000 msg/min |
| inflight post | 100 同时 |
| user-specific 订阅的唯一用户 | 10 |

### 3.5 其它平台特性

- **funding 每小时结算**（非币安 8h），premium + 利率钳制公式 → 回测与再平衡节奏须按小时建模。
- HL 是**链上 L1 撮合**，"毫秒级 HFT" 语义与 CEX colocation 竞速不同，第三阶段延迟预期按出块/共识节奏校准。
- 支持 **sub-account**、**agent/API wallet**、**cloid（客户端订单 ID）**——这三个是隔离、安全、对账的基础。

---

## 4. 总体架构

**第一/二阶段：单进程模块化单体**。下列为内部模块（非独立服务），边界清晰，便于未来按需拆分。

```
                 ┌──────────────────────────────────────────┐
                 │              HypeEdge (单进程)              │
                 │                                            │
   WS/REST ────▶│  market-data ──▶ 内存行情快照(策略读取)       │
                 │       │                                    │
                 │       ▼                                    │
                 │   strategy ──signal──▶ risk(内联校验)       │
                 │                          │                 │
                 │                 fail-safe │                │
                 │                   检查失败=拒绝下单           │
                 │                          │                 │
                 │                          ▼                 │
                 │                     execution ──签名/nonce──┼──▶ Hyperliquid
                 │                          │                 │
                 │                          ▼                 │
                 │   account(余额/持仓/PnL) ◀── 对账 reconciler│
                 │                                            │
                 │   monitor (Prometheus 指标 + 告警)          │
                 └──────────────────────────────────────────┘
                       │                  │
                ClickHouse            Postgres(订单/持仓/PnL)
             (tick/盘口/成交/K线)
                                      Redis(热状态/pubsub, optional,
                                            多进程时再加)
```

模块职责：

- **market-data**：WS 订阅盘口/成交/K线 + REST 拉 funding/OI/mark/历史；落 ClickHouse；维护内存行情快照供策略读取。
- **strategy**：消费行情，产出信号（目标仓位 / 挂撤单意图）。
- **risk**：**同步内联**校验每个订单意图（仓位、杠杆、亏损、回撤、动作额度），通过才放行；**fail-safe 语义：检查本身出错/超时时拒绝下单**（见 §8.4）。
- **execution**：**唯一签名出口**，负责 nonce 串行、cloid 生成、下/撤/改单、重试。
- **account**：余额、持仓、PnL 记账；与交易所对账。
- **reconciler**：启动/重连后，用交易所真实挂单与持仓校正本地状态。
- **monitor**：暴露 Prometheus 指标，触发 Telegram/钉钉 告警。

> 拆分时机：某模块成为瓶颈或需独立伸缩时（通常是第三阶段的 execution / market-data），再抽成独立进程。

---

## 5. 数据系统（第一阶段重点，关键路径最长的一段）

### 5.1 采集

- **WebSocket（行情主通道）**：`l2Book`（盘口）、`trades`（成交）、`candle`（K线）、`activeAssetCtx`/`allMids`（mark、funding、OI 等上下文）。
- **REST/Info（历史与账户）**：`fundingHistory`、`candleSnapshot`（历史回填，权重 20 + 按条目累加，需分页限速排队）、`clearinghouseState`（账户状态，权重 2，可常轮询）、`meta`/`metaAndAssetCtxs`（合约元信息）。
- **可靠性**：WS 断线重连 + 自动重订阅 + 用快照做缺口对账；记录每条消息的本地接收时间戳，用于延迟监控与数据完整性校验。

### 5.2 存储

| 用途 | 选型 | 起步策略 |
|---|---|---|
| tick / 盘口 / 成交 / K线 | **ClickHouse** | 第一阶段必上 |
| 订单 / 持仓 / PnL（事务性） | **Postgres** | **第二阶段必上**（第一阶段仅骨架） |
| 热状态 / 进程间 pub-sub | Redis | **按需**，多进程时再加 |

ClickHouse 表设计（含 DDL 模板）：

```sql
-- 模板表：l2_book（盘口快照）
-- 引擎：MergeTree（追加写，按时间+币种排序）
-- 分区：按天（便于按时间范围裁剪/删除旧数据）
-- TTL：保留 365 天（可按需调整）
CREATE TABLE l2_book (
    ts          DateTime64(3),       -- 接收时间戳（毫秒精度）
    coin        LowCardinality(String), -- 币种，如 "BTC"
    side        Enum8('bid' = 1, 'ask' = 2),
    level       UInt16,              -- 档位（0=最优价）
    px          Float64,             -- 价格
    sz          Float64              -- 数量
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY (coin, ts, side, level)
TTL ts + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;
```

其他表遵循相同模式：

- `trades`：`(ts DateTime64(3), coin LowCardinality(String), px Float64, sz Float64, side Enum8, tid UInt64)`，`ORDER BY (coin, ts)`，按天分区，TTL 365 天。
- `candles`：`(ts DateTime64(3), coin LowCardinality(String), interval LowCardinality(String), o Float64, h Float64, l Float64, c Float64, v Float64)`，`ORDER BY (coin, interval, ts)`，按月分区，TTL 730 天（2 年，供长期回测）。
- `funding`：`(ts DateTime64(3), coin LowCardinality(String), funding_rate Float64, premium Float64, oi Float64, mark_px Float64)`，`ORDER BY (coin, ts)`，按月分区，TTL 730 天。
- `mid_prices`：`(ts DateTime64(3), coin LowCardinality(String), px Float64)`，`ORDER BY (coin, ts)`，按天分区，TTL 90 天（高频数据，短期查询为主）。
- 做市遥测（P6 已实现）：`mm_feature_samples`、`mm_quote_decisions`、`mm_inventory_samples`、
  `mm_action_credit_samples`、`mm_fill_markouts`。五表均为追加型分析投影并按
  `(strategy_id, symbol, ts)` 排序；feature 原始样本 TTL 30 天，报价/库存样本 TTL 180 天，动作额度样本 TTL 365 天，
  fill markout TTL 730 天。`mm_fill_markouts` 按 fill/horizon 保存 reference、side convention 和 calculation version，
  仅用于执行质量分析，不属于 Accounting PnL 或风控事实源。

> **起步可选方案**：如果 ClickHouse 的运维负担在第一阶段过高，可用 **DuckDB**（嵌入式分析库，无需独立服务）替代做初期数据积累与回测查询，待数据量或查询需求超出时再迁移到 ClickHouse。需在路线图中明确迁移触发条件（如：单表超 1 亿行、查询延迟 > 5s）。

Postgres 关键表：

- `orders`：含 **`cloid`（唯一）**、交易所 oid、状态、子账户、策略 id、时间线。
- `positions`：按子账户/策略的持仓快照。
- `fills` / `pnl`：成交与盈亏记账。

### 5.3 数据是长周期资产

有意义的回测需积累数月数据 → **采集现在立刻开始**。但 funding/OI/K线历史可经 REST 回填，**部分策略不必等自采数据就能先回测**。

### 5.4 数据可靠性策略

当前 `ClickHouseWriter` 采用内存批量缓冲 + 定时刷写的模式。存在以下风险和应对：

| 风险 | 当前行为 | 应对策略 |
|---|---|---|
| ClickHouse 连接丢失 | flush 失败仅记日志，缓冲区满后清空，丢失未写入数据 | Phase 1B：增加磁盘溢出缓存（SQLite/WAL），连接恢复后回放 |
| 进程崩溃 | 内存缓冲区数据丢失 | 可接受：WS 实时数据可通过 REST 回填补缺；关键交易数据（Phase 2）走 Postgres WAL |
| 数据重复 | 当前不检测 | Phase 1B：回填写入使用 `INSERT INTO ...`（ClickHouse ReplacingMergeTree 或去重查询） |

**Phase 1 可接受的数据丢失范围**：实时行情数据（l2_book/trades）短暂丢失可通过 REST 回填补偿。Phase 2 的交易数据（订单/成交/持仓）必须走 Postgres 事务，零丢失要求。

---

## 6. 回测与模拟撮合

- **务必建模**：手续费（含 maker rebate）、滑点、**每小时 funding**、资金成本。漏掉任一项盈亏方向都可能反。
- **做市 / 盘口不平衡策略的回测基本不可信**：无法从 L2 快照重建队列位置，也无法模拟自身挂单对盘口的影响 → 这类策略用**极小真金白银实盘验证**，不信回测。
- **可较可靠回测**的是趋势/网格这类基于公开行情、不强依赖队列位置的策略，但仍需保守的滑点假设。
- 模拟撮合先做「乐观/悲观」两档假设，给出收益区间而非单点值。

### 6.1 防过拟合方法论

> 量化系统最常见的失败模式：回测炸裂、实盘爆炸。以下方法应在开发回测框架时一并内置。

- **Walk-forward 分析**：将数据按时间窗口分为训练段和验证段，滚动前进（如 60 天训练 → 30 天验证 → 窗口前移 30 天）。参数在训练段优化，在验证段评估。这比单次训练/测试划分更能模拟真实部署后的参数老化。
- **样本内 / 样本外划分**：所有参数调优只能在样本内（训练集）进行；样本外（测试集）用于最终评估，**绝不回调参数**。报告样本外结果作为策略预期。
- **多重检验校正**：测试了 N 组参数组合后，统计显著性需打折（如 Bonferroni 校正：若测试 100 组参数，则显著性阈值从 0.05 提高到 0.0005）。框架应自动记录参数搜索次数并输出校正后的 p 值。
- **蒙特卡罗模拟**：对历史收益序列做随机重排序（bootstrap），检验策略收益是否显著优于随机。同时对关键参数做扰动测试，观察收益曲线的鲁棒性——参数微调就崩的策略不值得实盘。

---

## 7. 策略设计（已移除资金费率套利）

### 7.1 趋势跟随 —【近期，单所，第一个实盘策略】

- 中频，吃大行情；单所即可，回测相对可靠。
- 信号示例：多周期均线/动量/突破 + 波动率过滤（ATR 定仓位）。
- 风控：明确止损 + 仓位级风控（见 §8）。

### 7.2 动态网格 —【近期，单所，仅震荡开启】

- **难点在 regime 识别**（区分震荡 vs 趋势）与实时切换——这是该策略成败的核心，趋势中网格会爆。
- 网格逆势累积库存 → 必须有**硬止损 / 区间击穿强平**。
- 注意挂单上限：档位多时给止损/reduce-only 留够额度（§3.3）。

### 7.3 盘口做市 —【近期，仅小仓位测试】

- 权威方案见 §18 与 `docs/market_making_design.md`：单币、单档、事件驱动、库存型、动作额度感知，并允许
  `NO_QUOTE`；不采用固定点差和逐盘口撤挂。
- 受 **§3.2 地址动作额度**强约束：刷新只有在预期净 edge 覆盖 adverse selection、库存、funding、延迟和
  动作影子成本时才执行。
- 风险核心是**库存和在途最坏成交**：使用 soft/hard/emergency inventory bands、reservation-price skew、
  quote-set 风险预占和全局 kill switch，不采用逐单止损。
- 仅小资金验证，不以 L2 回测收益作为上线依据；必须经过 mainnet shadow、testnet 执行验证和单 symbol canary。
- `reserveRequestWeight` 默认关闭，只允许用于退出风险或处理 UNKNOWN，必须受单次、日和月成本上限约束。

### 7.4 跨交易所基差套利 —【后期项，依赖多所基建】

- 需要第二个交易所的连接、两腿对冲、转账延迟与各自爆仓线管理。
- 与「资金费率套利」共享同一套基建：真正 delta-neutral 的 funding 套利 = HL 空永续 + 他所现货多。**故资金费率套利若日后加回，应在本模块基建之上实现。**
- 单所阶段不启动。

---

## 8. 风控设计

### 8.1 限额（默认值，可配置）

| 维度 | 默认 | 口径 |
|---|---|---|
| 单币最大仓位 | 账户权益 10%–20% | — |
| 单策略最大亏损 | 账户权益 2%–5% | 触发后停该策略 |
| 总账户最大回撤 | 10% **停机** | **距历史峰值权益**的回撤 |
| 最大杠杆 | 2x–5x 起步 | — |
| 动作额度 | 剩余额度低于阈值时退避/降频 | 来自 `userRateLimit`（§3.2） |
| 付费续命上限 | 可配置（如 10 USDC/天） | `reserveRequestWeight` 的成本上限，防止无限制花钱 |

### 8.2 止损语义（按策略类型区分）

- **趋势跟随**：逐笔/逐仓止损成立。
- **做市 / 网格**：以**仓位级 / 库存级限额 + skew + 全局 kill switch** 为主，**不是逐单止损**。
- **止损单的多层防御**（止损单本身耗额度、急跌会跳空、挂单≥1000 时会被拒）：
  1. 交易所原生止损（触发单）
  2. 机器人侧独立监控 → 条件满足主动市价平仓
  3. 总回撤 kill switch（最后兜底，停所有策略）

### 8.3 隔离

- **每策略一个子账户 + isolated margin**：防止策略间保证金穿仓、PnL 可独立归因。
- **agent/API wallet 签名**：不可提现；主钱包私钥隔离。

### 8.4 Fail-safe 语义

> **原则：宁可少赚，不可多亏。**

风控检查本身出错（持仓查询超时、数据库不可用、风控模块异常）时，默认行为是**拒绝下单**。具体规则：

- 风控模块必须在自己的超时窗口内（可配置，默认 **500ms**）返回明确结果（通过/拒绝），超时视为拒绝。
- 风控依赖的数据源（交易所持仓查询、本地数据库）不可用时，策略降级为**只撤不下**模式。
- 风控模块本身的异常/崩溃由 execution 捕获，触发全局暂停（等同于 kill switch），等待人工介入。

---

## 9. 执行引擎要点（最易被忽略、出事最严重）

### 9.1 核心保证

- **幂等**：每个订单带 `cloid`；下单失败/超时按 cloid 查询真实状态，不盲目重发。
- **对账（reconciliation）**：进程启动 / WS 重连后，先用交易所真实挂单 + 持仓校正本地状态，再恢复策略——否则会重复下单或丢单。
- **nonce 串行**：所有签名动作经 execution 单点串行，nonce 单调递增；多策略并发签名必须在此收敛（也是用子账户隔离的另一个理由）。
- **撤单/expiresAfter**：避免 `expiresAfter` 过期触发 5x 额度惩罚；高拥堵时不重复发送已返回结果的撤单。
- **kill switch**：一键停机（撤所有单 / 可选平仓），由 monitor 与风控共同触发。

### 9.2 订单状态机

每个订单在本地维护明确的状态，状态流转如下：

```
               ┌─── cancelled (策略主动撤 / 超时未成交)
               │
  pending ──▶ submitted ──▶ acknowledged ──┬──▶ filled
       │                                    │
       │                                    ├──▶ partial_fill ──▶ filled
       │                                    │                (剩余继续挂单)
       │                                    │
       └──▶ rejected                        └──▶ expired (expiresAfter 到期)
           (交易所拒绝)                           ⚠️ 触发 5x 额度惩罚
```

状态说明：

- **pending**：策略发出意图，尚未提交给交易所。
- **submitted**：已签名并发送，等待交易所响应。
- **acknowledged**：交易所确认收到，挂单在簿。
- **partial_fill**：部分成交，剩余挂单仍在簿。
- **filled**：全部成交。
- **cancelled**：撤单成功（策略主动或执行引擎触发）。
- **rejected**：交易所拒绝（余额不足、限频、参数错误等）。
- **expired**：因 `expiresAfter` 过期被交易所自动撤单（⚠️ 触发 5x 额度惩罚，应尽量避免）。

### 9.3 部分成交处理

- 订单部分成交后，execution 通知 strategy 当前成交信息（成交价、成交量、剩余挂单量）。
- 策略自行决定是否追单（发新单补足）或接受部分成交。
- 风控对部分成交的订单按实际持仓实时更新，不会因为挂单未完全成交而放松约束。

### 9.4 超时与重试策略

- **下单超时**：发送订单后等待交易所响应的超时阈值默认 **3 秒**。
  1. 超时后，用 cloid 调用 `orderStatus` 查询订单真实状态。
  2. 若交易所确认未收到 → 使用相同 cloid 重新提交（幂等）。
  3. 若交易所确认已收到（已挂单/已成交）→ 更新本地状态，不重发。
  4. 若查询也超时 → **视为拒绝，不重发**，等下一轮 reconciler 对账。
- **撤单超时**：撤单请求超时后，同样先查订单状态再决定是否重试。
- **最大重试次数**：同一订单（同一 cloid）最多重试 **2 次**，超过后标记为 `rejected` 并告警。
- **退避**：连续失败时退避：首次立即重试，第二次等 1s，第三次等 2s，超过上限暂停该策略。

---

## 10. 技术栈

| 层 | 第一/二阶段 | 第三阶段 |
|---|---|---|
| 语言 | **Python**（研究/回测/低频执行） | 执行热路径重写 **Rust** |
| 行情 | WebSocket + REST | 同 |
| 时序存储 | ClickHouse（或起步用 DuckDB） | ClickHouse |
| 事务存储 | Postgres | 同 |
| 缓存/总线 | Redis（按需） | Redis |
| 监控 | Prometheus + Grafana | 同 |
| 告警 | Telegram / 钉钉 | 同 |
| HTTP API | **FastAPI**（Phase 2B+，为前端仪表盘提供 REST） | 同 |
| 前端 | —（Phase 3 引入 Next.js 仪表盘） | Next.js + shadcn/ui |

### HTTP API 层规划（Phase 2B+）

第二阶段执行引擎完成后，引入 **FastAPI** 作为 HTTP API 层，为前端仪表盘提供数据接口。API 端点定义见 `rules/frontend.md` 的 API 契约小节（12 个端点）。引入时机：ExecutionClient 和 AccountTracker 实现完成后、前端开发开始前。

- **实时推送策略**：账户/持仓/订单数据使用 SSE（Server-Sent Events）推送变更，行情数据前端直连 Hyperliquid WS（跳过后端中转以降低延迟）。低频数据（策略状态、风控面板）使用 SWR 轮询（5s 间隔）。
- **API 响应格式**：统一 `{ "ok": true, "data": ... }` 或 `{ "ok": false, "error": "..." }`。
- **认证**：本地部署阶段用简单 token 或 IP 白名单，不引入复杂 OAuth。

---

## 11. 路线图

**第一阶段：数据系统（现在立刻，预计 2–4 周）**
1. WS 行情采集（盘口/成交/K线/上下文）+ 断线重连/对账
2. ClickHouse 落地（或 DuckDB 起步）+ REST 历史回填（funding/OI/K线）
3. 回测框架骨架（含费率/滑点/小时 funding 建模 + 防过拟合方法论）

**第二阶段：第一个实盘策略（趋势跟随，预计 3–6 周）**
1. 打磨执行基建：cloid 幂等、nonce 串行、reconciler、kill switch、订单状态机
2. 子账户 + agent wallet + isolated margin
3. Testnet 集成测试 + Paper trading 验证
4. 小资金实盘；风控全程在线（§8）

**第三阶段：做市 / HFT（确认额度可行后，预计 4–8 周）**
1. 用 `userRateLimit` 验证目标刷新率可持续
2. 执行热路径换 Rust
3. 动态网格在可靠 regime 识别就绪后启用

> 时间为单人兼职开发的量级估算，全职可缩短 50%–70%。

---

## 12. 监控与告警

- **指标（Prometheus）**：WS 连接状态/延迟、剩余动作额度、各策略 PnL、持仓/杠杆、回撤、订单成功率、撤单率、对账差异。
- **告警（Telegram/钉钉）**：WS 断线、动作额度低水位、回撤逼近停机线、对账不一致、下单连续失败、kill switch 触发。
- **Grafana**：账户总览 + 各子账户/策略分面板。

---

## 13. 待决问题 / 风险登记

- [ ] 动态网格的 **regime 识别**方法（成败核心，目前未定）。
- [ ] 做市目标刷新率在 §3.2 额度下是否可持续（需实测 `userRateLimit`）。
- [ ] 做市/盘口策略回测不可信 → 验证只能靠小资金实盘。
- [ ] 数据积累周期：多久数据量才够支撑可信回测。
- [ ] 跨所基差套利的第二交易所选型（后期）。
- [ ] 资金费率套利是否、何时加回（依赖 §7.4 基建）。
- [ ] 第一阶段是否先用 DuckDB 替代 ClickHouse 降低运维负担，以及迁移触发条件。
- [ ] 服务器部署位置：与 Hyperliquid 验证节点的网络延迟实测。

---

## 14. 测试策略

> 量化系统最怕「跑起来看起来对但实际算错了」。测试不是可选的，是每个阶段的一部分。

### 14.1 单元测试

- **风控计算**：仓位限额、回撤计算、杠杆检查、动作额度退避逻辑——用已知输入/预期输出覆盖边界条件。
- **订单状态机**：验证每个状态转换的合法性（如：filled 后不能再 cancel）。
- **仓位计算**：开仓/加仓/减仓/平仓后的持仓更新、PnL 计算、手续费扣除。
- **目标**：核心逻辑覆盖率 ≥ 90%。

### 14.2 集成测试（Testnet）

- 对 Hyperliquid **testnet** 运行真实的下单/撤单/改单/查询流程。
- 验证：cloid 幂等（同 cloid 重复下单不产生两笔）、nonce 串行、reconciler 对账。
- 验证：风控在 testnet 上的实际拦截效果（超限下单被拒、回撤触发停机）。
- **第二阶段实盘前必须通过**。

### 14.3 模拟盘（Paper Trading）

- 用真实行情驱动，但不实际签名/发送订单（或发到 testnet）。
- 验证策略信号 → 风控 → 执行的完整链路。
- 至少运行 **2 周**，确认无异常后才上实盘。

### 14.4 回测验证

- 用**已知结果的历史数据**（如手动验证过的某段行情）跑回测引擎，确认输出与预期一致。
- 验证手续费扣除、funding 扣除、滑点模拟是否正确。
- 每次修改回测引擎后重新运行验证。

---

## 15. 配置与密钥管理

### 15.1 密钥存储

- **主钱包私钥**：**永远不进交易进程**，仅用于生成 agent wallet 授权签名，离线安全存储。
- **Agent/API wallet 私钥**：dev/testnet 可使用权限为 600 的本地 `.env`；mainnet 仅通过部署 secret manager
  注入真实进程环境，不硬编码在代码、YAML、systemd unit 或前端 bundle 中。
- mainnet 启动强制要求 `HYPE_EXCHANGE__ACCOUNT_ADDRESS`、`HYPE_EXCHANGE__AGENT_PRIVATE_KEY`、
  `HYPE_POSTGRES__URL` 和 admin API token；任一缺失、任一已配置 token 少于 32 字符/重复、Postgres 使用
  弱默认密码或未启用 TLS 时拒绝启动。
- Dashboard BFF 按 viewer/operator/admin 分别使用服务端 Basic 凭据和对应 `HYPEEDGE_*_API_TOKEN`；浏览器
  不可读取后端 Bearer token。旧单用户变量仅保留 viewer 兼容权限。
- 推荐使用主机/云 secret manager 或权限为 600 的 systemd `EnvironmentFile`；所有密钥文件必须保持在
  `.gitignore` 中。
- **所有密钥文件纳入 `.gitignore`**，绝不提交到版本控制。

### 15.2 策略参数管理

- 策略参数（周期、阈值、仓位比例等）使用 **YAML/TOML 配置文件**，与代码分离。
- 配置文件支持**热更新**：monitor 文件变更 → 通知策略重新加载参数（不重启进程）。
- 每次参数变更记录日志（旧值 → 新值、变更时间、触发者），便于事后审计。

### 15.3 多环境隔离

| 环境 | 用途 | Hyperliquid 网络 |
|---|---|---|
| `dev` | 本地开发/调试 | 自动连接 **testnet**（API + WS） |
| `testnet` | 集成测试 / Paper Trading | 自动连接 **testnet**，用测试币 |
| `mainnet` | 实盘 | 自动连接 **mainnet**，真实资金 |

- 通过环境变量 `HYPE_ENV` 切换环境，各环境配置文件独立（`configs/dev.yaml`、`configs/testnet.yaml`、`configs/mainnet.yaml`）。
- **`api_url` / `ws_url` 由 `HYPE_ENV` 自动决定**，忽略 YAML 与 `HYPE_EXCHANGE__API_URL` / `WS_URL` 的手动覆盖，避免连错网络。
- **mainnet 配置默认不含任何私钥**，必须通过环境变量或交互式输入提供。

---

## 16. 部署与运维

### 16.1 进程守护

- 使用 **systemd** 管理交易进程：自动重启、日志收集、开机自启。
- 配置 `Restart=on-failure`，但设置 `StartLimitBurst=3` / `StartLimitIntervalSec=300`——连续崩溃 3 次后停止重启，等待人工介入（防止错误状态下的无限重启）。
- 提供 `Makefile` 或脚本封装常用操作：`make start`、`make stop`、`make status`、`make kill-switch`。

### 16.2 时钟同步

- 交易系统必须启用 **NTP 同步**（`chrony` 或 `systemd-timesyncd`）。
- 进程启动时检查本地时钟与 Hyperliquid API 返回的服务器时间偏差，超过 **1 秒**时告警并拒绝启动。

### 16.3 日志策略

- **格式**：结构化 JSON 日志（便于日志分析工具解析），包含时间戳、级别、模块、消息、关联字段（如 `cloid`、`strategy_id`）。
- **级别**：`DEBUG`（开发）、`INFO`（生产默认）、`WARNING`（风控触发）、`ERROR`（下单失败、连接断开）、`CRITICAL`（kill switch 触发）。
- **归档**：按天轮转，保留 30 天本地，超期自动清理；关键事件（kill switch、大额亏损）同时写入独立审计日志文件。
- **使用 Python `structlog`** 或类似库实现。

### 16.4 优雅停机

收到 SIGTERM / SIGINT 时的停机顺序：

1. **停止策略信号生成**（不再产生新订单意图）。
2. **撤回所有挂单**（或按配置保留 reduce-only 单）。
3. **等待执行队列排空**（inflight 订单完成或超时）。
4. **保存当前状态**（持仓、策略参数、nonce 最新值）到持久化存储。
5. **关闭连接**（WS、数据库、Prometheus exporter）。
6. **进程退出**。

总超时：**30 秒**，超时后强制退出并告警。

### 16.5 网络与服务器

- **服务器选址**：优先选择与 Hyperliquid 验证节点网络延迟最低的区域。建议部署前用 `ping` / `traceroute` / `curl` 测试 `api.hyperliquid.xyz` 的延迟，目标 **< 50ms**。
- **网络冗余**：建议配置备用网络链路（如双 ISP 或 4G 备份），WS 断线重连时自动切换。
- **资源估算**（起步）：2 核 CPU / 4GB RAM / 100GB SSD（ClickHouse 数据量取决于采集品种和频率，按需扩容）。

---

## 参考来源

- [Hyperliquid Docs — Rate limits and user limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)（IP 权重 1200/min、按地址 1 请求/USDC、初始 10,000、挂单上限、WS 上限、`reserveRequestWeight`、撤单累计上限等）
- [Hyperliquid Docs — Info endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)（按返回条目数的额外权重）
- [Hyperliquid Docs — Exchange endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint)（exchange 权重公式、`expiresAfter` 5x 惩罚、`reserveRequestWeight`）
- [freqtrade issue #10960 — 真实踩到 HL 按地址限频的案例](https://github.com/freqtrade/freqtrade/issues/10960)

---

## 17. V2 安全内核重构（2026-07）

### 17.1 事实源与模块边界

- Hyperliquid 是账户、订单和成交的外部权威；Postgres 是本地交易状态的唯一事务事实源。
- EventBus 只分发事务提交后的领域事件，不作为订单、成交、风控或系统状态的唯一存储。
- 策略、HTTP API 和运维命令统一通过 `TradingCommandService`，禁止直接访问 Exchange SDK 或
  `ExecutionEngine` 私有状态。
- 保持 asyncio 单进程模块化单体；高频行情使用进程内快照，关键交易命令使用持久化队列。

### 17.2 交易链路

```text
market-data -> StrategyRunner -> TradingCommandService
                                  |-> SafetyController
                                  |-> RiskGate + risk reservation
                                  |-> Postgres order/event/command/outbox transaction
                                  `-> SignedActionExecutor -> Hyperliquid

Hyperliquid user stream/REST -> ExchangeEventIngestor
                              -> fill/order/position/ledger transaction
                              -> outbox -> EventBus + SSE
```

所有 placement 必须持久化 `risk_decision`。撤单不受 Kill Switch、风控数据过期或动作额度低水位阻止。
订单 ACK 不代表成交，策略仓位只能由成交或交易所权威持仓更新。

### 17.3 系统安全状态机

系统状态持久化到 Postgres：

```text
STARTING -> RECONCILING -> NORMAL
                         -> REDUCE_ONLY
                         -> CANCEL_ONLY
                         -> HALTING -> HALTED -> RECOVERING -> RECONCILING
NORMAL/CANCEL_ONLY/HALTED -> STOPPING
```

- `NORMAL`：允许普通单、reduce-only 和撤单。
- `REDUCE_ONLY`：仅允许严格减少权威仓位的订单和撤单。
- `CANCEL_ONLY`：仅允许撤单。
- `HALTING/HALTED`：禁止普通 placement，始终允许撤单；紧急平仓走独立受控通道。
- Kill Switch 不直接终止进程。触发后停止策略、查询交易所权威挂单、撤全单、可选平仓、对账，
  最终进入 `HALTED`。重置必须经过人工确认和完整成功对账。

### 17.4 订单、执行与恢复

- 内部 `order_id` 使用 UUID；交易所 `cloid` 使用规范的 `0x` + 32 个小写十六进制字符并唯一持久化。
- 网络尝试与订单业务状态分离，记录在 `execution_actions`。
- 不确定响应进入 `SUBMIT_UNKNOWN` / `CANCEL_UNKNOWN`，先按 cloid 查询真实状态，禁止盲目重发。
- 成交通过 authenticated user stream 接收，并以 REST 增量查询补缺；重复、乱序事件必须幂等。
- 启动对账任一必要查询失败即失败，不能用空列表代替失败。只有无关键未解决差异且数据新鲜时才能进入
  `NORMAL` 并启动策略。

### 17.5 Postgres 数据模型

当前投影：`orders`、`positions`、`account_state`、`system_state`。其中 `positions` 只保存
`(sub_account, symbol)` 唯一的交易所权威账户级投影；策略归因只保存在成交与账本事实中，禁止复制为第二套当前持仓。

不可变事实：`order_events`、`fills`、`ledger_entries`、`risk_events`、`reconciliation_runs`、
`reconciliation_diffs`、`api_audit`。

可靠执行与分发：`execution_commands`、`risk_reservations`、`inbox_events`、`outbox_events`、
`exchange_sync_cursors`。WS 与 REST 历史补缺必须复用相同的 `(source, external_event_id)` inbox key；
游标按账户和流持久化，并用一个时间戳重叠窗口恢复，依赖 inbox 去重，不能仅依赖进程内 checkpoint。

价格、数量、费用、PnL 和 funding 使用 `NUMERIC(38,18)`；所有时间使用 `TIMESTAMPTZ`；schema 只通过
Alembic 管理，应用启动禁止 `create_all`。

### 17.6 API 与前端实时通道

- 新 API 使用 `/api/v1`；query 与 command 分离，mutation 必须带 `Idempotency-Key`。
- placement、reduce-only close 和 cancel 在调用交易执行前，先以 `(actor_id, Idempotency-Key)` 在 Postgres
  `execution_commands` 原子占位。规范化 action/resource/body 的 SHA-256 相同时重放已提交结果；首次命令仍在
  处理时返回原 command；同 key 但请求哈希不同返回 `409 IDEMPOTENCY_KEY_REUSED`，禁止再次执行。
- API mutation 的成功、失败、重放与 key 冲突均写入 `api_audit`；首次命令的完成状态与审计记录在同一
  Postgres 事务提交。mainnet 不允许退回进程内幂等实现。
- 浏览器使用 HttpOnly/Secure/SameSite session；mutation 校验 CSRF 与权限，mainnet 无认证配置时拒绝启动。
- API token/session 使用三级 RBAC：`viewer` 只读，`operator` 可下单、撤单、平仓和启停策略，`admin` 才可
  触发或重置 Kill Switch。旧 `auth_token` 仅作为 admin token 的兼容入口；权限拒绝同样写入 API 审计。
- 每个已配置的 API token 至少 32 个随机字符且互不重复。Dashboard BFF 必须按 viewer/operator/admin
  使用独立 Basic 凭据和对应后端 token；旧单用户配置仅保留 viewer 权限，不能隐式获得 admin。
- HTTP API 对请求、mutation 和认证失败分别限速。公开行情 WebSocket 只接受允许的浏览器 Origin，设置
  全局/每 IP 连接上限、独立小队列和每连接消息速率上限，慢客户端不得放大 EventBus 内存占用。
- mainnet Postgres 连接必须启用 `ssl=require`、`verify-ca` 或 `verify-full`。
- 错误使用稳定错误码和 `application/problem+json`，不向客户端暴露 SDK/数据库异常。
- 控制事件使用支持 `Last-Event-ID` 的可靠 SSE。SSE 的唯一 sequence 与 replay 事实源是 Postgres
  `outbox_events`，不得由进程内 EventBus 自增生成。dispatcher 按 sequence 使用短租约
  `FOR UPDATE SKIP LOCKED` claim，完成 fan-out 后才写 `published_at`；发布后、标记前崩溃允许以同一
  sequence 至少一次重投，broker 与客户端按 sequence 去重。
- SSE 客户端先注册独立有界队列，再按连接时数据库 high-water mark 重放，避免重放/实时切换窗口丢事件；
  单个慢客户端只断开自身。`Last-Event-ID` 早于保留窗口或高于当前 high-water mark 时，服务端发送
  `StreamResyncRequired`，前端全量刷新投影；Postgres identity 的正常空洞不能被前端误判为丢事件。
- 高频盘口、成交和 K 线使用后端 `/ws/v1/market`。
- 浏览器不直连 Hyperliquid，保证前端、策略和落库使用同一标准化行情。
- JSON 中精确数值使用十进制字符串；币种精度由 instrument meta 接口统一提供。
- placement/close 的价格与数量只接受十进制字符串，并在进入执行服务前按 instrument meta 校验 tick size、
  lot size 和 minimum size。前端只在图表坐标等非交易展示边界显式转换为 IEEE-754 number。

### 17.7 上线门禁

- 风控、数据库、执行、对账、SSE 和前端契约单元/集成测试全部通过。
- Kill Switch 触发后不产生新 placement，且可撤销交易所全部挂单。
- 启动对账失败时策略任务数为零。
- timeout/崩溃恢复不产生重复订单，所有 UNKNOWN 在 SLA 内被对账处理。
- testnet 连续至少 14 天无重复单、无未解释仓位差异、无风控绕过后才允许 mainnet。
- `durable_ledger_v2`、`execution_v2`、`user_stream_v2`、`reconciliation_v2`、`api_v1`、
  `strategy_runner_v2` 独立配置并校验依赖；旧执行与 V2 执行互斥。mainnet 默认全部关闭，soak 门禁通过后
  只能显式整组启用交易链，禁止自动回退旧链路。

## 18. 盘口做市系统权威架构（2026-07）

详细算法、数据模型、API、前端和门禁定义见 `docs/market_making_design.md`；实施顺序见
`docs/market_making_implementation_plan.md`。本节冻结跨模块架构决策，若详细文档与本节冲突，以本节为准。

### 18.1 策略与报价决策

- 第一版只允许单 symbol、独立 isolated sub-account、每侧单档 ALO/post-only。
- bid、ask、KEEP 和 `NO_QUOTE` 以一个 quote lifecycle 的增量预期 USDC PnL 联合求解，扣除库存方差、funding、
  flatten、真实 quote diff 动作成本和执行尾部成本；最小预期收益门槛只应用一次。
- 公平价使用 mid、microprice、L1/L5 OFI、短窗 trade flow、bounded return 和 funding carry 的可解释模型；
  预测偏移必须硬封顶，不得把做市隐式变成方向策略。
- Hyperliquid 本地可成交盘口是公平价主锚。Binance 或股票等外部市场只允许作为经合约/计价转换和 EWMA basis 校正后的
  有界参考，不是 Hyperliquid oracle；其贡献必须经过 confidence、freshness decay、最大权重和 ticks/bps cap。外部数据
  stale、质量未知或 basis/divergence 失稳时权重降为 0，并退回纯本地模型或按版本化配置 fail closed。
- 外部领先信号只有在 shadow 实测半衰期显著大于 `receipt -> decision -> durable command -> sign -> ACK` p99 时才允许增加
  fair 权重；否则只能用于异常检测、扩点差和撤单。所有决策记录 source/adjusted price/basis/weight/age/quality。
- reservation price 使用库存名义/soft limit 的有界 bps skew，波动方差与 horizon 单位必须一致；触 soft limit 后
  停止增加该方向库存，触 hard/emergency limit 后只允许降低库存、撤单或受控平仓。

### 18.2 行情与事件语义

- 盘口在 update 时保存不可变 `exchange_ts`、`received_at`、`version` 和连接代次；读取不得刷新时间。
- L2/trade/feature 使用 latest-value/coalescing 通道；fill/order/risk/safety/config 使用 Postgres inbox/outbox
  支撑的可靠通道。策略禁止使用混合 wildcard 队列。
- WS/user stream 断连、stale、gap、队列溢出、空/交叉盘口、event-loop lag 或补缺失败时立即禁止 placement，
  权威撤报价并进入 `CANCEL_ONLY`；完整对账后才可恢复。

### 18.3 QuoteCoordinator 与交易入口

- 策略只产生带 market/config/revision 的 `DesiredQuoteSet`，不得直接调用 `ExecutionEngine`。
- `QuoteCoordinator` 是 quote slot 的唯一所有者，比较 desired 与交易所权威 live/inflight/UNKNOWN orders，执行
  KEEP/PLACE/CANCEL/MODIFY 的最小差异，按真实 child action 重新计算 transition utility，并实施 min lifetime、
  cooldown、hysteresis 和 revision fencing。
- 同侧 UNKNOWN 未解决前禁止补挂；旧单未权威确认撤销前不得释放风险预占。
- 策略、API 和运维命令统一进入 `TradingCommandService`：Safety -> DataHealth -> Risk -> ActionBudget ->
  OrderNormalizer -> durable Postgres transaction -> SignedActionExecutor。
- quote revision 作为 durable batch command 持久化；网络尝试与订单业务状态分离到不可变
  `execution_actions`，timeout 必须查询权威状态，禁止盲目重发。
- placement/modify child 发送前必须重新检查 deadline、连接/行情版本、数据新鲜度、Safety、风险预占和 ALO；
  迟到 revision/orphan/modify UNKNOWN 必须持久化、计入风险并优先撤销。Postgres 故障时仅撤单可走带本地 WAL 的
  emergency cancel path，恢复后补录并对账。

### 18.4 动作额度与风控

- `ActionBudgetController` 分开管理 address child actions、cancel headroom 和 IP weight，按真实 quota owner address
  使用 `userRateLimit` 权威快照、本地 shadow debit 和远端差分校正，维护 burn/earn、USDC/action、runway 和动态 reserve。
- 预算状态为 NORMAL、CONSERVE、CRITICAL、CANCEL_ONLY、EXHAUSTED。撤单永远不被预算 gate 阻断。
- 扩容默认要求 trailing `USDC/action >= 1.25`，并由实时差分校准；低于可持续性门槛时自动降频或停止报价。
- 风控必须按全部 live/inflight/UNKNOWN/new quotes 的最坏成交场景预占，并覆盖 inventory bands、gross exposure、
  margin/liquidation、数据新鲜度、短窗波动/毒性、日亏损、markout、quote age、reject/unknown 和 latency。
- `reserveRequestWeight` 默认关闭，只能由 emergency policy 用于退出风险或 UNKNOWN 恢复，受 admin 权限和成本上限控制。

### 18.5 数据、API 与前端

- Postgres 是订单、成交、配置、策略状态、quote command、风险预占、额度当前投影和恢复状态的事务事实源。
  `positions(sub_account,symbol)` 保持唯一当前仓位投影，不创建第二套策略当前仓位。
- 高频 feature、quote decision、inventory sample、action-credit sample 和 fill markout 追加写 ClickHouse；每次模型计算
  不得写 Postgres。
- 新增 `strategy_instances`、活跃账户/币种排他 allocation、版本化强类型做市配置、runtime/session、quote plan/item、
  quote slot、execution command item/action、逐风险所有者 reservation 和地址级 action budget 表；普通 KEEP/NO_QUOTE
  只写 ClickHouse。schema 仅通过 Alembic
  expand/backfill/fenced-cutover/contract 演进，迁移期保持唯一写者，所有精确数值使用 `NUMERIC(38,18)`。
- Postgres ledger/fill/funding/paid-action 是 Accounting PnL 和风控权威；markout 是执行质量诊断，不能重复计入净 PnL，
  ClickHouse PnL 仅是可重建分析投影，不能驱动 Kill Switch。
- API 重构为多策略实例、版本化配置和 start/pause/resume/drain/stop；mutation 继续强制 RBAC、CSRF、
  Idempotency-Key、revision 和 api_audit。
- 前端新增做市工作台，展示 desired/live/UNKNOWN quotes、库存 skew、PnL 分解、动作 runway、配置 diff 和 stale 状态；
  可靠控制事件走 SSE，高频报价遥测走有界 WebSocket，精确数值使用 decimal string。

### 18.6 验证和扩容

- 顺序固定为：历史 replay/故障注入 -> 至少 14 个完整 UTC 日 mainnet shadow -> testnet 执行恢复并连续 14 天安全
  soak -> mainnet 单币单档最小 size canary -> 只增加 size -> 第二 symbol -> 最后评估第二档。
- L2/queue 仿真只能用于淘汰坏方案；testnet 只证明协议和安全，不证明 mainnet 微观结构盈利。
- 扩大资金必须同时满足至少 30 个完整 UTC 交易日、预注册独立 inventory episode 和 regime coverage；按交易日/episode
  block bootstrap 的 Accounting net edge 95% 置信区间下界大于 0、边际 `USDC/action >= 1.25`、动态 action/cancel/IP
  reserve 充足、硬库存越限/重复订单/关键对账差异为 0，且 UNKNOWN/orphan 均在 SLA 内有终态。
- 第一版保持 Python asyncio 单进程。只有 profile 证明 event-loop/CPU/receipt-to-send 成为瓶颈时才按 WS/订单簿、
  feature/quote、diff/batch、签名热路径顺序迁移 Rust，优先 PyO3，不因“做市”名义提前拆微服务。
