# Mainnet 资金费率套利 · 首次实盘检查清单

> 面向真实资金。任何一步未满足，系统要么无法启动（fail-closed），要么策略卡在入场门槛不出单。
> 本文是操作指引，不是代码审查；部署前先过一遍 [deployment.md](./deployment.md)。

## 0. 前置：代码层已解锁（本次已完成）

- `crates/config/src/settings.rs` validator：`funding_arb_execution_enabled` 允许 `testnet` / `mainnet`（dev 仍禁止）。
- `crates/app/src/runtime.rs`：funding_arb 依赖（LiveFundingArbScanner / InstrumentMeta / cycle store）由
  `funding_arb_execution_enabled` 控制装配，不再写死 `is_testnet`。
- `configs/mainnet.yaml`：完整 V2 交易链 + `funding_arb_execution_enabled: true` 已开启。
- mainnet 交易硬禁用闩（runtime.rs:119-127）：`HYPE_ENV=mainnet` 启动必须显式设置 `HYPE_MAINNET_TRADING_ENABLED=1`（见 §1）。
- 配套单测通过（`crates/config/tests/config_parity.rs`）。

## 1. 环境变量（.env 或 secret manager）

```bash
# 网络 —— 决定加载 configs/mainnet.yaml 与官方 mainnet API/WS
HYPE_ENV=mainnet

# ⚠️ 唯一解除 mainnet 交易硬禁用的开关（crates/app/src/runtime.rs:119-127）：
# 未设置时 mainnet 启动直接报错拒绝。确认下方全部准备就绪后再置 1。
HYPE_MAINNET_TRADING_ENABLED=1

# Agent/API Wallet（主钱包私钥永不进交易进程）
HYPE_EXCHANGE__ACCOUNT_ADDRESS=<mainnet 账户地址>
HYPE_EXCHANGE__AGENT_PRIVATE_KEY=<agent 私钥>

# mainnet 独立数据库（必须与 testnet 分开、TLS、强随机密码）
# loader 会拒绝 hypeedge/postgres/password/changeme 等弱密码，且必须 ssl=require/verify-ca/verify-full
HYPE_POSTGRES__URL=postgresql://<user>:<强密码>@<host>:5432/<mainnet_db>?ssl=require

# API token（mainnet 必须有 admin，全部 ≥32 随机字符）
# openssl rand -base64 32   # 或任意 ≥32 字符随机串
HYPE_API__VIEWER_TOKEN=<...>
HYPE_API__OPERATOR_TOKEN=<...>
HYPE_API__ADMIN_TOKEN=<...>
```

其余 `HYPE_CLICKHOUSE__*`、`HYPE_LOG_LEVEL` 等按现有 `.env` 结构复制，仅替换上述 mainnet 项。

## 2. 数据库迁移

```bash
HYPE_ENV=mainnet cargo run -p hypeedge_app   # 启动时自动运行 sqlx migrate（crates/storage/migrations/*.sql）
```

## 3. 重建策略实例（重要）

`fa1` 等策略实例存在 **testnet 库**（`hypeedge_testnet`），主网库是空库，需要重建：

```bash
# 通过后端 API（此时 HYPE_ENV=mainnet）：
# POST /api/v1/strategies  { strategy_id, strategy_type: "funding_arb", initial_config: {...} }
# 再 POST .../config-versions + .../activate 上传 mainnet-ready 参数
```

建议参数（与之前 testnet 验证一致，首次实盘**先降到最小档**）：

| 参数 | 值 | 说明 |
|---|---|---|
| `entry_funding_rate` | `0.0001` | ≥1bp/h 才入场 |
| `exit_funding_rate` | `0` | funding 归零即平 |
| `max_notional_usd` | `25`（首次）/ `500`（稳定后） | 部署上限 1000，首次最小档 |
| `hedge_ratio` | `1` | 1:1 对冲 |
| `leverage` | `1`（首次）/ `2`（稳定后） | 首次 1x 最保守 |
| `max_slippage_bps` | `30` | 两腿 taker 滑点上限 |
| `min_expected_edge_bps` | `3` | 扣费后净边 ≥3bps |
| `expected_hold_hours` | `48` | 与 mainnet funding 结算周期匹配 |
| `round_trip_fee_bps` | `10` | 实际 taker 费用 ≈7bps + 余量 |

## 4. 资金准备

- **现货 USDC**：mainnet 现货账户必须有 USDC。首个仓位需 `max_notional_usd × (1 + max_slippage)`。
  - `25` 档 → ≥ ~25.1 USDC；`500` 档 → ≥ ~505 USDC。
  - 通过 Hyperliquid 官方 UI / `usdClassTransfer` 划入，运行时不会自动转账。
- **perp 保证金**：`notional / leverage`（1x 即 25 USDC）。账户总 equity 需同时覆盖两腿。
- **Action credits**：mainnet 地址有真实动作额度（初始 10,000），每次下单消耗；确认余额充足再跑。

## 5. 启动

```bash
HYPE_ENV=mainnet cargo run -p hypeedge_app
```

## 6. 上线前逐项确认（fail-closed 自检）

- [ ] `HYPE_ENV=mainnet` 生效（日志 `environment=mainnet`）。
- [ ] 配置加载不报错（loader 门禁：Agent Wallet / Postgres TLS / API token）。
- [ ] Postgres schema 已迁移，DB 可用。
- [ ] 启动对账成功 → `trading_enabled=true`；`system_state` 不处于 `cancel_only`。
- [ ] Kill Switch 未触发；账户健康通过；动作额度新鲜。
- [ ] 策略实例已建、`desired_state=running`、effective config 指向新版本。
- [ ] 策略运行时 `live_enabled=true`（依赖已注入，而非观察模式）。
- [ ] 现货 USDC / perp 保证金余额满足入场要求。

## 7. 首次实盘观察（最小档全链路）

以 `max_notional_usd=25`、`leverage=1` 跑第一单，逐项验证：

- [ ] scanner 发现候选市场（funding ≥ entry、book 新鲜、四侧深度够）。
- [ ] 现货腿买入成交，perp 腿卖出成交。
- [ ] 对账一致：本地持仓 = 交易所持仓，cycle 进入 `OPEN`。
- [ ] 持仓期间 funding 结算正常入账。
- [ ] 达到 exit 条件（funding ≤0）时自动平仓，cycle 转 `CLOSED`。
- [ ] 全程无 UNKNOWN 订单；若出现，复核 fault 路径与 `max_unhedged_seconds` 处理。

## 8. 回滚 / 退出

- **停单**：后端 API 将策略 `desired_state` 置 `stopped`（runtime 会尝试先平两腿，见 `runtime.stop()`）。
- **彻底停**：设置 `funding_arb_execution_enabled=false`（mainnet.yaml 或 `HYPE_FEATURES__FUNDING_ARB_EXECUTION_ENABLED=false`）重启。
- **紧急**：Kill Switch；触发后系统自动撤全部挂单并拒绝新单。

## 9. 风险备注

- mainnet funding 目前主流币 ≈0.03%/天（0.0000125/h），低于 `entry_funding_rate=0.0001` → **不会触发**，属正常"等机会"。
- 真实 funding 机会通常出现在资金费率失衡的短窗口；策略只在满足全部门槛时才下单。
- testnet 无真实现货盘口，**testnet 无法完整验证实盘成交链路**；首次 mainnet 最小档 = 实际的全链路验证，务必按第 7 节逐项确认后再放大。
