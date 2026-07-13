# HypeEdge 部署与凭证配置

生产部署遵循 fail-closed：mainnet 缺少 Agent Wallet、Postgres 或 API 凭证时，配置加载直接失败；
敏感值不得写入 `configs/*.yaml`、systemd unit 或前端 `NEXT_PUBLIC_*` 变量。

## 纯内网部署（dev / testnet，推荐默认）

本仓库的个人 lab 默认按**纯内网、无鉴权**运行：

| 项 | 约定 |
|---|---|
| 网络边界 | 仅局域网 / 受信私网；**不要**把 API 或 Dashboard 暴露到公网 |
| `HYPE_ENV` | `dev` 或 `testnet` |
| API 监听 | `configs/{dev,testnet}.yaml` 中 `api.host: 0.0.0.0`（可用 `HYPE_API__HOST` 覆盖） |
| API token | **全部留空**：未配置 token 时 `/api` 以本机信任的 `local-admin` 身份运行，不校验 Bearer |
| Dashboard | 默认无鉴权（不要设 `HYPEEDGE_DASHBOARD_AUTH=on`）；页面不弹登录，创建/启停策略直接可用 |
| 行情 WS | `NEXT_PUBLIC_HYPEEDGE_MARKET_WS_URL=ws://<主机局域网IP>:37001`；Origin 须在 `api.cors_origins` |
| Next 代理 | `HYPEEDGE_BACKEND_URL=http://127.0.0.1:37001`（同机 loopback，不要写成局域网 IP） |

约束：

- **一旦配置任一 `HYPE_API__*TOKEN`，所有 `/api` 请求都必须带合法 Bearer**；此时须在 `web/.env.local` 设 `HYPEEDGE_DASHBOARD_AUTH=on` 并配置完整 Basic Auth 三元组，否则代理无法转发。
- 若 `.env` 已清空 token 但后端未重启，进程仍会按旧配置要求 Bearer；修改鉴权相关配置后必须重启 `hypeedge` 与 `pnpm dev`。
- **mainnet 不适用无鉴权模式**：必须配置 admin API token；非回环监听时同样强制 token。
- 行情 WebSocket（`/ws/v1/market`）本身是只读公开通道，靠 `cors_origins` 与连接限速约束，与 HTTP `/api` 鉴权开关独立。
- **内网单人**：请将根目录 `.env` 中所有 `HYPE_API__*TOKEN` / `HYPEEDGE_DASHBOARD_*` / `HYPEEDGE_*_API_TOKEN` 留空，且不要设置 `HYPEEDGE_DASHBOARD_AUTH=on`。

最小内网启动：

```bash
# 仓库根
set -a && source .env && set +a
uv run alembic upgrade head
uv run hypeedge

# 另一终端
cd web && pnpm dev
# 浏览器打开 http://<局域网IP>:34001
```

## mainnet 必需变量

| 变量 | 用途 | 要求 |
|---|---|---|
| `HYPE_ENV` | 选择环境 | mainnet 使用 `mainnet` |
| `HYPE_EXCHANGE__ACCOUNT_ADDRESS` | Hyperliquid 账户地址 | 只使用 Agent/API Wallet 对应账户 |
| `HYPE_EXCHANGE__AGENT_PRIVATE_KEY` | 交易签名 | 主钱包私钥永不进入交易进程 |
| `HYPE_POSTGRES__URL` | 事务事实源 | 独立数据库用户、强随机密码、TLS/受信网络 |
| `HYPE_API__VIEWER_TOKEN` / `OPERATOR_TOKEN` / `ADMIN_TOKEN` | 后端分级 Bearer token | 每个至少 32 个随机字符且互不重复；mainnet 必须有 admin |

`HYPE_POSTGRES__URL` 不在 `configs/mainnet.yaml` 中提供默认值。mainnet 还会拒绝 `hypeedge`、
`postgres`、`password`、`changeme` 等弱默认密码。可用以下命令生成 API token：

```bash
python -c 'import secrets; print(secrets.token_urlsafe(48))'
```

推荐通过主机 secret manager 或权限为 `0600` 的 `EnvironmentFile` 注入。不要把真实值写进命令历史、
日志或仓库；修改凭证后同时重启后端和 Dashboard。

## Dashboard Basic Auth

Next.js 仅在服务端读取以下变量：

| 变量 | 用途 |
|---|---|
| `HYPEEDGE_BACKEND_URL` | 后端内网地址，默认 `http://127.0.0.1:37001` |
| `HYPEEDGE_{VIEWER,OPERATOR,ADMIN}_API_TOKEN` | 按登录角色转发给后端，分别匹配对应 `HYPE_API__*TOKEN` |
| `HYPEEDGE_DASHBOARD_{VIEWER,OPERATOR,ADMIN}_USERNAME` | 对应角色的 Dashboard Basic Auth 用户名 |
| `HYPEEDGE_DASHBOARD_{VIEWER,OPERATOR,ADMIN}_PASSWORD` | 对应角色的强随机密码 |

每个角色的用户名、密码、后端 token 必须三者同时设置；部分配置返回 `503`，未认证请求返回 `401`。

**内网单人默认**：不设置 `HYPEEDGE_DASHBOARD_AUTH`，Dashboard 无 Basic Auth，BFF 以 admin 放行。

仅在需要鉴权时设 `HYPEEDGE_DASHBOARD_AUTH=on`，并配置三组
`HYPEEDGE_DASHBOARD_{VIEWER,OPERATOR,ADMIN}_{USERNAME,PASSWORD}`（不得用一个 Basic 用户共享 admin token）。
旧 `HYPEEDGE_DASHBOARD_USERNAME/PASSWORD` 仅映射为只读 viewer。启用鉴权后，创建策略需要 operator。

所有 `HYPEEDGE_*` 凭证都不能添加 `NEXT_PUBLIC_` 前缀，否则会进入浏览器 bundle。

## 最小启动检查

```bash
uv run alembic upgrade head
uv run hypeedge
```

上线前确认：

- **纯内网 lab（dev/testnet）**：API 可绑 `0.0.0.0` 且不配 token；确认防火墙/路由不会把 34001/37001 暴露到公网；
- **mainnet**：后端 API 只绑定 loopback，或位于 TLS 反向代理/受信私网之后，且已配置 admin token；
- Dashboard：内网无鉴权可留空凭证；若启用了 API token，则须同时配置 Dashboard Basic Auth；
- Postgres schema 已迁移且数据库不可用时交易保持关闭；
- testnet 门禁全部通过后才切换 `HYPE_ENV=mainnet`。

## 做市发布阶梯

做市发布顺序固定，不允许跳级：

```text
历史 replay + 故障注入
-> 至少 14 个完整 UTC 日 mainnet shadow
-> testnet 完整执行/恢复 + 连续至少 14 个完整 UTC 日 clean soak
-> mainnet 单 symbol、单档、最小合规 size canary
-> 只增加 quote size
-> 第二 symbol
-> 最后才评估第二档
```

mainnet shadow 必须覆盖预注册的波动、流动性、流量和 funding regime，并保存 NO_QUOTE、候选报价、
markout、库存 episode 和预计动作 runway。shadow 只用于淘汰候选，不证明实盘盈利；shadow fill 不得写入正式
orders、fills、positions 或 ledger。

testnet 用于验证 batch partial result、nonce、UNKNOWN、重启、WS/user stream 断连、Postgres/ClickHouse
故障、三预算不足、Kill Switch、pause/drain、配置切换和完整对账。clean soak 期间重复订单、未解释仓位/
对账差异和风控绕过必须均为零。testnet 成交不能证明 mainnet 微观结构收益。

仓库中的机器可读模板
[`market_making_release_gates.yaml`](../configs/operations/market_making_release_gates.yaml)
明确将所有 real-time soak 完成标志保留为 `false`。截至本文档更新，项目没有声称已完成真实 14 天 shadow、
真实 14 天 clean testnet 或真实 30 天扩容观察窗口；只有带 UTC 起止时间、版本和审阅人的外部证据才能改变该结论。

## Canary 与逐级扩容

首次 mainnet canary 只能使用一个经 shadow 数据选择的 symbol、一个 isolated sub-account、每侧一档、最小
合规 size 和 1x/极低杠杆。启动前必须在 Postgres 激活不可变、版本化的 `CanaryRiskEnvelope`，至少限制：

- 部署权益和 live quote notional；
- 日亏损、累计亏损、日动作和总动作；
- 最低远端 action credits 和 cancel headroom；
- forced flatten 次数/成本和 UNKNOWN SLA；
- 最长持续时间、最大成交量；
- 自动 `PAUSED`、`CANCEL_ONLY`、`HALTED` 条件。

每次扩容至少观察 30 个完整 UTC 交易日，并满足预注册的独立 inventory episode 数和 regime coverage。按交易日
或 inventory episode 做 block bootstrap 后，Accounting net edge 的 95% CI 下界必须大于零；trailing
marginal USDC/action 必须至少为 `1.25`，动作/cancel/IP reserve 足以覆盖下一观察窗口；hard inventory breach、
重复订单和关键 reconciliation diff 必须为零，所有 UNKNOWN/orphan 必须在 SLA 内有审计终态。

一次变更只允许扩大一个风险维度，并保存 target、previous/rollback 配置版本及独立观察窗口 ID：

1. 只增加 quote size；
2. 再增加第二 symbol（默认新 strategy instance + 独立 sub-account）；
3. 最后评估第二档；
4. Rust 热路径迁移独立进行，不与 size、symbol 或档位同时变化。

任一硬门槛失败时立即应用当前版本化 envelope 给出的 directive，撤回到上一配置版本，并重新开始独立观察窗口。
不得通过修改 Prometheus/ClickHouse 投影来解除门禁；交易、额度、配置、PnL 和对账的权威证据均来自 Postgres/
交易所事实及带版本的审阅产物。

## Prometheus 与 Grafana

做市指标通过 `hype_mm_*` 暴露，包括 feed/user stream/account/credit freshness、fair/reservation、
desired/live quote、quote age/uptime、库存 band、margin/liquidation distance/funding、动作 burn/earn、
USDC/action/runway/reserve、reject/UNKNOWN、关键路径 latency、reconciliation diff、strategy/config 和
canary directive。启用外部参考价时还包括 source freshness、raw/adjusted price、basis、相对 Hyperliquid 本地主锚的
divergence、confidence/effective weight 和 one-hot quality；这些值仅用于观察，外部市场不是 Hyperliquid oracle。

- Grafana dashboard：[`hypeedge-market-making.json`](../configs/grafana/dashboards/hypeedge-market-making.json)
- Prometheus 告警规则：[`market_making_alerts.yml`](../configs/prometheus/market_making_alerts.yml)
- 运维门禁模板：[`market_making_release_gates.yaml`](../configs/operations/market_making_release_gates.yaml)

Prometheus 是实时健康和告警投影，不是订单、PnL、额度、配置或发布审计事实源。Grafana 中的价格值经过
Prometheus 浮点边界，仅用于观察，不能回流交易计算。部署时将 alert rules 挂载到 Prometheus，并将 dashboard
provision 到 UID 为 `prometheus` 的数据源；若环境使用其他 UID，应在导入时映射数据源。

## P0 告警处置

收到以下任一 P0 告警时，先禁止 placement，再确保撤单和权威对账继续可用：

- stale/gap 或 user stream loss；
- 外部参考 stale 但 effective weight 未归零；
- hard/emergency inventory、危险 margin/liquidation；
- UNKNOWN 超 SLA；
- emergency cancel-all 失败；
- Postgres 不可用；
- address credits 进入 emergency reserve、cancel headroom 耗尽；
- 关键 reconciliation diff；
- canary directive 收紧或 Kill Switch 触发。

统一处置顺序：

1. 确认 supervisor/CanaryRiskEnvelope 已进入 `PAUSED`、`CANCEL_ONLY` 或 `HALTED`，且没有新 placement；
2. 从交易所权威 open orders 核对并撤销全部相关挂单；Postgres 不可用时仅使用 fsync WAL emergency cancel path；
3. 保存告警结构化 payload、strategy/config/envelope revision、UTC 时间和操作人；
4. 修复数据源后执行完整 reconciliation，UNKNOWN/orphan 必须有终态；
5. 仅在账户、行情、credit freshness 和 reconciliation 全部恢复后，按审批流程恢复；
6. 发生 Kill Switch、漏撤、重复单、风控绕过或未解释仓位差异时，本观察窗口作废并回滚。

外部 basis/divergence 超限的 warning 先验证 symbol/合约映射、计价币、时间戳与 EWMA basis 状态，再降低外部 confidence/
weight。不得通过提高 cap、关闭 stale decay 或直接追随外部价来消除告警；若外部源仍异常，退回纯 Hyperliquid 本地模型。
