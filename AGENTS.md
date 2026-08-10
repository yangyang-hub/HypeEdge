# AGENTS.md — HypeEdge 项目指南

## 项目概述

HypeEdge 是一个面向 Hyperliquid 永续合约交易所的个人量化交易系统。**后端已全部重写为 Rust**（tokio + axum + sqlx），Python 代码已彻底移除。设计文档：`docs/design.md`（架构决策的权威来源，修改前必读）。

## 技术栈

- **语言**：Rust 1.93（edition 2024），workspace 多 crate
- **异步**：tokio（rt-multi-thread），业务逻辑不引入线程
- **API**：axum 0.8 + tower/tower-http（CORS、request-id、trace）
- **存储**：ClickHouse（行情时序，`clickhouse` crate）、Postgres（订单/持仓事务，sqlx 0.8 原始 SQL + 迁移 `crates/storage/migrations/*.sql`）、DuckDB（离线研究）
- **数值**：手写 i128 定点 `Decimal`（scale 18，ethnum::I256 中间值），见 `crates/domain/src/decimal.rs`
- **签名**：EIP-712/L1 phantom agent 签名，k256 + sha3/hex + rmp-serde（非社区 HL SDK）
- **配置**：分层加载（env > .env > YAML > 默认），`crates/config`
- **日志**：tracing + tracing-subscriber（JSON 结构化）
- **监控**：prometheus
- **Lint**：cargo fmt + cargo clippy（-D warnings）
- **测试**：cargo test（单元 + 黄金语料 parity + 集成）

## 项目结构

```
crates/
├── domain/       # 纯类型：Decimal、enums、models、events、error、durable traits（无 IO）
├── config/       # 配置加载：settings（分层优先级）、loader
├── infra/        # EventBus（事件总线）、共享基础设施
├── storage/      # Postgres 事务存储（durable order store、command queue、outbox、
│                 #   config version、quote plan store、system state）、ClickHouse writer、
│                 #   DuckDB 导出、去重、检查点
├── trading/      # 交易核心：market_data、execution、risk、account、strategy、backtest、
│                 #   monitor、trading（quote/command service）、funding_arb、market_maker
├── api/          # axum 路由：system/risk/market/account/strategies、SSE、WebSocket
└── app/          # 二进制入口：配置装配、kill switch、HTTP 服务器
docs/             # 设计文档
configs/          # 环境配置文件
web/              # Next.js 前端仪表盘
```

**依赖方向**：`domain ← infra ← storage/trading ← api ← app`，trading 只依赖 domain 的 trait，不依赖具体存储实现（通过 trait 注入，内存 fake 可测）。

## 常用命令

```bash
cargo check --workspace           # 编译检查
cargo test --workspace            # 全部测试（单元 + parity）
cargo clippy --workspace --all-targets -- -D warnings   # Lint
cargo fmt --all                   # 格式化
cargo run -p hypeedge_app         # 启动应用（HYPE_ENV=dev|testnet|mainnet）
cargo test -p hypeedge_domain --test decimal_corpus    # Decimal 黄金语料
make lint && make test            # 一键检查
```

## 编码规范

### 通用

- 遵循现有代码风格：注释密度、命名习惯、import 顺序。
- 所有公共函数和类型必须带类型注解（Rust 类型系统自带）。
- 文档注释用 `//!`（模块）和 `///`（item），引用 `[`Type`]` 链接。
- 错误用 `HypeEdgeError`（`crates/domain/src/error.rs`，thiserror），不 panic（库代码），`.unwrap()` 仅限编译期不变量或测试。
- 关键操作绑定 contextvars（cloid、strategy_id）到 tracing span。

### 异步

- 所有 I/O 用 async/await，业务逻辑不引入 `std::thread`。
- 长时间任务用 `tokio::spawn`，CPU 密集用 `spawn_blocking`。
- 数据保护优先 `std::sync::Mutex`，仅在跨 `.await` 持锁时用 `tokio::sync::Mutex`。

### 数据模型

- 用 `crates/domain/src/decimal.rs` 的语义类型（`Decimal`、`Price`、`Size`、`Usd`），不直接用裸 f64/i128。
- 领域模型在 `crates/domain/src/models.rs`，用 `#[derive(Debug, Clone, PartialEq)]` 结构体。
- 枚举在 `crates/domain/src/enums.rs`，带 `as_str()` 和状态机校验。
- 模块间通信通过 EventBus（`crates/infra/src/event_bus.rs`）的 `DomainEvent`，不直接调用其他模块方法。

### 配置

- 新增配置项在 `crates/config/src/settings.rs` 对应 Settings 结构体添加，带默认值和校验（`validate()`）。
- 三个环境配置文件 `configs/*.yaml` 须同步更新。
- 密钥/私钥只通过环境变量传入，不写进代码或 YAML。

## 模块间通信模式

```
market_data ──publish──▶ EventBus ──queue──▶ strategy
                                              │
                                         OrderIntent
                                              │
                                              ▼
                              risk(同步内联, 500ms超时, fail-safe)
                                              │
                                              ▼
                              execution(串行nonce队列) ──▶ Hyperliquid
                                              │
                                              ▼
                              account/reconciler ◀── 交易所对账
```

- **EventBus** 是唯一的模块间通信通道（发布/订阅，bounded mailbox per subscriber）。
- 策略通过注入的 `ExecutionClient` trait 提交订单意图，不直接访问 ExecutionEngine。
- 风控在执行路径中同步内联，超时 = 拒绝（fail-safe）。
- 所有签名操作汇聚到 NonceQueue 的单队列串行处理。

## Hyperliquid 平台关键约束（必须遵守）

- **IP 权重**：1200 weight/min，轻量端点(l2Book等)权重2，普通info权重20，exchange = 1 + floor(batch/40)。
- **地址动作额度**：初始10,000，1动作/USDC成交量，额度耗尽系统停摆。做市前必须估算额度消耗。
- **按条目权重**：fundingHistory 等端点每返回20条+1权重，candleSnapshot每60条+1。回填需分页限速。
- **expiresAfter 过期撤单**：5x 动作额度惩罚，尽量避免。
- **挂单≥1000** 时：reduce-only 和止损单被拒，网格/做市须留额度。
- **WS 限制**：10连接/IP，1000订阅，2000 msg/min。
- **funding 每小时结算**，非币安 8h。

## 测试要求

- 每个 bug fix 和新功能必须有对应测试。
- 单元测试用 `#[cfg(test)] mod tests` 内联在模块内，或 `crates/*/tests/` 集成测试。
- 黄金语料 parity 测试：`crates/domain/tests/decimal_corpus.rs`、`crates/config/tests/config_parity.rs`，fixtures 在 `crates/domain/tests/fixtures/`。
- 测试异步代码用 `#[tokio::test]`。
- 测试风控逻辑用已知输入/输出覆盖边界条件。
- 测试订单状态机验证每个状态转换的合法性。
- 核心逻辑覆盖率目标 ≥ 90%。

## 安全红线

- 主钱包私钥永不进交易进程。
- agent wallet 私钥只通过环境变量传入，不硬编码。
- `configs/mainnet.yaml` 在 `.gitignore` 中，不入版本控制。
- `.env` 不入版本控制。
- 下单前必须通过风控检查，无例外。

## 修改时的注意事项

- 修改 `crates/domain/src/enums.rs` 的枚举值时，同步更新其 `as_str()` 和任何状态机转移逻辑。
- 新增 EventBus 事件类型时，在 `DomainEvent` 枚举、`EventType` 及 `ALL_EVENT_TYPES` 中注册。
- 修改 ClickHouse 表结构时，同步更新 `crates/storage/src/clickhouse_writer.rs` 的 DDL 和 `docs/design.md` §5.2。
- 修改 Postgres 表结构时，更新 `crates/storage/migrations/*.sql`（sqlx migrate）。
- 新增模块接口时，先定义 trait（domain 层），在骨架文件中占位，再实现。
- 修改配置结构后，运行 `cargo test -p hypeedge_config` 验证。

---

## 编码实现规范

详细的编码规范存放在 `rules/` 目录下，按前后端分离：

- **后端规范**：[`rules/backend.md`](rules/backend.md) — Rust 后端的架构约束、类型系统、异步模式、EventBus、风控、执行引擎、存储层、配置、日志、错误处理、测试规范。
- **前端规范**：[`rules/frontend.md`](rules/frontend.md) — Next.js + React + shadcn/ui 的组件设计、类型系统、数据获取、样式、性能、API 契约、测试规范。

### 前后端共享约束

#### 通用原则

- **先设计文档，后写代码**：新功能先更新 `docs/design.md`，再实现。
- **先接口，后实现**：trait 先定义，测试可 mock。
- **先测试，后上线**：单元测试通过 → 集成测试通过 → testnet 验证 → mainnet。
- **最小变更原则**：每次 commit 只做一件事，方便回滚和 review。

#### 数据一致性

- 前端显示的价格/数量精度与后端一致：
  - 价格：2 位小数（BTC）或 4 位小数（山寨币），由后端 `meta` 接口提供精度信息。
  - 数量：按币种精度显示，不自行截断。
  - 百分比：2 位小数 + `%`（如 `12.34%`）。
- 时间显示统一为 UTC + 本地时区转换，格式 `YYYY-MM-DD HH:mm:ss`。
- PnL 颜色：全球统一（正=绿、负=红），不使用地区性红绿反转。

#### 错误处理

- 后端错误通过 API 响应码 + 结构化错误信息传递。
- 前端对每个 API 调用做错误处理，显示 toast 通知用户。
- 网络断连时前端显示连接状态指示器 + 最后更新时间。
- 后端 kill switch 触发时，前端全屏红色告警横幅。

#### 版本同步

- 后端新增/修改 API 时，同步更新前端 `lib/types.ts`。
- 后端新增配置项时，前端设置页面同步添加对应控件。
- 后端新增事件类型时，评估是否需要前端实时展示。
