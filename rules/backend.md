# 后端编码实现规范（Rust）

## 通用规则

- 遵循现有代码风格：注释密度、命名习惯、import 顺序。
- 模块用 `//!` 文档注释说明职责，公共 item 用 `///`。
- 库代码不 panic；`.unwrap()` 仅限编译期不变量或测试。
- 错误用 `HypeEdgeError`（`crates/domain/src/error.rs`，thiserror），不抛裸错误。

## 架构约束

- **模块化单体**：所有模块在同一进程内，通过 EventBus 通信，不引入进程间调用。
- **依赖方向**：`domain ← infra ← storage/trading ← api ← app`，单向依赖，禁止反向调用。
- **依赖注入**：模块通过构造函数接收依赖（`Arc<dyn Trait>`、EventBus 等），不在模块内直接依赖具体存储实现。
- **trait 先于实现**：新增功能先在 `crates/domain/src/traits.rs` 定义 trait，再用内存 fake 测试，最后实现 Postgres/ClickHouse 版本。

## 类型系统

```rust
// ✅ 正确：使用语义类型
use hypeedge_domain::decimal::{Decimal, Price, Size, Usd};

pub fn calculate_pnl(entry: Price, exit: Price, size: Size) -> Usd { ... }

// ❌ 错误：使用裸类型
pub fn calculate_pnl(entry: f64, exit: f64, size: f64) -> f64 { ... }
```

- 定点数值：`crates/domain/src/decimal.rs`（i128 scale 18，`Decimal` + `Price`/`Size`/`Usd` 语义包装）。
- 领域模型：`crates/domain/src/models.rs`（`#[derive(Debug, Clone, PartialEq)]` 结构体）。
- 枚举：`crates/domain/src/enums.rs`（带 `as_str()` 和状态机校验）。
- 错误层级：`crates/domain/src/error.rs`（`HypeEdgeError`，thiserror 派生）。

## 异步模式

```rust
// ✅ 正确：tokio 原生 async
pub async fn fetch_candles(symbol: &str) -> Result<Vec<Candle>, HypeEdgeError> {
    let body = json!({ "type": "candleSnapshot", "req": { "coin": symbol } });
    let response = http.post("/info").json(&body).send().await?;
    Ok(response.json::<Vec<Candle>>().await?)
}

// ❌ 错误：阻塞事件循环
pub fn fetch_candles(symbol: &str) -> Vec<Candle> {  // 非 async，阻塞
    ...
}
```

- 所有 I/O（HTTP、数据库、WebSocket）必须 async。
- CPU 密集或同步库调用用 `tokio::task::spawn_blocking`。
- 后台常驻任务用 `tokio::spawn`。
- 数据保护优先 `std::sync::Mutex`；仅跨 `.await` 持锁时用 `tokio::sync::Mutex`。

## EventBus 使用

```rust
use hypeedge_domain::events::{DomainEvent, Event, EventType};

// 发布
bus.publish_sync(Arc::new(Event::new(DomainEvent::L2BookUpdate(snapshot))));

// 订阅
let mailbox = bus.subscribe(EventType::L2BookUpdate);
while let Some(event) = mailbox.recv().await {
    handle(event).await;
}

// 多类型订阅（保持顺序）
let mailbox = bus.subscribe_many(&[EventType::TradeUpdate, EventType::FundingUpdate]);
```

- 使用 `crates/domain/src/events.rs` 的 `DomainEvent` / `EventType`，不硬编码事件名。
- 新事件类型必须加入 `DomainEvent` 枚举、`EventType` 及 `ALL_EVENT_TYPES`。
- payload 类型必须与事件类型文档一致（如 `L2BookUpdate` 的 payload 是 `L2BookSnapshot`）。
- 高频行情事件（l2Book, trades）用 `publish_sync`（非阻塞），低频控制事件可用 `await publish`。

## 风控实现规范

- 风控检查必须在 `RiskLimits.timeout_ms`（默认 500ms）内返回。
- 超时 = 拒绝（fail-safe），不放过任何订单。
- 风控数据源不可用时，策略降级为**只撤不下**模式。
- 风控模块异常 = 触发全局 kill switch。

## 执行引擎规范

- 所有订单必须带 `cloid`（通过 `crates/trading/src/execution/cloid.rs` 的 `CloidGenerator` 生成）。
- 禁止盲目重发：下单超时后先按 cloid 查询真实状态（`SUBMIT_UNKNOWN` 降级到对账）。
- Nonce 必须串行：所有签名经 `NonceQueue` 单队列（`crates/trading/src/execution/nonce.rs`）。
- 订单状态机转换必须合法（参考 `crates/domain/src/enums.rs` 的 `OrderStatus` 与状态机）。

## 存储层规范

- ClickHouse：只用于追加型时序数据（行情、成交、K线、funding），不做 UPDATE/DELETE。
- Postgres：用于事务性数据（订单、持仓、PnL），所有写操作在事务内。
- Postgres 迁移：`crates/storage/migrations/*.sql`（sqlx migrate 顺序执行）。
- 存储实现实现 domain 的 trait（如 `DurableOrderStore`、`ConfigVersionStore`、`QuotePlanStore`），通过 `Arc<dyn Trait>` 注入。

## 配置规范

```rust
// ✅ 正确：在 Settings 结构体中定义，带默认值和校验
pub struct RiskSettings {
    pub max_leverage: u32,
}

impl RiskSettings {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=50).contains(&self.max_leverage) {
            return Err("max_leverage must be in [1, 50]".into());
        }
        Ok(())
    }
}

// ❌ 错误：硬编码或裸常量
pub const MAX_LEVERAGE: u32 = 5;
```

- 所有配置项在 `crates/config/src/settings.rs` 中定义，带默认值和 `validate()`。
- 三环境 YAML 文件同步更新。
- 敏感信息（密钥、私钥）只通过环境变量传入。

## 日志规范

```rust
// ✅ 正确：结构化日志 + 业务上下文
tracing::info!(cloid = %order.cloid, symbol = %order.symbol, side = order.side.as_str(), "order_submitted");
tracing::warn!(remaining = credits, watermark, "action_credits_low");
tracing::error!(table, error = %e, rows, "ch_flush_error");

// ❌ 错误：格式化字符串日志
tracing::info!("Order {} submitted for {}", order.cloid, order.symbol);
```

- 使用 tracing 的结构化键值对。
- 每条日志包含足够的上下文（cloid、symbol、strategy_id）用于追踪。
- 生产环境用 JSON formatter，开发环境用 fmt formatter。
- 关键事件（kill switch、大额亏损、对账不一致）同时写入审计日志。

## 错误处理

```rust
// ✅ 正确：使用 HypeEdgeError + 明确信息
return Err(HypeEdgeError::order_rejected(
    format!("Insufficient margin: need {required}, have {available}"),
    Some(order.cloid.clone()),
    Some("insufficient_margin".into()),
));

// ❌ 错误：裸 String + 模糊信息
return Err("Order failed".into());
```

- 使用 `crates/domain/src/error.rs` 的 `HypeEdgeError` 层级。
- 错误消息包含：发生了什么、涉及的实体（cloid/symbol）、具体的数值。
- 可恢复错误：重试 + 退避。
- 不可恢复错误：告警 + 降级或停机。

## 测试规范

```rust
// 单元测试：测试纯逻辑，不依赖外部服务
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_to_submitted_is_valid() {
        let sm = OrderStateMachine::new();
        let mut order = make_order(OrderStatus::Pending);
        sm.transition(&mut order, OrderStatus::Submitted).unwrap();
        assert_eq!(order.status, OrderStatus::Submitted);
    }

    // 异步测试
    #[tokio::test]
    async fn event_bus_publish_reaches_subscriber() {
        let bus = EventBus::new(16);
        let mailbox = bus.subscribe(EventType::TestEvent);
        bus.publish_sync(Arc::new(Event::new(DomainEvent::TestEvent("data".into()))));
        let event = mailbox.recv().await.unwrap();
        assert_eq!(event.payload, ...);
    }
}
```

- 单元测试用 `#[cfg(test)] mod tests` 内联在模块内。
- 黄金语料 parity 测试：`crates/domain/tests/`、`crates/config/tests/`，fixtures 在 `crates/domain/tests/fixtures/`。
- 集成测试（需 Postgres/ClickHouse/网络）放 `crates/*/tests/`，用真实服务或 mock。
- Mock 外部依赖（交易所 API、数据库），不 mock 内部模块。
