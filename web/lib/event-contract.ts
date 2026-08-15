// SSE 事件契约表 — 前后端事件名对照清单
//
// 权威来源：crates/domain/src/events.rs 的 `ALL_EVENT_TYPES`（29 个 PascalCase
// 字符串，与 `EventType::as_str()` 逐字一致）。这些拼写会原样出现在 SSE/WS
// 帧与 durable outbox 中。
//
// 契约测试 `lib/event-contract.test.ts` 锁定两个方向：
//   1. 后端全部事件名 ∈ 前端 EVENT_KEYS（改名/删除会红）；
//   2. EVENT_KEYS 中所有 PascalCase 键 ∈ 后端事件名（前端拼错会红）。

/** 后端 29 个事件类型字符串（必须与 events.rs `ALL_EVENT_TYPES` 保持一致）。 */
export const BACKEND_EVENT_TYPES = [
  // Market data（lossy）。
  "L2BookUpdate",
  "TradeUpdate",
  "CandleUpdate",
  "FundingUpdate",
  "MidPriceUpdate",
  "ExternalReferenceUpdate",
  // Market-making analytics（lossy）。
  "MarketMakerFeatureSample",
  "MarketMakerQuoteDecision",
  "MarketMakerInventorySample",
  "MarketMakerActionCreditSample",
  "MarketMakerFillMarkout",
  // Execution（reliable）。
  "OrderSubmitted",
  "OrderAcknowledged",
  "OrderFilled",
  "OrderPartialFill",
  "OrderCancelled",
  "OrderRejected",
  "OrderExpired",
  // Account（reliable）。
  "PositionChanged",
  "BalanceChanged",
  "AccountStateUpdate",
  // Strategy（reliable）。
  "SignalGenerated",
  // System（reliable）。
  "RiskCheckPassed",
  "RiskCheckFailed",
  "KillSwitchTriggered",
  "ReconciliationComplete",
  "ActionCreditsLow",
  "WsConnected",
  "WsDisconnected",
] as const

/** 前端专用事件：后端不下发此名，收到后触发全量重验证（use-sse 单独处理）。 */
export const STREAM_RESYNC_EVENT = "StreamResyncRequired"

/** SSE 事件类型 → 需要重验证的 SWR key 前缀。 */
export const EVENT_KEYS: Record<string, string[]> = {
  // --- 后端真实事件（PascalCase，与 ALL_EVENT_TYPES 一一对应）---
  // Market data：重验证行情 REST 缓存（WS 连接时轮询关闭，帧直接喂缓存）。
  L2BookUpdate: ["/api/v1/market/"],
  TradeUpdate: ["/api/v1/market/"],
  CandleUpdate: ["/api/v1/market/"],
  FundingUpdate: ["/api/v1/market/"],
  MidPriceUpdate: ["/api/v1/market/"],
  ExternalReferenceUpdate: ["/api/v1/market/", "/api/v1/market-making/"],
  // Market-making analytics：重验证 MM 工作台 REST 快照。
  MarketMakerFeatureSample: ["/api/v1/market-making/"],
  MarketMakerQuoteDecision: ["/api/v1/market-making/"],
  MarketMakerInventorySample: ["/api/v1/market-making/"],
  MarketMakerActionCreditSample: ["/api/v1/market-making/"],
  MarketMakerFillMarkout: ["/api/v1/market-making/"],
  // Execution。
  OrderSubmitted: ["/api/v1/orders"],
  OrderAcknowledged: ["/api/v1/orders"],
  OrderFilled: ["/api/v1/orders", "/api/v1/positions", "/api/v1/account"],
  OrderPartialFill: ["/api/v1/orders", "/api/v1/positions", "/api/v1/account"],
  OrderCancelled: ["/api/v1/orders"],
  OrderRejected: ["/api/v1/orders"],
  OrderExpired: ["/api/v1/orders"],
  // Account。
  PositionChanged: ["/api/v1/positions", "/api/v1/account"],
  BalanceChanged: ["/api/v1/account"],
  AccountStateUpdate: ["/api/v1/account", "/api/v1/risk/status"],
  // Strategy。
  SignalGenerated: ["/api/v1/strategies"],
  // System。
  RiskCheckPassed: ["/api/v1/risk/status"],
  RiskCheckFailed: ["/api/v1/risk/status"],
  KillSwitchTriggered: ["/api/v1/system/status", "/api/v1/risk/status"],
  ReconciliationComplete: [
    "/api/v1/system/status",
    "/api/v1/account",
    "/api/v1/positions",
    "/api/v1/orders",
    "/api/v1/risk/status",
  ],
  ActionCreditsLow: ["/api/v1/risk/status"],
  WsConnected: [],
  WsDisconnected: [],

  // --- 历史遗留别名（防御性保留；Rust 后端只发 PascalCase）---
  "order.submitted": ["/api/v1/orders"],
  "order.acknowledged": ["/api/v1/orders"],
  "order.filled": ["/api/v1/orders", "/api/v1/positions", "/api/v1/account"],
  "order.cancelled": ["/api/v1/orders"],
  "order.rejected": ["/api/v1/orders"],
  "exchange.fill.ingested": ["/api/v1/orders", "/api/v1/positions", "/api/v1/account"],
  "exchange.order.updated": ["/api/v1/orders"],
  "system.safety.transitioned": ["/api/v1/system/status", "/api/v1/risk/status"],
  "reconciliation.completed": [
    "/api/v1/system/status",
    "/api/v1/account",
    "/api/v1/positions",
    "/api/v1/orders",
    "/api/v1/risk/status",
  ],
  order_submitted: ["/api/v1/orders"],
  order_acknowledged: ["/api/v1/orders"],
  order_filled: ["/api/v1/orders", "/api/v1/positions", "/api/v1/account"],
  order_partial_fill: ["/api/v1/orders", "/api/v1/positions", "/api/v1/account"],
  order_expired: ["/api/v1/orders"],
  order_cancelled: ["/api/v1/orders"],
  order_rejected: ["/api/v1/orders"],
  position_changed: ["/api/v1/positions", "/api/v1/account"],
  balance_changed: ["/api/v1/account"],
  account_state_update: ["/api/v1/account", "/api/v1/risk/status"],
  kill_switch_triggered: ["/api/v1/system/status", "/api/v1/risk/status"],
  signal_generated: ["/api/v1/strategies"],
  risk_check_passed: ["/api/v1/risk/status"],
  risk_check_failed: ["/api/v1/risk/status"],
  action_credits_low: ["/api/v1/risk/status"],
  reconciliation_complete: [
    "/api/v1/system/status",
    "/api/v1/account",
    "/api/v1/positions",
    "/api/v1/orders",
    "/api/v1/risk/status",
  ],
  ws_connected: [],
  ws_disconnected: [],
}

/** 事件名 → 需要重验证的 key 前缀列表（无映射返回空数组）。 */
export function revalidationTargets(eventType: string): string[] {
  return EVENT_KEYS[eventType] ?? []
}
