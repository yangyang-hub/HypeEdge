# 后端编码实现规范（Python）

## 通用规则

- **每个模块顶部**必须加 `from __future__ import annotations`，启用 PEP 604 延迟注解求值。

## 架构约束

- **模块化单体**：所有模块在同一进程内，通过 EventBus 通信，不引入进程间调用。
- **依赖方向**：`strategy → risk → execution → exchange`，单向依赖，禁止反向调用。
- **依赖注入**：模块通过构造函数接收依赖（EventBus、ExecutionClient 等），不在模块内直接 import 其他业务模块实例。
- **骨架先于实现**：新增功能先在 `execution/`、`risk/`、`strategy/` 等目录中定义 Protocol/ABC，再写实现类。

## 类型系统

```python
# ✅ 正确：使用语义类型
from hypeedge.core.types import Symbol, Price, Size, Cloid

def calculate_pnl(entry: Price, exit: Price, size: Size) -> Usd: ...

# ❌ 错误：使用裸类型
def calculate_pnl(entry: float, exit: float, size: float) -> float: ...
```

- 文件路径语义类型：`core/types.py`
- 领域模型：`core/models.py`（`@dataclass` 或 pydantic BaseModel）
- 枚举：`core/enums.py`（继承 `str, Enum`）
- 异常层级：`core/exceptions.py`（继承 `HypeEdgeError`）

## 异步模式

```python
# ✅ 正确：asyncio 原生
async def fetch_data(symbol: Symbol) -> list[Candle]:
    async with httpx.AsyncClient() as client:
        response = await client.post("/info", json=body)
        return [parse_candle(d) for d in response.json()]

# ❌ 错误：在 async 函数中调用阻塞 I/O
def fetch_data(symbol: Symbol) -> list[Candle]:  # 缺少 async
    response = requests.post(url, json=body)  # 阻塞整个事件循环
```

- 所有 I/O（HTTP、数据库、WebSocket）必须 async。
- CPU 密集计算或同步库调用用 `asyncio.to_thread()` 或 `loop.run_in_executor()`。
- 后台常驻任务用 `asyncio.create_task()` + 有意义的 `name` 参数。
- 取消通过 `task.cancel()` + 在任务内捕获 `CancelledError` 做清理。

## EventBus 使用

```python
# 发布
from hypeedge.core.events import EVENT_L2_BOOK_UPDATE, Event
self._event_bus.publish_sync(
    Event(event_type=EVENT_L2_BOOK_UPDATE, payload=snapshot, correlation_id=str(symbol))
)

# 订阅
queue = self._event_bus.subscribe(EVENT_L2_BOOK_UPDATE)
while self._running:
    event = await queue.get()
    await self._handle(event)

# 全量订阅（审计/监控）
audit_queue = self._event_bus.subscribe_all()
```

- 使用 `core/events.py` 中的常量字符串，不硬编码事件名。
- 新事件类型必须加入 `ALL_EVENT_TYPES` 集合。
- payload 类型必须与事件类型文档一致（如 `EVENT_L2_BOOK_UPDATE` 的 payload 是 `L2BookSnapshot`）。
- 高频行情事件（l2Book, trades）使用 `publish_sync`（非阻塞），低频控制事件可用 `await publish`。

## 风控实现规范

- 风控检查必须在 `risk_check_timeout_ms`（默认 500ms）内返回。
- 超时 = 拒绝（fail-safe），不放过任何订单。
- 风控数据源不可用时，策略降级为**只撤不下**模式。
- 风控模块异常 = 触发全局 kill switch。

## 执行引擎规范

- 所有订单必须带 `cloid`（通过 `execution/cloid.py` 生成）。
- 禁止盲目重发：下单超时后先按 cloid 查询真实状态。
- Nonce 必须串行：所有签名经 `NonceManager` 单队列。
- 订单状态机转换必须合法（参考 `core/enums.py` 的 `ORDER_TRANSITIONS`）。

## 存储层规范

- ClickHouse：只用于追加型时序数据（行情、成交、K线、funding），不做 UPDATE/DELETE。
- Postgres：用于事务性数据（订单、持仓、PnL），所有写操作在事务内。
- ORM 模型定义在 `storage/postgres.py`，继承 `Base`。
- ClickHouse DDL 定义在 `storage/clickhouse.py` 的 `DDL_STATEMENTS` 列表。

## 配置规范

```python
# ✅ 正确：在 Settings 类中定义，带验证
class RiskSettings(BaseSettings):
    max_leverage: int = Field(default=5, ge=1, le=50)

# ❌ 错误：硬编码或裸变量
MAX_LEVERAGE = 5
```

- 所有配置项在 `config/settings.py` 中定义，带 `Field` 约束。
- 三环境 YAML 文件同步更新。
- 敏感信息（密钥、私钥）只通过环境变量传入。

## 日志规范

```python
import structlog
logger = structlog.get_logger(__name__)

# ✅ 正确：结构化日志 + 业务上下文
logger.info("order_submitted", cloid=str(order.cloid), symbol=str(order.symbol), side=order.side.value)
logger.warning("action_credits_low", remaining=credits, watermark=self._watermark)
logger.error("ch_flush_error", table=table, error=str(e), rows=len(rows))

# ❌ 错误：f-string 日志
logger.info(f"Order {order.cloid} submitted for {order.symbol}")
```

- 使用 structlog 的键值对格式。
- 每条日志包含足够的上下文（cloid、symbol、strategy_id）用于追踪。
- 生产环境用 `JSONRenderer`，开发环境用 `ConsoleRenderer`。
- 关键事件（kill switch、大额亏损、对账不一致）同时写入审计日志。

## 错误处理

```python
# ✅ 正确：使用自定义异常 + 明确的错误信息
from hypeedge.core.exceptions import OrderRejectedError

raise OrderRejectedError(
    f"Insufficient margin: need {required}, have {available}",
    cloid=str(order.cloid),
    reason="insufficient_margin",
)

# ❌ 错误：裸 Exception + 模糊信息
raise Exception("Order failed")
```

- 使用 `core/exceptions.py` 中的异常层级。
- 异常消息包含：发生了什么、涉及的实体（cloid/symbol）、具体的数值。
- 可恢复错误：重试 + 退避。
- 不可恢复错误：告警 + 降级或停机。

## 测试规范

```python
# 单元测试：测试纯逻辑，不依赖外部服务
class TestOrderStateMachine:
    def test_pending_to_submitted(self):
        sm = OrderStateMachine()
        order = make_order(OrderStatus.PENDING)
        sm.transition(order, OrderStatus.SUBMITTED)
        assert order.status == OrderStatus.SUBMITTED

# 异步测试：直接用 async def + pytest-asyncio auto
@pytest.mark.asyncio
async def test_event_bus_publish(event_bus: EventBus):
    queue = event_bus.subscribe("TestEvent")
    await event_bus.publish(Event(event_type="TestEvent", payload="data"))
    assert queue.get_nowait().payload == "data"

# 集成测试：标记需要外部服务
@pytest.mark.integration
async def test_ws_connection():
    ...
```

- 测试文件命名：`test_<模块名>.py`。
- 测试类命名：`Test<ClassName>`。
- 测试方法命名：`test_<具体场景>`。
- fixture 定义在 `tests/conftest.py`。
- Mock 外部依赖（交易所 API、数据库），不 mock 内部模块。
