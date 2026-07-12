# 前端编码实现规范（Next.js + React + shadcn/ui）

> 前端为交易系统监控仪表盘，负责展示实时行情、持仓、PnL、风控状态、告警等。
> 后端通过 REST API 提供数据，前端通过轮询或 SSE 获取实时更新。

## 技术栈

- **框架**：Next.js 15+（App Router）
- **UI 库**：shadcn/ui + Radix UI 原语
- **样式**：Tailwind CSS v4
- **状态管理**：React hooks + SWR（数据获取）/ zustand（全局 UI 状态）
- **图表**：Recharts 或 lightweight-charts（K线图）
- **类型安全**：TypeScript strict 模式
- **Lint**：ESLint + Prettier
- **测试**：Vitest + React Testing Library

## 前端项目结构（规划）

```
web/
├── app/                    # Next.js App Router 页面
│   ├── layout.tsx          # 根布局（导航 + 侧边栏）
│   ├── page.tsx            # 首页（账户总览）
│   ├── market/             # 行情页面
│   ├── positions/          # 持仓页面
│   ├── orders/             # 订单页面
│   ├── strategy/           # 策略管理
│   ├── risk/               # 风控面板
│   └── settings/           # 系统设置
├── components/
│   ├── ui/                 # shadcn/ui 组件（button, card, table, dialog...）
│   ├── charts/             # 图表组件（K线、PnL曲线、持仓分布）
│   ├── market/             # 行情相关组件（盘口、成交列表、资金费率）
│   ├── trading/            # 交易相关组件（订单表、持仓卡、成交历史）
│   └── layout/             # 布局组件（导航栏、侧边栏、状态栏）
├── hooks/                  # 自定义 hooks（useMarketData, usePositions, useWebSocket）
├── lib/
│   ├── api.ts              # API 客户端（fetch wrapper）
│   ├── types.ts            # TypeScript 类型定义（与后端 models 对齐）
│   ├── utils.ts            # 工具函数（格式化价格、时间、百分比）
│   └── constants.ts        # 常量
├── styles/
│   └── globals.css         # Tailwind 全局样式 + CSS 变量（主题色）
├── public/                 # 静态资源
├── next.config.ts
├── tailwind.config.ts
├── tsconfig.json
└── package.json
```

## 组件设计规范

```tsx
// ✅ 正确：组合模式 + props 类型 + 明确的职责
interface PositionCardProps {
  position: Position
  onClose: (symbol: string) => void
}

export function PositionCard({ position, onClose }: PositionCardProps) {
  const pnlColor = position.unrealizedPnl >= 0 ? "text-green-500" : "text-red-500"

  return (
    <Card>
      <CardHeader>
        <CardTitle>{position.symbol}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className={pnlColor}>
          {formatUsd(position.unrealizedPnl)}
        </p>
      </CardContent>
      <CardFooter>
        <Button variant="destructive" onClick={() => onClose(position.symbol)}>
          平仓
        </Button>
      </CardFooter>
    </Card>
  )
}

// ❌ 错误：巨型组件 + 无类型 + 内联样式
export default function PositionCard(props: any) {
  return <div style={{ color: props.pnl > 0 ? "green" : "red" }}>...</div>
}
```

- 每个组件文件只导出一个主组件。
- Props 用 `interface` 定义并导出，不用 `type`。
- 组件用函数声明 + 箭头函数，不用 `React.memo` 除非有性能问题。
- 使用 shadcn/ui 的 `Card`、`Table`、`Dialog` 等组件，不自己造轮子。
- 条件样式用 `clsx()` 或 `cn()`（shadcn 的 `lib/utils.ts`），不用三元内联。

## 类型系统

```typescript
// lib/types.ts — 与后端 core/models.py 对齐

// 使用 branded type 保持语义
type Symbol = string & { readonly __brand: "Symbol" }
type Price = number & { readonly __brand: "Price" }

interface Position {
  symbol: Symbol
  size: number        // 正=多, 负=空
  entryPrice: Price | null
  markPrice: Price | null
  unrealizedPnl: number
  leverage: number
  liquidationPrice: Price | null
}

interface Order {
  cloid: string
  symbol: Symbol
  side: "buy" | "sell"
  size: number
  price: Price | null
  status: OrderStatus
  filledSize: number
  strategyId: string | null
  createdAt: string  // ISO 8601
}

type OrderStatus =
  | "pending"
  | "submitted"
  | "acknowledged"
  | "partial_fill"
  | "filled"
  | "cancelled"
  | "rejected"
  | "expired"
```

- 类型定义集中在 `lib/types.ts`，与后端 `core/models.py` 和 `core/enums.py` 一一对应。
- API 响应类型 = 后端模型类型的 JSON 序列化版本。
- 枚举值字符串与后端完全一致（如 `"partial_fill"` 不是 `"partialFill"`）。

## 数据获取

```tsx
// hooks/usePositions.ts
import useSWR from "swr"
import { fetcher } from "@/lib/api"
import type { Position } from "@/lib/types"

export function usePositions(subAccount?: string) {
  const params = subAccount ? `?sub_account=${subAccount}` : ""
  const { data, error, isLoading } = useSWR<Position[]>(
    `/api/positions${params}`,
    fetcher,
    { refreshInterval: 5000 }  // 5s 轮询
  )

  return { positions: data ?? [], error, isLoading }
}
```

- 使用 SWR 管理服务端状态，配置 `refreshInterval` 做轮询。
- 所有 API 调用经过 `lib/api.ts` 的统一 fetcher（错误处理 + 认证）。
- 组件内只调 hooks，不直接 fetch。
- 实时性要求高的数据（盘口、成交）用 WebSocket hook。

## 样式规范

```css
/* globals.css — 主题色用 CSS 变量 */
@theme {
  --color-profit: #22c55e;
  --color-loss: #ef4444;
  --color-warning: #f59e0b;
  --color-critical: #dc2626;
}
```

```tsx
// ✅ 正确：Tailwind 工具类 + 语义化自定义色
<p className="text-profit font-mono">{formatPnl(pnl)}</p>

// ❌ 错误：硬编码颜色值
<p className="text-[#22c55e]">{formatPnl(pnl)}</p>
```

- 使用 Tailwind 工具类，不写自定义 CSS（除非动画等 Tailwind 不覆盖的场景）。
- 主题色通过 CSS 变量定义，不硬编码。
- 价格/PnL 用等宽字体 `font-mono`。
- 盈利用 `text-profit`（绿），亏损用 `text-loss`（红）。
- 响应式：仪表盘优先桌面端，移动端降级为列表视图。

## 性能要求

- 页面首次加载 < 2s（LCP）。
- 行情数据更新延迟 < 1s（从后端推送到 UI 渲染）。
- 大列表（成交历史、订单表）使用虚拟滚动。
- 图表数据点超过 1000 时做降采样。

## 前后端 API 契约

后端需要为前端提供以下 API（Phase 2 实现）：

| 端点 | 方法 | 说明 |
|---|---|---|
| `/api/account` | GET | 账户状态（余额、权益、回撤） |
| `/api/positions` | GET | 当前持仓列表 |
| `/api/orders` | GET | 订单列表（支持 status 过滤） |
| `/api/orders` | POST | 提交订单（经过风控） |
| `/api/orders/:cloid` | DELETE | 撤单 |
| `/api/strategies` | GET | 策略列表及状态 |
| `/api/strategies/:id/start` | POST | 启动策略 |
| `/api/strategies/:id/stop` | POST | 停止策略 |
| `/api/market/:symbol/book` | GET | 最新盘口 |
| `/api/market/:symbol/candles` | GET | K线数据 |
| `/api/risk/status` | GET | 风控状态（限额、动作额度、kill switch） |
| `/api/kill-switch` | POST | 触发/重置 kill switch |

- API 响应格式统一为 `{ "ok": true, "data": ... }` 或 `{ "ok": false, "error": "..." }`。
- 类型定义前后端必须同步更新。

## 前端测试规范

```tsx
// 组件测试
import { render, screen } from "@testing-library/react"
import { PositionCard } from "./position-card"

describe("PositionCard", () => {
  it("displays profit in green", () => {
    render(<PositionCard position={mockPosition({ unrealizedPnl: 100 })} onClose={vi.fn()} />)
    expect(screen.getByText("$100.00")).toHaveClass("text-profit")
  })

  it("calls onClose when button clicked", async () => {
    const onClose = vi.fn()
    render(<PositionCard position={mockPosition} onClose={onClose} />)
    await userEvent.click(screen.getByRole("button", { name: /平仓/ }))
    expect(onClose).toHaveBeenCalledWith("BTC")
  })
})
```

- 组件测试：渲染 + 交互 + 样式断言。
- Hook 测试：`renderHook` + 数据验证。
- API 测试：MSW mock 后端响应。
- 测试文件放在组件同级：`position-card.test.tsx`。
