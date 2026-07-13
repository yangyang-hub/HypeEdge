"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { MarketMakerConfigFields } from "@/components/market-making/market-maker-config-fields"
import { useAccount } from "@/hooks/use-account"
import { createStrategy } from "@/hooks/use-strategies"
import { useInstrumentMeta } from "@/hooks/use-system-status"
import { TRADE_SYMBOLS } from "@/lib/constants"
import { ApiError, createIdempotencyKey } from "@/lib/api"
import {
  cloneDefaultMmConfig,
  inventoryBandsFromEquity,
  suggestQuoteSize,
  validateMmConfig,
  validateStrategyIdentity,
} from "@/lib/market-maker-config"
import type { MarketMakerConfig, StrategyInstance } from "@/lib/types"

export interface CreateMarketMakerDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  existing: StrategyInstance[]
  onCreated?: (strategy: StrategyInstance) => void
}

type Step = 1 | 2

export function CreateMarketMakerDialog({
  open,
  onOpenChange,
  existing,
  onCreated,
}: CreateMarketMakerDialogProps) {
  const router = useRouter()
  const { account } = useAccount()
  const [step, setStep] = useState<Step>(1)
  const [strategyId, setStrategyId] = useState("")
  const [subAccount, setSubAccount] = useState("")
  const [symbol, setSymbol] = useState<string>(TRADE_SYMBOLS[0])
  const [metadataNote, setMetadataNote] = useState("")
  const [config, setConfig] = useState<MarketMakerConfig>(() => cloneDefaultMmConfig())
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const idempotencyKeyRef = useRef(createIdempotencyKey())
  const { meta, error: metaError } = useInstrumentMeta(open ? symbol : undefined)

  // Reset form when dialog opens.
  useEffect(() => {
    if (!open) return
    setStep(1)
    setStrategyId("")
    setSubAccount("")
    setSymbol(TRADE_SYMBOLS[0])
    setMetadataNote("")
    setShowAdvanced(false)
    setError(null)
    setSubmitting(false)
    idempotencyKeyRef.current = createIdempotencyKey()
    const next = cloneDefaultMmConfig()
    if (account?.equity) {
      Object.assign(next, inventoryBandsFromEquity(account.equity))
    }
    setConfig(next)
  }, [open, account?.equity])

  // Apply quote size from instrument meta when symbol changes.
  useEffect(() => {
    if (!open || !meta) return
    setConfig((previous) => ({ ...previous, quote_size: suggestQuoteSize(meta) }))
  }, [meta, open, symbol])

  const conflictHint = useMemo(() => {
    const id = strategyId.trim()
    const sub = subAccount.trim()
    const sym = symbol.trim().toUpperCase()
    if (id && existing.some((item) => item.strategy_id === id)) {
      return `strategy_id「${id}」已存在`
    }
    if (
      sub &&
      sym &&
      existing.some(
        (item) =>
          item.strategy_type === "market_maker" &&
          item.sub_account?.toLowerCase() === sub.toLowerCase() &&
          item.symbol.toUpperCase() === sym,
      )
    ) {
      return `子账户「${sub}」已占用 ${sym}`
    }
    return null
  }, [existing, strategyId, subAccount, symbol])

  function goNext() {
    const identityError = validateStrategyIdentity({
      strategy_id: strategyId.trim(),
      sub_account: subAccount.trim(),
      symbol: symbol.trim().toUpperCase(),
    })
    if (identityError) {
      setError(identityError)
      return
    }
    if (conflictHint) {
      setError(conflictHint)
      return
    }
    if (metaError) {
      setError(`无法加载 ${symbol} 合约元数据，请换品种或稍后重试`)
      return
    }
    setError(null)
    setStep(2)
  }

  async function handleSubmit() {
    const configError = validateMmConfig(config)
    if (configError) {
      setError(configError)
      return
    }
    setSubmitting(true)
    setError(null)
    try {
      const metadata: Record<string, string> = {}
      if (metadataNote.trim()) metadata.note = metadataNote.trim()
      const created = await createStrategy(
        {
          strategy_id: strategyId.trim(),
          strategy_type: "market_maker",
          sub_account: subAccount.trim(),
          symbol: symbol.trim().toUpperCase(),
          initial_config: config,
          metadata,
        },
        { idempotencyKey: idempotencyKeyRef.current },
      )
      onCreated?.(created)
      onOpenChange(false)
      router.push(`/strategy/${encodeURIComponent(created.strategy_id)}/market-making?created=1`)
    } catch (err) {
      const message =
        err instanceof ApiError
          ? err.code === "STRATEGY_CREATE_CONFLICT"
            ? "创建冲突：strategy_id 或 sub_account+symbol 已占用"
            : err.message
          : err instanceof Error
            ? err.message
            : "创建失败"
      setError(message)
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>新建做市策略</DialogTitle>
          <DialogDescription>
            {step === 1
              ? "第一步：定义实例身份。创建后为 stopped，需在工作台 Shadow → Running。"
              : "第二步：设置初始风控配置。高级参数可折叠，默认值已填好。"}
          </DialogDescription>
          <p className="text-2xs text-text-tertiary">步骤 {step} / 2</p>
        </DialogHeader>

        {step === 1 ? (
          <div className="grid gap-3 sm:grid-cols-2">
            <label className="text-xs text-text-secondary sm:col-span-2" htmlFor="create-strategy-id">
              strategy_id
              <Input
                id="create-strategy-id"
                className="mt-1 font-mono"
                placeholder="mm-btc-1"
                value={strategyId}
                onChange={(event) => setStrategyId(event.target.value)}
                autoComplete="off"
              />
            </label>
            <label className="text-xs text-text-secondary" htmlFor="create-symbol">
              symbol
              <select
                id="create-symbol"
                value={symbol}
                onChange={(event) => setSymbol(event.target.value)}
                className="mt-1 flex h-8 w-full rounded-md border border-border-default bg-bg-elevated px-3 text-sm text-text-primary"
              >
                {TRADE_SYMBOLS.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>
            <label className="text-xs text-text-secondary" htmlFor="create-sub-account">
              sub_account
              <Input
                id="create-sub-account"
                className="mt-1 font-mono"
                placeholder="mm-btc-1"
                value={subAccount}
                onChange={(event) => setSubAccount(event.target.value)}
                autoComplete="off"
              />
            </label>
            <label className="text-xs text-text-secondary sm:col-span-2" htmlFor="create-note">
              备注（可选，写入 metadata.note）
              <Input
                id="create-note"
                className="mt-1"
                placeholder="testnet lab"
                value={metadataNote}
                onChange={(event) => setMetadataNote(event.target.value)}
              />
            </label>
            {meta ? (
              <p className="sm:col-span-2 font-mono text-2xs text-text-tertiary">
                lot {meta.lot_size} · min {meta.min_order_size} · tick {meta.tick_size}
              </p>
            ) : null}
          </div>
        ) : (
          <div className="space-y-3">
            {account?.equity ? (
              <p className="rounded-md border border-border-default bg-bg-elevated px-3 py-2 text-xs text-text-secondary">
                已按权益 {account.equity} USDC 建议库存带（约 5% / 10% / 15%），可手动调整。
              </p>
            ) : null}
            <MarketMakerConfigFields
              mode="create"
              showAdvanced={showAdvanced}
              value={config}
              onChange={setConfig}
            />
            <Button type="button" variant="ghost" size="sm" onClick={() => setShowAdvanced((value) => !value)}>
              {showAdvanced ? "收起高级参数" : "展开高级参数"}
            </Button>
          </div>
        )}

        {error || conflictHint ? (
          <p role="alert" className="mt-3 rounded-md border border-critical/30 bg-critical/10 px-3 py-2 text-sm text-critical">
            {error ?? conflictHint}
          </p>
        ) : null}

        <DialogFooter>
          {step === 2 ? (
            <Button type="button" variant="ghost" onClick={() => setStep(1)} disabled={submitting}>
              上一步
            </Button>
          ) : (
            <Button type="button" variant="ghost" onClick={() => onOpenChange(false)} disabled={submitting}>
              取消
            </Button>
          )}
          {step === 1 ? (
            <Button type="button" variant="primary" onClick={goNext}>
              下一步
            </Button>
          ) : (
            <Button type="button" variant="primary" loading={submitting} onClick={() => void handleSubmit()}>
              创建策略
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
