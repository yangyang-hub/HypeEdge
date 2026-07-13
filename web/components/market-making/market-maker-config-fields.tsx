"use client"

import { Input } from "@/components/ui/input"
import {
  MM_CREATE_CORE_DECIMAL_KEYS,
  MM_CREATE_CORE_INTEGER_KEYS,
  MM_DECIMAL_FIELDS,
  MM_INTEGER_FIELDS,
  type MarketMakerConfigFieldKey,
} from "@/lib/market-maker-config"
import type { DecimalString, MarketMakerConfig } from "@/lib/types"
import { cn } from "@/lib/utils"

export interface MarketMakerConfigFieldsProps {
  value: MarketMakerConfig
  onChange: (next: MarketMakerConfig) => void
  /** When true, only core create fields are shown unless `showAdvanced` is also true. */
  mode?: "full" | "create"
  showAdvanced?: boolean
  className?: string
}

export function MarketMakerConfigFields({
  value,
  onChange,
  mode = "full",
  showAdvanced = false,
  className,
}: MarketMakerConfigFieldsProps) {
  function setDecimal(key: MarketMakerConfigFieldKey, raw: string) {
    onChange({ ...value, [key]: raw as DecimalString })
  }

  function setInteger(key: MarketMakerConfigFieldKey, raw: string) {
    onChange({ ...value, [key]: Number.parseInt(raw, 10) || 0 })
  }

  const showAll = mode === "full" || showAdvanced
  const decimalFields = showAll
    ? MM_DECIMAL_FIELDS
    : MM_DECIMAL_FIELDS.filter(([key]) => MM_CREATE_CORE_DECIMAL_KEYS.includes(key))
  const integerFields = showAll
    ? MM_INTEGER_FIELDS
    : MM_INTEGER_FIELDS.filter(([key]) => MM_CREATE_CORE_INTEGER_KEYS.includes(key))

  return (
    <div className={cn("grid gap-3 md:grid-cols-2 xl:grid-cols-3", className)}>
      {decimalFields.map(([key, label]) => (
        <label key={key} className="text-xs text-text-secondary" htmlFor={`mm-cfg-${key}`}>
          {label}
          <Input
            id={`mm-cfg-${key}`}
            value={String(value[key])}
            onChange={(event) => setDecimal(key, event.target.value)}
            inputMode="decimal"
            className="mt-1 font-mono"
          />
        </label>
      ))}
      {integerFields.map(([key, label]) => (
        <label key={key} className="text-xs text-text-secondary" htmlFor={`mm-cfg-${key}`}>
          {label}
          <Input
            id={`mm-cfg-${key}`}
            value={String(value[key])}
            onChange={(event) => setInteger(key, event.target.value)}
            inputMode="numeric"
            className="mt-1 font-mono"
          />
        </label>
      ))}
    </div>
  )
}
