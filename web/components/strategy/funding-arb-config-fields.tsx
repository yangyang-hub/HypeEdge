"use client"

import { Input } from "@/components/ui/input"
import {
  FA_DECIMAL_FIELDS,
  FA_INTEGER_FIELDS,
  FA_STRING_FIELDS,
  type FundingArbConfigFieldKey,
  type FundingArbFieldMeta,
} from "@/lib/funding-arb-config"
import type { DecimalString, FundingArbConfig } from "@/lib/types"
import { cn } from "@/lib/utils"

export interface FundingArbConfigFieldsProps {
  value: FundingArbConfig
  onChange: (next: FundingArbConfig) => void
  className?: string
}

function ConfigField({
  field,
  value,
  onChange,
  type = "text",
}: {
  field: FundingArbFieldMeta
  value: string | number
  onChange: (raw: string) => void
  type?: "text" | "number"
}) {
  return (
    <label className="block text-xs text-text-secondary" htmlFor={`fa-${field.key}`}>
      <span className="font-medium text-text-primary">{field.label}</span>
      <p className="mt-0.5 text-2xs leading-snug text-text-tertiary">{field.description}</p>
      <Input
        id={`fa-${field.key}`}
        type={type}
        className="mt-1.5 font-mono"
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  )
}

export function FundingArbConfigFields({ value, onChange, className }: FundingArbConfigFieldsProps) {
  function setDecimal(key: FundingArbConfigFieldKey, raw: string) {
    onChange({ ...value, [key]: raw as DecimalString })
  }

  function setInteger(key: FundingArbConfigFieldKey, raw: string) {
    onChange({ ...value, [key]: Number.parseInt(raw, 10) || 0 })
  }

  function setString(key: FundingArbConfigFieldKey, raw: string) {
    onChange({ ...value, [key]: raw })
  }

  return (
    <div className={cn("grid gap-4 sm:grid-cols-2", className)}>
      {FA_STRING_FIELDS.map((field) => (
        <ConfigField
          key={field.key}
          field={field}
          value={value[field.key]}
          onChange={(raw) => setString(field.key, raw)}
        />
      ))}
      {FA_INTEGER_FIELDS.map((field) => (
        <ConfigField
          key={field.key}
          field={field}
          type="number"
          value={value[field.key]}
          onChange={(raw) => setInteger(field.key, raw)}
        />
      ))}
      {FA_DECIMAL_FIELDS.map((field) => (
        <ConfigField
          key={field.key}
          field={field}
          value={value[field.key]}
          onChange={(raw) => setDecimal(field.key, raw)}
        />
      ))}
    </div>
  )
}
