"use client"

import { Input } from "@/components/ui/input"
import {
  TF_DECIMAL_FIELDS,
  TF_INTEGER_FIELDS,
  type TrendFollowConfigFieldKey,
  type TrendFollowFieldMeta,
} from "@/lib/trend-follow-config"
import type { DecimalString, TrendFollowConfig } from "@/lib/types"
import { cn } from "@/lib/utils"

export interface TrendFollowConfigFieldsProps {
  value: TrendFollowConfig
  onChange: (next: TrendFollowConfig) => void
  className?: string
}

function ConfigField({
  field,
  value,
  onChange,
  type = "text",
}: {
  field: TrendFollowFieldMeta
  value: string | number
  onChange: (raw: string) => void
  type?: "text" | "number"
}) {
  return (
    <label className="block text-xs text-text-secondary" htmlFor={`tf-${field.key}`}>
      <span className="font-medium text-text-primary">{field.label}</span>
      <p className="mt-0.5 text-2xs leading-snug text-text-tertiary">{field.description}</p>
      <Input
        id={`tf-${field.key}`}
        type={type}
        className="mt-1.5 font-mono"
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  )
}

export function TrendFollowConfigFields({ value, onChange, className }: TrendFollowConfigFieldsProps) {
  function setDecimal(key: TrendFollowConfigFieldKey, raw: string) {
    onChange({ ...value, [key]: raw as DecimalString })
  }

  function setInteger(key: TrendFollowConfigFieldKey, raw: string) {
    onChange({ ...value, [key]: Number.parseInt(raw, 10) || 0 })
  }

  return (
    <div className={cn("grid gap-4 sm:grid-cols-2", className)}>
      {TF_INTEGER_FIELDS.map((field) => (
        <ConfigField
          key={field.key}
          field={field}
          type="number"
          value={value[field.key]}
          onChange={(raw) => setInteger(field.key, raw)}
        />
      ))}
      {TF_DECIMAL_FIELDS.map((field) => (
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
