"use client"

import { useEffect, useState } from "react"
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

export interface ConfirmPhraseDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  title: string
  description: string
  phrase: string
  confirmLabel?: string
  cancelLabel?: string
  loading?: boolean
  onConfirm: () => void | Promise<void>
}

export function ConfirmPhraseDialog({
  open,
  onOpenChange,
  title,
  description,
  phrase,
  confirmLabel = "确认",
  cancelLabel = "取消",
  loading = false,
  onConfirm,
}: ConfirmPhraseDialogProps) {
  const [value, setValue] = useState("")

  useEffect(() => {
    if (!open) setValue("")
  }, [open])

  const canConfirm = value === phrase && !loading

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent showClose={!loading}>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>
        <div className="space-y-2">
          <label className="text-xs text-text-tertiary" htmlFor="confirm-phrase">
            请输入 <span className="font-mono text-text-primary">{phrase}</span> 以确认
          </label>
          <Input
            id="confirm-phrase"
            value={value}
            onChange={(event) => setValue(event.target.value)}
            autoComplete="off"
            spellCheck={false}
            disabled={loading}
          />
        </div>
        <DialogFooter>
          <Button type="button" variant="ghost" disabled={loading} onClick={() => onOpenChange(false)}>
            {cancelLabel}
          </Button>
          <Button
            type="button"
            variant="danger"
            loading={loading}
            disabled={!canConfirm}
            title={!canConfirm && !loading ? `需输入 ${phrase}` : undefined}
            onClick={() => void onConfirm()}
          >
            {confirmLabel}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
