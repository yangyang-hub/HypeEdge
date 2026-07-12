import { cleanup, render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { afterEach, describe, expect, it, vi } from "vitest"
import { ConfirmPhraseDialog } from "@/components/ui/confirm-phrase-dialog"
import { EnvBadge } from "@/components/ui/env-badge"
import { PnLText } from "@/components/ui/data-display"
import { Button } from "@/components/ui/button"

afterEach(cleanup)

describe("EnvBadge", () => {
  it("renders environment label with mainnet emphasis", () => {
    render(<EnvBadge environment="mainnet" />)
    expect(screen.getByText("MAINNET")).toBeInTheDocument()
  })

  it("falls back for unknown environment", () => {
    render(<EnvBadge environment={null} />)
    expect(screen.getByText("UNKNOWN")).toBeInTheDocument()
  })
})

describe("PnLText", () => {
  it("colors positive and negative values", () => {
    const { rerender } = render(<PnLText value="12.5" />)
    expect(screen.getByText("+$12.50")).toHaveClass("text-profit")

    rerender(<PnLText value="-3.2" />)
    expect(screen.getByText("-$3.20")).toHaveClass("text-loss")

    rerender(<PnLText value="0" />)
    expect(screen.getByText("$0.00")).toHaveClass("text-text-tertiary")
  })
})

describe("Button disabled title", () => {
  it("exposes disabled reason via title", () => {
    render(
      <Button type="button" disabled title="生命周期切换中">
        启动
      </Button>,
    )
    expect(screen.getByRole("button", { name: "启动" })).toHaveAttribute("title", "生命周期切换中")
  })
})

describe("ConfirmPhraseDialog", () => {
  it("keeps confirm disabled until phrase matches", async () => {
    const user = userEvent.setup()
    const onConfirm = vi.fn()

    render(
      <ConfirmPhraseDialog
        open
        onOpenChange={() => undefined}
        title="触发 Kill Switch"
        description="输入 CONFIRM"
        phrase="CONFIRM"
        onConfirm={onConfirm}
      />,
    )

    const confirm = screen.getByRole("button", { name: "确认" })
    expect(confirm).toBeDisabled()

    await user.type(screen.getByLabelText(/请输入/), "CONFIRM")
    expect(confirm).toBeEnabled()

    await user.click(confirm)
    expect(onConfirm).toHaveBeenCalledTimes(1)
  })
})
