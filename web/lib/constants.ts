// Application constants

export const API_BASE = ""
export const SWR_REFRESH_INTERVAL = 5000
export const SWR_SLOW_INTERVAL = 30000
export const SWR_FAST_INTERVAL = 60000

export const ORDER_STATUS_LABELS: Record<string, string> = {
  pending: "待处理",
  submitted: "已提交",
  submit_unknown: "提交结果未知",
  acknowledged: "已确认",
  partial_fill: "部分成交",
  cancel_pending: "撤单处理中",
  cancel_unknown: "撤单结果未知",
  filled: "已成交",
  cancelled: "已撤单",
  rejected: "已拒绝",
  expired: "已过期",
}

export const ORDER_STATUS_COLORS: Record<string, string> = {
  pending: "text-text-tertiary",
  submitted: "text-info",
  submit_unknown: "text-warning",
  acknowledged: "text-info",
  partial_fill: "text-warning",
  cancel_pending: "text-warning",
  cancel_unknown: "text-warning",
  filled: "text-profit",
  cancelled: "text-text-tertiary",
  rejected: "text-loss",
  expired: "text-text-tertiary",
}
