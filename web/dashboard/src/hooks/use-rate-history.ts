import { useRef, useCallback } from 'react'
import type { ClusterStatusWithRates } from './use-all-cluster-status'

export interface RateDataPoint {
  time: number // unix ms
  label: string // "HH:MM:SS"
  recv: number
  sent: number
  deadLettered: number
}

// 5 minutes at 2-second intervals = 150 points.
const MAX_POINTS = 150
const INTERVAL_MS = 2000

function makeLabel(date: Date): string {
  return date.toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })
}

/** Build an array of zero-valued data points covering the last 5 minutes. */
function initHistory(): RateDataPoint[] {
  const now = Date.now()
  const points: RateDataPoint[] = []
  for (let i = MAX_POINTS - 1; i >= 0; i--) {
    const t = now - i * INTERVAL_MS
    points.push({
      time: t,
      label: makeLabel(new Date(t)),
      recv: 0,
      sent: 0,
      deadLettered: 0,
    })
  }
  return points
}

/**
 * Maintains a sliding window of throughput rate history.
 * Call `push(status)` on each new poll result to append a data point.
 * Returns the accumulated `history` array (most recent last).
 * The window is always 5 minutes wide, pre-filled with zeros.
 */
export function useRateHistory() {
  const historyRef = useRef<RateDataPoint[]>(initHistory())

  const push = useCallback((status: ClusterStatusWithRates) => {
    const r = status.rates
    const now = new Date()

    historyRef.current = [
      ...historyRef.current.slice(-(MAX_POINTS - 1)),
      {
        time: now.getTime(),
        label: makeLabel(now),
        recv: r.messages_received ?? 0,
        sent: r.messages_sent ?? 0,
        deadLettered: r.messages_dead_lettered ?? 0,
      },
    ]
  }, [])

  return { history: historyRef.current, push }
}
