/**
 * Format a number with locale-specific thousands separators.
 */
export function formatNumber(n: number): string {
  return n.toLocaleString()
}

/**
 * Format milliseconds into a human-readable duration string.
 * Examples: "4.2s", "1m 23s", "2h 5m"
 */
export function formatDuration(ms: number): string {
  if (ms < 0) return '0s'

  const totalSeconds = Math.floor(ms / 1000)
  if (totalSeconds < 60) {
    const tenths = Math.floor((ms % 1000) / 100)
    return tenths > 0 ? `${totalSeconds}.${tenths}s` : `${totalSeconds}s`
  }

  const minutes = Math.floor(totalSeconds / 60)
  const seconds = totalSeconds % 60

  if (minutes < 60) {
    return seconds > 0 ? `${minutes}m ${seconds}s` : `${minutes}m`
  }

  const hours = Math.floor(minutes / 60)
  const remainingMinutes = minutes % 60
  return remainingMinutes > 0 ? `${hours}h ${remainingMinutes}m` : `${hours}h`
}
