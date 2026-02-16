/**
 * Format a number with locale-specific thousands separators.
 */
export function formatNumber(n: number): string {
  return n.toLocaleString()
}

/**
 * Format megabytes into a human-readable string.
 * Examples: "12.3 MB", "1.2 GB"
 */
export function formatMB(mb: number): string {
  if (mb >= 1024) return `${(mb / 1024).toFixed(1)} GB`
  if (mb >= 10) return `${Math.round(mb)} MB`
  return `${mb.toFixed(1)} MB`
}

/**
 * Format a percentage (0-100) with one decimal place.
 */
export function formatPct(pct: number): string {
  if (pct === 0) return '0%'
  if (pct >= 99.9) return '100%'
  return `${pct.toFixed(1)}%`
}

/**
 * Format microseconds into a human-readable string.
 */
export function formatMicroseconds(us: number): string {
  if (us === 0) return '0'
  if (us < 1000) return `${us}us`
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)}ms`
  return `${(us / 1_000_000).toFixed(2)}s`
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
