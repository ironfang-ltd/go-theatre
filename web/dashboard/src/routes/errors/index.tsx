import { Fragment, useState } from 'react'
import { createFileRoute, Link } from '@tanstack/react-router'
import { useClusterErrors } from '../../hooks/use-cluster-errors'
import { useClusterStatus } from '../../hooks/use-cluster-status'
import StatCard from '../../components/StatCard'
import { formatNumber } from '../../lib/format'
import type { ErrorEntry } from '../../lib/api'

export const Route = createFileRoute('/errors/')({
  component: ErrorsPage,
})

function ErrorsPage() {
  const { data: status } = useClusterStatus()
  const { data, isLoading, error } = useClusterErrors()

  if (isLoading) {
    return <p className="text-zinc-500">Loading...</p>
  }

  if (error) {
    return (
      <p className="text-red-400">
        Failed to load errors: {error.message}
      </p>
    )
  }

  const entries = data?.errors ?? []
  const total = data?.total ?? 0
  const errorCount = entries.filter((e) => e.level === 'error').length
  const warnCount = entries.filter((e) => e.level === 'warn').length

  const m = status?.metrics ?? {}

  return (
    <div className="space-y-6">
      <h1 className="text-lg font-semibold text-zinc-100">
        Errors{' '}
        <span className="text-sm font-normal text-zinc-500">
          ({total} total)
        </span>
      </h1>

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <StatCard label="Total Recorded" value={formatNumber(m.errors_recorded ?? 0)} />
        <StatCard label="Showing" value={formatNumber(entries.length)} />
        <StatCard label="Errors" value={formatNumber(errorCount)} />
        <StatCard label="Warnings" value={formatNumber(warnCount)} />
      </div>

      {entries.length === 0 ? (
        <p className="text-zinc-500 text-sm">No errors recorded.</p>
      ) : (
        <ErrorsTable entries={entries} />
      )}
    </div>
  )
}

function ErrorsTable({ entries }: { entries: ErrorEntry[] }) {
  const [expanded, setExpanded] = useState<Set<number>>(new Set())
  const hasHost = entries.some((e) => e.host_id)
  const colCount = 5 + (hasHost ? 1 : 0)

  const toggle = (i: number) =>
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(i)) next.delete(i)
      else next.add(i)
      return next
    })

  return (
    <div className="overflow-x-auto rounded-lg border border-zinc-800">
      <table className="w-full text-left text-sm">
        <thead className="border-b border-zinc-800 bg-zinc-900/50">
          <tr>
            <th className="px-4 py-3 font-medium text-zinc-400">Time</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Level</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Source</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Actor</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Message</th>
            {hasHost && (
              <th className="px-4 py-3 font-medium text-zinc-400">Host</th>
            )}
          </tr>
        </thead>
        <tbody className="divide-y divide-zinc-800">
          {entries.map((e, i) => {
            const isOpen = expanded.has(i)
            return (
              <Fragment key={i}>
                <tr
                  className="cursor-pointer bg-zinc-900 hover:bg-zinc-800/60"
                  onClick={() => toggle(i)}
                >
                  <td className="whitespace-nowrap px-4 py-3 text-zinc-300">
                    <span title={e.time}>{timeAgo(e.time)}</span>
                  </td>
                  <td className="whitespace-nowrap px-4 py-3">
                    <LevelBadge level={e.level} />
                  </td>
                  <td className="whitespace-nowrap px-4 py-3 text-zinc-300">
                    {e.source}
                  </td>
                  <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-100">
                    {e.actor ? (
                      <Link
                        to="/actors/$type/$id"
                        params={{
                          type: e.actor.split('/')[0],
                          id: e.actor.split('/').slice(1).join('/'),
                        }}
                        className="hover:text-white hover:underline"
                        onClick={(ev) => ev.stopPropagation()}
                      >
                        {e.actor}
                      </Link>
                    ) : (
                      <span className="text-zinc-500">-</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-zinc-300">{e.message}</td>
                  {hasHost && (
                    <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-400">
                      {e.host_id ?? '-'}
                    </td>
                  )}
                </tr>
                {isOpen && e.detail && (
                  <tr className="bg-zinc-950">
                    <td colSpan={colCount} className="px-4 py-3">
                      <pre className="whitespace-pre-wrap break-all font-mono text-xs text-zinc-400">
                        {e.detail}
                      </pre>
                    </td>
                  </tr>
                )}
              </Fragment>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

const levelStyles: Record<string, string> = {
  error: 'bg-red-500/15 text-red-400 border-red-500/25',
  warn: 'bg-amber-500/15 text-amber-400 border-amber-500/25',
}

function LevelBadge({ level }: { level: string }) {
  const style = levelStyles[level] ?? levelStyles['error']
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-medium ${style}`}
    >
      {level}
    </span>
  )
}

function timeAgo(isoDate: string): string {
  const ms = Date.now() - new Date(isoDate).getTime()
  if (ms < 0) return 'just now'
  const seconds = Math.floor(ms / 1000)
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}
