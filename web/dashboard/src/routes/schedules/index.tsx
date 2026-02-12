import { createFileRoute, Link } from '@tanstack/react-router'
import { useClusterSchedules } from '../../hooks/use-cluster-schedules'
import { useClusterStatus } from '../../hooks/use-cluster-status'
import StatCard from '../../components/StatCard'
import { formatNumber } from '../../lib/format'
import type { ScheduleEntry } from '../../lib/api'

export const Route = createFileRoute('/schedules/')({
  component: SchedulesPage,
})

function SchedulesPage() {
  const { data: status } = useClusterStatus()
  const { data, isLoading, error } = useClusterSchedules()

  if (isLoading) {
    return <p className="text-zinc-500">Loading...</p>
  }

  if (error) {
    return (
      <p className="text-red-400">
        Failed to load schedules: {error.message}
      </p>
    )
  }

  const schedules = data?.schedules ?? []
  const oneShots = schedules.filter((s) => s.kind === 'one-shot')
  const crons = schedules.filter((s) => s.kind === 'cron')

  const m = status?.metrics ?? {}

  return (
    <div className="space-y-6">
      <h1 className="text-lg font-semibold text-zinc-100">
        Schedules{' '}
        <span className="text-sm font-normal text-zinc-500">
          ({schedules.length})
        </span>
      </h1>

      {/* Headline Stats */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <StatCard label="Pending" value={formatNumber(schedules.length)} />
        <StatCard label="One-Shot" value={formatNumber(oneShots.length)} />
        <StatCard label="Cron" value={formatNumber(crons.length)} />
        <StatCard
          label="Fired"
          value={formatNumber(m.schedules_fired ?? 0)}
          subtitle={
            (m.schedules_recovered ?? 0) > 0
              ? `${formatNumber(m.schedules_recovered ?? 0)} recovered`
              : undefined
          }
        />
      </div>

      {/* Schedules Table */}
      {schedules.length === 0 ? (
        <p className="text-zinc-500 text-sm">No pending schedules.</p>
      ) : (
        <SchedulesTable schedules={schedules} />
      )}
    </div>
  )
}

function SchedulesTable({ schedules }: { schedules: ScheduleEntry[] }) {
  const sorted = [...schedules].sort(
    (a, b) => new Date(a.next_fire).getTime() - new Date(b.next_fire).getTime(),
  )

  return (
    <div className="overflow-x-auto rounded-lg border border-zinc-800">
      <table className="w-full text-left text-sm">
        <thead className="border-b border-zinc-800 bg-zinc-900/50">
          <tr>
            <th className="px-4 py-3 font-medium text-zinc-400">ID</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Host</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Kind</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Actor</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Body</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Next Fire</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Cron</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-zinc-800">
          {sorted.map((s) => (
            <tr key={`${s.host_id}-${s.id}`} className="bg-zinc-900">
              <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-300">
                {s.id}
              </td>
              <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-300">
                {s.host_id ?? '—'}
              </td>
              <td className="whitespace-nowrap px-4 py-3">
                <KindBadge kind={s.kind} />
              </td>
              <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-100">
                <Link
                  to="/actors/$type/$id"
                  params={{ type: s.actor_type, id: s.actor_id }}
                  className="hover:text-white hover:underline"
                >
                  {s.actor_type}:{s.actor_id}
                </Link>
              </td>
              <td className="max-w-xs truncate px-4 py-3 text-zinc-300" title={s.body}>
                {s.body}
              </td>
              <td className="whitespace-nowrap px-4 py-3 text-zinc-300">
                <span title={s.next_fire}>
                  {new Date(s.next_fire).toLocaleTimeString()}
                </span>
                <span className="ml-2 text-zinc-500">
                  ({timeUntil(s.next_fire)})
                </span>
              </td>
              <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-400">
                {s.cron_expr || '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

const kindStyles: Record<string, string> = {
  'one-shot': 'bg-amber-500/15 text-amber-400 border-amber-500/25',
  cron: 'bg-sky-500/15 text-sky-400 border-sky-500/25',
}

function KindBadge({ kind }: { kind: string }) {
  const style = kindStyles[kind] ?? kindStyles['one-shot']
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-medium ${style}`}
    >
      {kind}
    </span>
  )
}

function timeUntil(isoDate: string): string {
  const ms = new Date(isoDate).getTime() - Date.now()
  if (ms <= 0) return 'overdue'
  const seconds = Math.floor(ms / 1000)
  if (seconds < 60) return `in ${seconds}s`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `in ${minutes}m`
  const hours = Math.floor(minutes / 60)
  return `in ${hours}h`
}
