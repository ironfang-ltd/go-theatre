import { createFileRoute, Link } from '@tanstack/react-router'
import { useActorDetail } from '../../../hooks/use-actor-detail'
import { useClusterErrors } from '../../../hooks/use-cluster-errors'
import ActorStatusBadge from '../../../components/ActorStatusBadge'
import StatCard from '../../../components/StatCard'
import DetailCard, { Row } from '../../../components/DetailCard'
import { formatNumber, formatDuration } from '../../../lib/format'

export const Route = createFileRoute('/actors/$type/$id')({
  component: ActorDetailPage,
})

function ActorDetailPage() {
  const { type, id } = Route.useParams()
  const { data, isLoading, error } = useActorDetail(type, id)
  const { data: errorsData } = useClusterErrors()

  // Filter errors for this actor.
  const actorKey = `${type}/${id}`
  const actorErrors = (errorsData?.errors ?? []).filter(
    (e) => e.actor === actorKey,
  )

  if (isLoading) {
    return <p className="text-zinc-500">Loading...</p>
  }

  if (error) {
    return (
      <p className="text-red-400">
        Failed to load actor: {error.message}
      </p>
    )
  }

  if (!data || !data.found) {
    return (
      <div className="space-y-3">
        <Link
          to="/actors"
          className="text-sm text-zinc-400 hover:text-zinc-100"
        >
          &larr; Back to actors
        </Link>
        <p className="text-zinc-500">
          Actor <code className="text-zinc-300">{type}:{id}</code> not found.
        </p>
      </div>
    )
  }

  const inboxPct =
    data.inbox_cap && data.inbox_cap > 0
      ? Math.round(((data.inbox_size ?? 0) / data.inbox_cap) * 100)
      : 0

  const avgRate =
    data.messages_total != null &&
    data.messages_total > 0 &&
    data.uptime_ms != null &&
    data.uptime_ms > 0
      ? ((data.messages_total / data.uptime_ms) * 1000).toFixed(1)
      : null

  return (
    <div className="space-y-6">
      {/* Breadcrumb */}
      <Link
        to="/actors"
        className="text-sm text-zinc-400 hover:text-zinc-100"
      >
        &larr; Back to actors
      </Link>

      {/* Header */}
      <div className="flex items-center gap-3">
        <ActorStatusBadge status={data.status ?? 'inactive'} />
        <h1 className="text-xl font-semibold text-zinc-100 font-mono">
          {data.type}:{data.id}
        </h1>
        {data.receiver_type && (
          <span className="rounded-md bg-zinc-800 px-2 py-0.5 text-xs text-zinc-400 font-mono">
            {data.receiver_type}
          </span>
        )}
        {data.host_id && (
          <span className="rounded-md bg-zinc-800 px-2 py-0.5 text-xs text-zinc-500 font-mono">
            {data.host_id}
          </span>
        )}
      </div>

      {/* Headline Stats */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <StatCard
          label="Messages"
          value={formatNumber(data.messages_total ?? 0)}
          subtitle={avgRate ? `${avgRate}/s avg` : undefined}
        />
        <StatCard
          label="Errors"
          value={formatNumber(data.errors_total ?? 0)}
        />
        <StatCard
          label="Uptime"
          value={data.uptime_ms != null ? formatDuration(data.uptime_ms) : '—'}
        />
        <StatCard
          label="Inbox"
          value={`${data.inbox_size ?? 0} / ${data.inbox_cap ?? 0}`}
          subtitle={`${inboxPct}% used`}
        />
      </div>

      {/* Detail Cards */}
      <div className="grid gap-4 md:grid-cols-2">
        {/* Lifecycle */}
        <DetailCard title="Lifecycle">
          {data.created_at && (
            <Row
              label="Created"
              value={`${new Date(data.created_at).toLocaleTimeString()} — ${new Date(data.created_at).toLocaleDateString()}`}
            />
          )}
          {data.uptime_ms != null && (
            <Row label="Uptime" value={formatDuration(data.uptime_ms)} />
          )}
          {data.last_message && (
            <Row
              label="Last Message"
              value={
                <>
                  {new Date(data.last_message).toLocaleTimeString()}
                  <span className="ml-2 text-zinc-500">
                    ({timeSince(data.last_message)})
                  </span>
                </>
              }
            />
          )}
          <Row label="Status" value={data.status ?? 'unknown'} />
        </DetailCard>

        {/* Throughput */}
        <DetailCard title="Throughput">
          <Row
            label="Messages Processed"
            value={formatNumber(data.messages_total ?? 0)}
            mono
          />
          <Row
            label="Errors"
            value={formatNumber(data.errors_total ?? 0)}
            mono
          />
          {avgRate && <Row label="Avg Rate" value={`${avgRate} msg/s`} mono />}
          {data.messages_total != null &&
            data.errors_total != null &&
            data.messages_total > 0 && (
              <Row
                label="Error Rate"
                value={`${((data.errors_total / data.messages_total) * 100).toFixed(2)}%`}
                mono
              />
            )}
        </DetailCard>

        {/* Inbox */}
        <DetailCard title="Inbox">
          <Row
            label="Queued"
            value={formatNumber(data.inbox_size ?? 0)}
            mono
          />
          <Row
            label="Capacity"
            value={formatNumber(data.inbox_cap ?? 0)}
            mono
          />
          <Row label="Backpressure" value={<InboxBar pct={inboxPct} />} />
        </DetailCard>

        {/* Identity */}
        <DetailCard title="Identity">
          <Row label="Type" value={data.type} mono />
          <Row label="ID" value={data.id} mono />
          {data.receiver_type && (
            <Row label="Receiver" value={data.receiver_type} mono />
          )}
          {data.host_id && (
            <Row label="Host" value={data.host_id} mono />
          )}
        </DetailCard>

        {/* Cluster Ownership */}
        {(data.owner_host || data.owner_addr || data.epoch) && (
          <DetailCard title="Cluster Ownership">
            {data.owner_host && (
              <Row label="Owner Host" value={data.owner_host} mono />
            )}
            {data.owner_addr && (
              <Row label="Address" value={data.owner_addr} mono />
            )}
            {data.epoch != null && data.epoch > 0 && (
              <Row label="Epoch" value={data.epoch} mono />
            )}
          </DetailCard>
        )}
      </div>

      {/* Recent Errors */}
      {actorErrors.length > 0 && (
        <div className="space-y-3">
          <h2 className="text-sm font-medium text-zinc-400">
            Recent Errors ({actorErrors.length})
          </h2>
          <div className="overflow-x-auto rounded-lg border border-zinc-800">
            <table className="w-full text-left text-sm">
              <thead className="border-b border-zinc-800 bg-zinc-900/50">
                <tr>
                  <th className="px-4 py-2 font-medium text-zinc-400">Time</th>
                  <th className="px-4 py-2 font-medium text-zinc-400">Level</th>
                  <th className="px-4 py-2 font-medium text-zinc-400">Message</th>
                  <th className="px-4 py-2 font-medium text-zinc-400">Detail</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-800">
                {actorErrors.map((e, i) => (
                  <tr key={i} className="bg-zinc-900">
                    <td className="whitespace-nowrap px-4 py-2 text-zinc-300">
                      <span title={e.time}>{timeSince(e.time)}</span>
                    </td>
                    <td className="whitespace-nowrap px-4 py-2">
                      <span
                        className={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium ${
                          e.level === 'error'
                            ? 'bg-red-500/15 text-red-400 border-red-500/25'
                            : 'bg-amber-500/15 text-amber-400 border-amber-500/25'
                        }`}
                      >
                        {e.level}
                      </span>
                    </td>
                    <td className="px-4 py-2 text-zinc-300">{e.message}</td>
                    <td className="px-4 py-2 text-zinc-400">
                      <pre className="whitespace-pre-wrap break-all font-mono text-xs">
                        {e.detail || '-'}
                      </pre>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

function InboxBar({ pct }: { pct: number }) {
  const color =
    pct >= 80
      ? 'bg-red-500'
      : pct >= 50
        ? 'bg-amber-500'
        : 'bg-emerald-500'
  return (
    <div className="flex items-center gap-2">
      <div className="h-2 w-20 rounded-full bg-zinc-800">
        <div
          className={`h-2 rounded-full ${color}`}
          style={{ width: `${Math.min(pct, 100)}%` }}
        />
      </div>
      <span className="text-sm font-mono text-zinc-100">{pct}%</span>
    </div>
  )
}

function timeSince(isoDate: string): string {
  const ms = Date.now() - new Date(isoDate).getTime()
  if (ms < 1000) return 'just now'
  const seconds = Math.floor(ms / 1000)
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  return `${hours}h ago`
}
