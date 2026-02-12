import { createFileRoute } from '@tanstack/react-router'
import { useClusterStatus } from '../hooks/use-cluster-status'
import StatCard from '../components/StatCard'
import DetailCard, { Row } from '../components/DetailCard'
import { formatNumber, formatDuration } from '../lib/format'

export const Route = createFileRoute('/')({
  component: Dashboard,
})

function Dashboard() {
  const { data: status, isLoading, error } = useClusterStatus()

  if (isLoading) {
    return <p className="text-zinc-500">Loading...</p>
  }

  if (error) {
    return (
      <p className="text-red-400">
        Failed to load cluster status: {error.message}
      </p>
    )
  }

  if (!status) return null

  const m = status.metrics
  const isCluster = status.state !== 'standalone'

  const cacheTotal = (m.placement_cache_hits ?? 0) + (m.placement_cache_misses ?? 0)
  const hitRate =
    cacheTotal > 0
      ? ((m.placement_cache_hits ?? 0) / cacheTotal * 100).toFixed(1)
      : null

  return (
    <div className="space-y-6">
      {/* Headline Stats */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <StatCard
          label="Active Actors"
          value={formatNumber(status.active_actors)}
        />
        <StatCard
          label="Messages Sent"
          value={formatNumber(m.messages_sent ?? 0)}
        />
        <StatCard
          label="Requests"
          value={formatNumber(m.requests_total ?? 0)}
          subtitle={
            (m.requests_timed_out ?? 0) > 0
              ? `${formatNumber(m.requests_timed_out ?? 0)} timed out`
              : undefined
          }
        />
        <StatCard
          label="Dead Lettered"
          value={formatNumber(m.messages_dead_lettered ?? 0)}
        />
      </div>

      {/* Detail Cards */}
      <div className="grid gap-4 md:grid-cols-2">
        {/* Node */}
        <DetailCard title="Node">
          <Row
            label="Active Actors"
            value={formatNumber(status.active_actors)}
            mono
          />
          <Row
            label="Registered Types"
            value={status.registered_types?.length ?? 0}
            mono
          />
          <Row
            label="Pending Schedules"
            value={formatNumber(status.pending_schedules)}
            mono
          />
          {isCluster && (
            <>
              <Row label="Epoch" value={status.epoch ?? 0} mono />
              <Row
                label="Lease Remaining"
                value={formatDuration(status.remaining_lease_ms ?? 0)}
                mono
              />
            </>
          )}
        </DetailCard>

        {/* Messaging */}
        <DetailCard title="Messaging">
          <Row
            label="Sent"
            value={formatNumber(m.messages_sent ?? 0)}
            mono
          />
          <Row
            label="Received"
            value={formatNumber(m.messages_received ?? 0)}
            mono
          />
          <Row
            label="Dead Lettered"
            value={formatNumber(m.messages_dead_lettered ?? 0)}
            mono
          />
        </DetailCard>

        {/* Requests */}
        <DetailCard title="Requests">
          <Row
            label="Total"
            value={formatNumber(m.requests_total ?? 0)}
            mono
          />
          <Row
            label="Timed Out"
            value={formatNumber(m.requests_timed_out ?? 0)}
            mono
          />
        </DetailCard>

        {/* Activations */}
        <DetailCard title="Activations">
          <Row
            label="Total"
            value={formatNumber(m.activations_total ?? 0)}
            mono
          />
          <Row
            label="Failed"
            value={formatNumber(m.activations_failed ?? 0)}
            mono
          />
        </DetailCard>

        {/* Schedules */}
        <DetailCard title="Schedules">
          <Row
            label="Fired"
            value={formatNumber(m.schedules_fired ?? 0)}
            mono
          />
          <Row
            label="Cancelled"
            value={formatNumber(m.schedules_cancelled ?? 0)}
            mono
          />
          <Row
            label="Recovered"
            value={formatNumber(m.schedules_recovered ?? 0)}
            mono
          />
        </DetailCard>

        {/* Placement Cache */}
        <DetailCard title="Placement Cache">
          <Row
            label="Size"
            value={formatNumber(status.placement_cache_size)}
            mono
          />
          <Row
            label="Hits"
            value={formatNumber(m.placement_cache_hits ?? 0)}
            mono
          />
          <Row
            label="Misses"
            value={formatNumber(m.placement_cache_misses ?? 0)}
            mono
          />
          {hitRate && <Row label="Hit Rate" value={`${hitRate}%`} mono />}
        </DetailCard>
      </div>
    </div>
  )
}
