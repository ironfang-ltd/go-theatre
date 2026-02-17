import { createFileRoute } from '@tanstack/react-router'
import { useEffect } from 'react'
import { useAllClusterStatus } from '../hooks/use-all-cluster-status'
import { useRateHistory } from '../hooks/use-rate-history'
import StatCard from '../components/StatCard'
import DetailCard, { Row } from '../components/DetailCard'
import ThroughputChart from '../components/ThroughputChart'
import { formatNumber, formatDuration, formatMB, formatPct, formatMicroseconds } from '../lib/format'

export const Route = createFileRoute('/')({
  component: Dashboard,
})

function rateStr(n: number | undefined): string {
  if (n === undefined || n === 0) return ''
  return `${formatNumber(n)}/s`
}

function channelPct(depth: number, cap: number): string {
  if (cap === 0) return '0%'
  return formatPct((depth / cap) * 100)
}

function Dashboard() {
  const { data: status, isLoading, error, dataUpdatedAt } = useAllClusterStatus()
  const { history, push } = useRateHistory()

  // Push a new data point whenever fresh data arrives from the server.
  useEffect(() => {
    if (status) push(status)
  }, [dataUpdatedAt]) // eslint-disable-line react-hooks/exhaustive-deps

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
  const r = status.rates
  const isCluster = status.state !== 'standalone'

  const cacheTotal = (m.placement_cache_hits ?? 0) + (m.placement_cache_misses ?? 0)
  const hitRate =
    cacheTotal > 0
      ? ((m.placement_cache_hits ?? 0) / cacheTotal * 100).toFixed(1)
      : null

  return (
    <div className="space-y-6">
      {/* Throughput */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <StatCard
          label="Recv/s"
          value={rateStr(r.messages_received) || '0/s'}
          subtitle={`${formatNumber(m.messages_received ?? 0)} total`}
        />
        <StatCard
          label="Sent/s"
          value={rateStr(r.messages_sent) || '0/s'}
          subtitle={`${formatNumber(m.messages_sent ?? 0)} total`}
        />
        <StatCard
          label="Active Actors"
          value={formatNumber(status.active_actors)}
        />
        <StatCard
          label="Dead Lettered"
          value={formatNumber(m.messages_dead_lettered ?? 0)}
          subtitle={rateStr(r.messages_dead_lettered)}
        />
      </div>

      {/* Throughput Chart */}
      <ThroughputChart data={history} />

      {/* Detail Cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {/* Cluster */}
        <DetailCard title="Cluster">
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
            detail={rateStr(r.messages_sent)}
            mono
          />
          <Row
            label="Received"
            value={formatNumber(m.messages_received ?? 0)}
            detail={rateStr(r.messages_received)}
            mono
          />
          <Row
            label="Dead Lettered"
            value={formatNumber(m.messages_dead_lettered ?? 0)}
            detail={rateStr(r.messages_dead_lettered)}
            mono
          />
        </DetailCard>

        {/* Requests */}
        <DetailCard title="Requests">
          <Row
            label="Total"
            value={formatNumber(m.requests_total ?? 0)}
            detail={rateStr(r.requests_total)}
            mono
          />
          <Row
            label="Timed Out"
            value={formatNumber(m.requests_timed_out ?? 0)}
            mono
          />
        </DetailCard>

        {/* Runtime */}
        <DetailCard title="Runtime">
          <Row
            label="Goroutines"
            value={formatNumber(status.goroutines)}
            mono
          />
          <Row
            label="Heap Alloc"
            value={formatMB(status.heap_alloc_mb)}
            mono
          />
          <Row
            label="Heap Sys"
            value={formatMB(status.heap_sys_mb)}
            mono
          />
          <Row
            label="Last GC Pause"
            value={formatMicroseconds(status.gc_pause_us)}
            mono
          />
          <Row
            label="GC Cycles"
            value={formatNumber(status.num_gc)}
            mono
          />
        </DetailCard>

        {/* Transport */}
        <DetailCard title="Transport">
          <Row
            label="Peers"
            value={status.transport_peers}
            mono
          />
          <Row
            label="Connected"
            value={status.transport_connections}
            mono
          />
          <Row
            label="Send Queue"
            value={formatNumber(status.transport_send_queue)}
            mono
          />
        </DetailCard>

        {/* Backpressure */}
        <DetailCard title="Backpressure">
          <Row
            label="Outbox"
            value={`${formatNumber(status.outbox_depth)} / ${formatNumber(status.outbox_cap)}`}
            detail={channelPct(status.outbox_depth, status.outbox_cap)}
            mono
          />
          <Row
            label="Inbox"
            value={`${formatNumber(status.inbox_depth)} / ${formatNumber(status.inbox_cap)}`}
            detail={channelPct(status.inbox_depth, status.inbox_cap)}
            mono
          />
        </DetailCard>

        {/* Activations */}
        <DetailCard title="Activations">
          <Row
            label="Total"
            value={formatNumber(m.activations_total ?? 0)}
            detail={rateStr(r.activations_total)}
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

        {/* Background Tasks */}
        <DetailCard title="Background Tasks">
          <Row
            label="Spawned"
            value={formatNumber(m.tasks_spawned ?? 0)}
            detail={rateStr(r.tasks_spawned)}
            mono
          />
          <Row
            label="Completed"
            value={formatNumber(m.tasks_completed ?? 0)}
            detail={rateStr(r.tasks_completed)}
            mono
          />
          <Row
            label="Failed"
            value={formatNumber(m.tasks_failed ?? 0)}
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
            detail={rateStr(r.placement_cache_hits)}
            mono
          />
          <Row
            label="Misses"
            value={formatNumber(m.placement_cache_misses ?? 0)}
            detail={rateStr(r.placement_cache_misses)}
            mono
          />
          {hitRate && <Row label="Hit Rate" value={`${hitRate}%`} mono />}
        </DetailCard>
      </div>
    </div>
  )
}
