import { Fragment, useState } from 'react'
import { createFileRoute } from '@tanstack/react-router'
import { useAllClusterStatus } from '../../hooks/use-all-cluster-status'
import { useClusterHosts } from '../../hooks/use-cluster-hosts'
import StatCard from '../../components/StatCard'
import StateBadge from '../../components/StateBadge'
import { formatNumber, formatDuration, formatMB, formatPct, formatMicroseconds } from '../../lib/format'
import type { PerHostStatus, PeerStats } from '../../lib/api'

export const Route = createFileRoute('/hosts/')({
  component: HostsPage,
})

function HostsPage() {
  const { data: allStatus } = useAllClusterStatus()
  const { data: hostsData, isLoading, error } = useClusterHosts()
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set())

  if (isLoading) {
    return <p className="text-zinc-500">Loading...</p>
  }

  if (error) {
    return (
      <p className="text-red-400">
        Failed to load hosts: {error.message}
      </p>
    )
  }

  const hosts = hostsData?.hosts ?? []
  const isCluster = allStatus && allStatus.state !== 'standalone'
  const perHost = allStatus?.hosts ?? []

  // Build lookup from host_id → per-host status.
  const hostStats = new Map<string, PerHostStatus>()
  for (const h of perHost) {
    hostStats.set(h.host_id, h)
  }

  const toggleExpand = (hostId: string) => {
    setCollapsed((prev) => {
      const next = new Set(prev)
      if (next.has(hostId)) {
        next.delete(hostId)
      } else {
        next.add(hostId)
      }
      return next
    })
  }

  const colCount = 10

  return (
    <div className="space-y-6">
      <h1 className="text-lg font-semibold text-zinc-100">
        Cluster Hosts{' '}
        <span className="text-sm font-normal text-zinc-500">
          ({hosts.length})
        </span>
      </h1>

      {/* Headline Stats */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <StatCard label="Hosts" value={formatNumber(hosts.length)} />
        <StatCard
          label="Goroutines"
          value={formatNumber(allStatus?.goroutines ?? 0)}
          subtitle="cluster total"
        />
        <StatCard
          label="Heap"
          value={formatMB(allStatus?.heap_alloc_mb ?? 0)}
          subtitle="cluster total"
        />
        <StatCard
          label="Lease Remaining"
          value={
            isCluster
              ? formatDuration(allStatus?.remaining_lease_ms ?? 0)
              : '—'
          }
        />
      </div>

      {/* Hosts Table */}
      {hosts.length === 0 ? (
        <p className="text-zinc-500 text-sm">
          No cluster hosts found. Running in standalone mode.
        </p>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-zinc-800">
          <table className="w-full text-left text-sm">
            <thead className="border-b border-zinc-800 bg-zinc-900/50">
              <tr>
                <th className="px-4 py-3 font-medium text-zinc-400">
                  Host ID
                </th>
                <th className="px-4 py-3 font-medium text-zinc-400">
                  Address
                </th>
                <th className="px-4 py-3 font-medium text-zinc-400 text-right">
                  Actors
                </th>
                <th className="px-4 py-3 font-medium text-zinc-400 text-right">
                  Goroutines
                </th>
                <th className="px-4 py-3 font-medium text-zinc-400 text-right">
                  Heap
                </th>
                <th className="px-4 py-3 font-medium text-zinc-400 text-right">
                  GC Pause
                </th>
                <th className="px-4 py-3 font-medium text-zinc-400 text-right">
                  Outbox
                </th>
                <th className="px-4 py-3 font-medium text-zinc-400 text-right">
                  Inbox
                </th>
                <th className="px-4 py-3 font-medium text-zinc-400 text-right">
                  Peers
                </th>
                <th className="px-4 py-3 font-medium text-zinc-400" />
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-800">
              {hosts.map((host) => {
                const isSelf = allStatus?.host_id === host.host_id
                const stats = hostStats.get(host.host_id)
                const isExpanded = !collapsed.has(host.host_id)
                const peers = stats?.transport_peers_detail ?? []
                const hasPeers = peers.length > 0
                return (
                  <Fragment key={host.host_id}>
                    <tr
                      className={`bg-zinc-900 ${hasPeers ? 'cursor-pointer hover:bg-zinc-800/50' : ''}`}
                      onClick={() => hasPeers && toggleExpand(host.host_id)}
                    >
                      <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-100">
                        <span className="flex items-center gap-2">
                          {hasPeers && (
                            <span className="text-zinc-500 w-4 inline-block text-center">
                              {isExpanded ? '\u25BE' : '\u25B8'}
                            </span>
                          )}
                          {host.host_id}
                          {isSelf && (
                            <span className="rounded bg-zinc-800 px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-zinc-500">
                              self
                            </span>
                          )}
                        </span>
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-300">
                        {host.address}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-300 text-right">
                        {stats ? formatNumber(stats.active_actors) : '—'}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-300 text-right">
                        {stats ? formatNumber(stats.goroutines) : '—'}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-300 text-right">
                        {stats ? formatMB(stats.heap_alloc_mb) : '—'}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-300 text-right">
                        {stats ? formatMicroseconds(stats.gc_pause_us) : '—'}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-300 text-right">
                        {stats ? (
                          <span title={`${stats.outbox_depth} / ${stats.outbox_cap}`}>
                            {formatPct(stats.outbox_cap > 0 ? (stats.outbox_depth / stats.outbox_cap) * 100 : 0)}
                          </span>
                        ) : '—'}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-300 text-right">
                        {stats ? (
                          <span title={`${stats.inbox_depth} / ${stats.inbox_cap}`}>
                            {formatPct(stats.inbox_cap > 0 ? (stats.inbox_depth / stats.inbox_cap) * 100 : 0)}
                          </span>
                        ) : '—'}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-300 text-right">
                        {stats ? stats.transport_peers : '—'}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3">
                        {isSelf && allStatus && (
                          <StateBadge state={allStatus.state} />
                        )}
                      </td>
                    </tr>
                    {isExpanded && hasPeers && (
                      <tr key={`${host.host_id}-peers`} className="bg-zinc-950">
                        <td colSpan={colCount} className="px-6 py-3">
                          <PeerTable peers={peers} />
                        </td>
                      </tr>
                    )}
                  </Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Per-Host Throughput */}
      {perHost.length > 0 && (
        <>
          <h2 className="text-base font-semibold text-zinc-100 mt-8">
            Per-Host Throughput
          </h2>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {perHost.map((h) => (
              <div
                key={h.host_id}
                className="rounded-lg border border-zinc-800 bg-zinc-900 p-4 space-y-2"
              >
                <p className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                  {h.host_id}
                </p>
                <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                  <span className="text-zinc-400">Recv</span>
                  <span className="text-zinc-100 font-mono text-right">
                    {formatNumber(h.metrics?.messages_received ?? 0)}
                  </span>
                  <span className="text-zinc-400">Sent</span>
                  <span className="text-zinc-100 font-mono text-right">
                    {formatNumber(h.metrics?.messages_sent ?? 0)}
                  </span>
                  <span className="text-zinc-400">Requests</span>
                  <span className="text-zinc-100 font-mono text-right">
                    {formatNumber(h.metrics?.requests_total ?? 0)}
                  </span>
                  <span className="text-zinc-400">Dead Letters</span>
                  <span className="text-zinc-100 font-mono text-right">
                    {formatNumber(h.metrics?.messages_dead_lettered ?? 0)}
                  </span>
                  <span className="text-zinc-400">Send Queue</span>
                  <span className="text-zinc-100 font-mono text-right">
                    {formatNumber(h.transport_send_queue)}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

function PeerTable({ peers }: { peers: PeerStats[] }) {
  return (
    <table className="w-full text-left text-xs">
      <thead>
        <tr className="text-zinc-500 border-b border-zinc-800">
          <th className="pb-2 pr-4 font-medium">Peer</th>
          <th className="pb-2 pr-4 font-medium">Address</th>
          <th className="pb-2 pr-4 font-medium text-right">Sent</th>
          <th className="pb-2 pr-4 font-medium text-right">Received</th>
          <th className="pb-2 pr-4 font-medium text-right">Errors</th>
          <th className="pb-2 pr-4 font-medium text-right">Queue</th>
          <th className="pb-2 pr-4 font-medium text-right">Latency</th>
          <th className="pb-2 font-medium text-center">Status</th>
        </tr>
      </thead>
      <tbody>
        {peers.map((p) => (
          <tr key={p.host_id} className="border-b border-zinc-800/50">
            <td className="py-1.5 pr-4 font-mono text-zinc-300">
              {p.host_id}
            </td>
            <td className="py-1.5 pr-4 font-mono text-zinc-400">
              {p.address}
            </td>
            <td className="py-1.5 pr-4 font-mono text-zinc-300 text-right">
              {formatNumber(p.messages_sent)}
            </td>
            <td className="py-1.5 pr-4 font-mono text-zinc-300 text-right">
              {formatNumber(p.messages_received)}
            </td>
            <td className="py-1.5 pr-4 font-mono text-right">
              <span className={p.send_errors > 0 ? 'text-red-400' : 'text-zinc-500'}>
                {formatNumber(p.send_errors)}
              </span>
            </td>
            <td className="py-1.5 pr-4 font-mono text-zinc-300 text-right">
              {formatNumber(p.send_queue)}
            </td>
            <td className="py-1.5 pr-4 font-mono text-zinc-300 text-right">
              {p.latency_us > 0 ? formatMicroseconds(p.latency_us) : '—'}
            </td>
            <td className="py-1.5 text-center">
              <span
                className={`inline-block h-2 w-2 rounded-full ${
                  p.connected ? 'bg-emerald-500' : 'bg-zinc-600'
                }`}
                title={p.connected ? 'Connected' : 'Disconnected'}
              />
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
