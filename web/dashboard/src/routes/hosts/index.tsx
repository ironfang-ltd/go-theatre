import { createFileRoute } from '@tanstack/react-router'
import { useClusterStatus } from '../../hooks/use-cluster-status'
import { useClusterHosts } from '../../hooks/use-cluster-hosts'
import StatCard from '../../components/StatCard'
import StateBadge from '../../components/StateBadge'
import { formatNumber, formatDuration } from '../../lib/format'

export const Route = createFileRoute('/hosts/')({
  component: HostsPage,
})

function HostsPage() {
  const { data: status } = useClusterStatus()
  const { data: hostsData, isLoading, error } = useClusterHosts()

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
  const isCluster = status && status.state !== 'standalone'

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
          label="Host"
          value={status?.host_id ?? '—'}
        />
        <StatCard
          label="Epoch"
          value={status?.epoch ?? '—'}
        />
        <StatCard
          label="Lease Remaining"
          value={
            isCluster
              ? formatDuration(status?.remaining_lease_ms ?? 0)
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
                <th className="px-4 py-3 font-medium text-zinc-400">
                  Epoch
                </th>
                <th className="px-4 py-3 font-medium text-zinc-400">
                  Lease Expiry
                </th>
                <th className="px-4 py-3 font-medium text-zinc-400" />
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-800">
              {hosts.map((host) => {
                const isSelf = status?.host_id === host.host_id
                return (
                  <tr key={host.host_id} className="bg-zinc-900">
                    <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-100">
                      <span className="flex items-center gap-2">
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
                    <td className="whitespace-nowrap px-4 py-3 text-zinc-300">
                      {host.epoch}
                    </td>
                    <td className="whitespace-nowrap px-4 py-3 text-zinc-300">
                      {host.lease_expiry || '—'}
                    </td>
                    <td className="whitespace-nowrap px-4 py-3">
                      {isSelf && status && (
                        <StateBadge state={status.state} />
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
