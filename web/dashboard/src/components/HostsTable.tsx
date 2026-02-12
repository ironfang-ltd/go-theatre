import type { HostEntry } from '../lib/api'

interface HostsTableProps {
  hosts: HostEntry[]
}

export default function HostsTable({ hosts }: HostsTableProps) {
  if (hosts.length === 0) return null

  return (
    <div className="overflow-x-auto rounded-lg border border-zinc-800">
      <table className="w-full text-left text-sm">
        <thead className="border-b border-zinc-800 bg-zinc-900/50">
          <tr>
            <th className="px-4 py-3 font-medium text-zinc-400">Host ID</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Address</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Epoch</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Lease Expiry</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-zinc-800">
          {hosts.map((host) => (
            <tr key={host.host_id} className="bg-zinc-900">
              <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-100">
                {host.host_id}
              </td>
              <td className="whitespace-nowrap px-4 py-3 text-zinc-300">
                {host.address}
              </td>
              <td className="whitespace-nowrap px-4 py-3 text-zinc-300">
                {host.epoch}
              </td>
              <td className="whitespace-nowrap px-4 py-3 text-zinc-300">
                {host.lease_expiry}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
