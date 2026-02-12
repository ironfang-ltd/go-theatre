import { createFileRoute, Link } from '@tanstack/react-router'
import { useState, useMemo } from 'react'
import { useClusterActors } from '../../hooks/use-cluster-actors'
import ActorStatusBadge from '../../components/ActorStatusBadge'
import type { ActorEntry } from '../../lib/api'

export const Route = createFileRoute('/actors/')({
  component: ActorsPage,
})

function ActorsPage() {
  const { data, isLoading, error } = useClusterActors()
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState<'all' | 'active' | 'inactive'>('all')

  const filtered = useMemo(() => {
    if (!data?.actors) return []
    let actors = data.actors

    if (statusFilter !== 'all') {
      actors = actors.filter((a) => a.status === statusFilter)
    }

    if (search) {
      const q = search.toLowerCase()
      actors = actors.filter(
        (a) =>
          a.type.toLowerCase().includes(q) ||
          a.id.toLowerCase().includes(q),
      )
    }

    return actors.sort((a, b) => {
      const cmp = a.type.localeCompare(b.type)
      return cmp !== 0 ? cmp : a.id.localeCompare(b.id)
    })
  }, [data, search, statusFilter])

  if (isLoading) {
    return <p className="text-zinc-500">Loading...</p>
  }

  if (error) {
    return (
      <p className="text-red-400">
        Failed to load actors: {error.message}
      </p>
    )
  }

  const total = data?.actors?.length ?? 0

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <h1 className="text-lg font-semibold text-zinc-100">
          Actors{' '}
          <span className="text-sm font-normal text-zinc-500">
            {filtered.length === total
              ? `(${total})`
              : `(${filtered.length} / ${total})`}
          </span>
        </h1>

        <div className="flex items-center gap-3">
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Filter by type or id..."
            className="rounded-md border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 placeholder-zinc-500 outline-none focus:border-zinc-500"
          />
          <select
            value={statusFilter}
            onChange={(e) =>
              setStatusFilter(e.target.value as 'all' | 'active' | 'inactive')
            }
            className="rounded-md border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none focus:border-zinc-500"
          >
            <option value="all">All statuses</option>
            <option value="active">Active</option>
            <option value="inactive">Inactive</option>
          </select>
        </div>
      </div>

      {filtered.length === 0 ? (
        <p className="text-zinc-500 text-sm">No actors match the filter.</p>
      ) : (
        <ActorsTable actors={filtered} />
      )}
    </div>
  )
}

function ActorsTable({ actors }: { actors: ActorEntry[] }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-zinc-800">
      <table className="w-full text-left text-sm">
        <thead className="border-b border-zinc-800 bg-zinc-900/50">
          <tr>
            <th className="px-4 py-3 font-medium text-zinc-400">Status</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Type</th>
            <th className="px-4 py-3 font-medium text-zinc-400">ID</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Inbox</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Last Message</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-zinc-800">
          {actors.map((a) => (
            <tr key={`${a.type}:${a.id}`} className="bg-zinc-900">
              <td className="whitespace-nowrap px-4 py-3">
                <ActorStatusBadge status={a.status} />
              </td>
              <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-100">
                <Link
                  to="/actors/$type/$id"
                  params={{ type: a.type, id: a.id }}
                  className="hover:text-white hover:underline"
                >
                  {a.type}
                </Link>
              </td>
              <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-100">
                <Link
                  to="/actors/$type/$id"
                  params={{ type: a.type, id: a.id }}
                  className="hover:text-white hover:underline"
                >
                  {a.id}
                </Link>
              </td>
              <td className="whitespace-nowrap px-4 py-3 text-zinc-300">
                {a.inbox_size} / {a.inbox_cap}
              </td>
              <td className="whitespace-nowrap px-4 py-3 text-zinc-300">
                {a.last_message
                  ? new Date(a.last_message).toLocaleTimeString()
                  : '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
