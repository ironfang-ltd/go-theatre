import { createFileRoute, Link } from '@tanstack/react-router'
import { useState, useMemo } from 'react'
import { useClusterActors } from '../../hooks/use-cluster-actors'
import ActorStatusBadge from '../../components/ActorStatusBadge'
import Select from '../../components/Select'
import { formatNumber } from '../../lib/format'
import type { ActorEntry } from '../../lib/api'

export const Route = createFileRoute('/actors/')({
  component: ActorsPage,
})

const PAGE_SIZES = [10, 25, 50, 100] as const

function ActorsPage() {
  const [pageSize, setPageSize] = useState<number>(50)
  const [offset, setOffset] = useState(0)
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState<'all' | 'active' | 'inactive'>('all')

  const { data, isLoading, error } = useClusterActors(pageSize, offset)

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

    return actors
  }, [data, search, statusFilter])

  if (isLoading && !data) {
    return <p className="text-zinc-500">Loading...</p>
  }

  if (error) {
    return (
      <p className="text-red-400">
        Failed to load actors: {error.message}
      </p>
    )
  }

  const total = data?.total ?? 0
  const page = Math.floor(offset / pageSize) + 1
  const totalPages = Math.ceil(total / pageSize)
  const showingFrom = total === 0 ? 0 : offset + 1
  const showingTo = Math.min(offset + pageSize, total)

  function goToPage(newPage: number) {
    const newOffset = (newPage - 1) * pageSize
    setOffset(Math.max(0, Math.min(newOffset, total - 1)))
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <h1 className="text-lg font-semibold text-zinc-100">
          Actors{' '}
          <span className="text-sm font-normal text-zinc-500">
            ({formatNumber(total)})
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
          <Select
            value={statusFilter}
            onChange={(e) =>
              setStatusFilter(e.target.value as 'all' | 'active' | 'inactive')
            }
          >
            <option value="all">All statuses</option>
            <option value="active">Active</option>
            <option value="inactive">Inactive</option>
          </Select>
          <Select
            value={pageSize}
            onChange={(e) => {
              setPageSize(Number(e.target.value))
              setOffset(0)
            }}
          >
            {PAGE_SIZES.map((s) => (
              <option key={s} value={s}>
                {s} per page
              </option>
            ))}
          </Select>
        </div>
      </div>

      {filtered.length === 0 ? (
        <p className="text-zinc-500 text-sm">No actors match the filter.</p>
      ) : (
        <ActorsTable actors={filtered} />
      )}

      {/* Pagination */}
      {total > 0 && (
        <div className="flex items-center justify-between text-sm text-zinc-400">
          <span>
            Showing {formatNumber(showingFrom)}&ndash;{formatNumber(showingTo)} of{' '}
            {formatNumber(total)}
          </span>
          <div className="flex items-center gap-2">
            <button
              onClick={() => goToPage(page - 1)}
              disabled={page <= 1}
              className="rounded-md border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-zinc-300 hover:bg-zinc-800 disabled:opacity-40 disabled:cursor-not-allowed"
            >
              Prev
            </button>
            <span className="text-zinc-500">
              Page {page} of {totalPages}
            </span>
            <button
              onClick={() => goToPage(page + 1)}
              disabled={page >= totalPages}
              className="rounded-md border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-zinc-300 hover:bg-zinc-800 disabled:opacity-40 disabled:cursor-not-allowed"
            >
              Next
            </button>
          </div>
        </div>
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
            <th className="px-4 py-3 font-medium text-zinc-400">ID</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Host</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Type</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Inbox</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Tasks</th>
            <th className="px-4 py-3 font-medium text-zinc-400">Last Message</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-zinc-800">
          {actors.map((a) => (
            <tr key={`${a.host_id}-${a.type}:${a.id}`} className="bg-zinc-900">
              <td className="whitespace-nowrap px-4 py-3">
                <ActorStatusBadge status={a.status} />
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
              <td className="whitespace-nowrap px-4 py-3 font-mono text-zinc-300">
                {a.host_id ?? '—'}
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
              <td className="whitespace-nowrap px-4 py-3 text-zinc-300">
                {a.inbox_size} / {a.inbox_cap}
              </td>
              <td className="whitespace-nowrap px-4 py-3 text-zinc-300">
                {a.running_tasks > 0 ? (
                  <span className="inline-flex items-center rounded-full bg-sky-500/15 border border-sky-500/25 px-2 py-0.5 text-xs font-medium text-sky-400">
                    {a.running_tasks}
                  </span>
                ) : (
                  '—'
                )}
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
