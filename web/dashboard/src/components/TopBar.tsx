import { Link } from '@tanstack/react-router'
import { useClusterStatus } from '../hooks/use-cluster-status'
import StateBadge from './StateBadge'

export default function TopBar() {
  const { data } = useClusterStatus()

  return (
    <header className="border-b border-zinc-800 bg-zinc-900/50 px-4 py-3 sm:px-6 lg:px-8">
      <div className="mx-auto flex max-w-7xl items-center justify-between">
        <div className="flex items-center gap-4">
          <Link to="/" className="text-lg font-semibold text-zinc-100 hover:text-white">
            Theatre
          </Link>
          {data && <StateBadge state={data.state} />}
          <nav className="flex items-center gap-1 text-sm">
            <Link
              to="/"
              className="rounded-md px-2.5 py-1 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100"
              activeProps={{ className: 'rounded-md px-2.5 py-1 bg-zinc-800 text-zinc-100' }}
              activeOptions={{ exact: true }}
            >
              Overview
            </Link>
            <Link
              to="/actors"
              className="rounded-md px-2.5 py-1 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100"
              activeProps={{ className: 'rounded-md px-2.5 py-1 bg-zinc-800 text-zinc-100' }}
            >
              Actors
            </Link>
            <Link
              to="/hosts"
              className="rounded-md px-2.5 py-1 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100"
              activeProps={{ className: 'rounded-md px-2.5 py-1 bg-zinc-800 text-zinc-100' }}
            >
              Hosts
            </Link>
            <Link
              to="/schedules"
              className="rounded-md px-2.5 py-1 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100"
              activeProps={{ className: 'rounded-md px-2.5 py-1 bg-zinc-800 text-zinc-100' }}
            >
              Schedules
            </Link>
          </nav>
        </div>
        {data && (
          <code className="text-sm text-zinc-400 font-mono">
            {data.host_id}
          </code>
        )}
      </div>
    </header>
  )
}
