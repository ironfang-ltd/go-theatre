const stateStyles: Record<string, string> = {
  clustered: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/25',
  frozen: 'bg-sky-500/15 text-sky-400 border-sky-500/25',
  draining: 'bg-amber-500/15 text-amber-400 border-amber-500/25',
  standalone: 'bg-zinc-500/15 text-zinc-400 border-zinc-500/25',
}

interface StateBadgeProps {
  state: string
}

export default function StateBadge({ state }: StateBadgeProps) {
  const style = stateStyles[state] ?? stateStyles.standalone
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-medium ${style}`}
    >
      {state}
    </span>
  )
}
