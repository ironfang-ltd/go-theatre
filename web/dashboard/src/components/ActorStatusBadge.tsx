const statusStyles: Record<string, string> = {
  active: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/25',
  inactive: 'bg-zinc-500/15 text-zinc-400 border-zinc-500/25',
}

interface ActorStatusBadgeProps {
  status: string
}

export default function ActorStatusBadge({ status }: ActorStatusBadgeProps) {
  const style = statusStyles[status] ?? statusStyles.inactive
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-medium ${style}`}
    >
      {status}
    </span>
  )
}
