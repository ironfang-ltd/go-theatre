interface DetailCardProps {
  title: string
  children: React.ReactNode
}

export default function DetailCard({ title, children }: DetailCardProps) {
  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-900">
      <div className="border-b border-zinc-800 px-4 py-2.5">
        <h3 className="text-xs font-medium uppercase tracking-wider text-zinc-500">
          {title}
        </h3>
      </div>
      <dl className="divide-y divide-zinc-800/50">{children}</dl>
    </div>
  )
}

interface RowProps {
  label: string
  value: React.ReactNode
  mono?: boolean
}

export function Row({ label, value, mono }: RowProps) {
  return (
    <div className="flex items-center justify-between px-4 py-2.5">
      <dt className="text-sm text-zinc-400">{label}</dt>
      <dd
        className={`text-sm text-zinc-100 ${mono ? 'font-mono' : ''}`}
      >
        {value}
      </dd>
    </div>
  )
}
