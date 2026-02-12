interface StatCardProps {
  label: string
  value: string | number
  subtitle?: string
}

export default function StatCard({ label, value, subtitle }: StatCardProps) {
  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-900 p-4">
      <p className="text-sm text-zinc-400">{label}</p>
      <p className="mt-1 text-2xl font-semibold tracking-tight text-zinc-100 truncate" title={String(value)}>
        {value}
      </p>
      {subtitle && (
        <p className="mt-1 text-xs text-zinc-500">{subtitle}</p>
      )}
    </div>
  )
}
