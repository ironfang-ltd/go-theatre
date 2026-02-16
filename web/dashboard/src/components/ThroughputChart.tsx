import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  Legend,
} from 'recharts'
import type { RateDataPoint } from '../hooks/use-rate-history'
import { formatNumber } from '../lib/format'

interface Props {
  data: RateDataPoint[]
}

function formatYAxis(value: number): string {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`
  if (value >= 1_000) return `${(value / 1_000).toFixed(0)}K`
  return String(value)
}

const rateKeys = new Set(['recv', 'sent', 'deadLettered'])

function CustomTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null
  return (
    <div className="rounded-md border border-zinc-700 bg-zinc-900 px-3 py-2 text-xs shadow-lg">
      <p className="text-zinc-400 mb-1">{label}</p>
      {payload.map((entry: any) => (
        <p key={entry.dataKey} style={{ color: entry.color }}>
          {entry.name}: {formatNumber(entry.value)}
          {rateKeys.has(entry.dataKey) ? '/s' : ''}
        </p>
      ))}
    </div>
  )
}

// Show ~10 tick labels spread evenly across whatever data we have.
function pickTicks(data: RateDataPoint[]): string[] {
  if (data.length <= 10) return data.map((d) => d.label)
  const step = Math.ceil(data.length / 10)
  const ticks: string[] = []
  for (let i = 0; i < data.length; i += step) {
    ticks.push(data[i].label)
  }
  return ticks
}

export default function ThroughputChart({ data }: Props) {
  const ticks = pickTicks(data)

  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-900 p-4">
      <h3 className="mb-3 text-xs font-medium uppercase tracking-wider text-zinc-500">
        Throughput (last 5 min)
      </h3>
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
          <XAxis
            dataKey="label"
            tick={{ fill: '#71717a', fontSize: 11 }}
            ticks={ticks}
            interval="preserveStartEnd"
            stroke="#3f3f46"
          />
          <YAxis
            yAxisId="left"
            tickFormatter={formatYAxis}
            tick={{ fill: '#71717a', fontSize: 11 }}
            stroke="#3f3f46"
            width={48}
          />
          <YAxis
            yAxisId="right"
            orientation="right"
            tickFormatter={formatYAxis}
            tick={{ fill: '#71717a', fontSize: 11 }}
            stroke="#3f3f46"
            width={48}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend
            wrapperStyle={{ fontSize: 12, color: '#a1a1aa' }}
          />
          <Line
            yAxisId="left"
            type="monotone"
            dataKey="recv"
            name="Recv/s"
            stroke="#34d399"
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
          />
          <Line
            yAxisId="left"
            type="monotone"
            dataKey="sent"
            name="Sent/s"
            stroke="#60a5fa"
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
          />
          <Line
            yAxisId="left"
            type="monotone"
            dataKey="deadLettered"
            name="Dead/s"
            stroke="#f87171"
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
          />
          <Line
            yAxisId="right"
            type="monotone"
            dataKey="actors"
            name="Actors"
            stroke="#a78bfa"
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
