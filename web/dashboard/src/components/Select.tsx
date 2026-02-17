import { ChevronDown } from 'lucide-react'
import type { SelectHTMLAttributes } from 'react'

type SelectProps = SelectHTMLAttributes<HTMLSelectElement>

export default function Select({ className, children, ...props }: SelectProps) {
  return (
    <div className="relative inline-block">
      <select
        {...props}
        className={`appearance-none rounded-md border border-zinc-700 bg-zinc-900 py-1.5 pl-3 pr-8 text-sm text-zinc-100 outline-none focus:border-zinc-500 ${className ?? ''}`}
      >
        {children}
      </select>
      <ChevronDown className="pointer-events-none absolute right-2 top-1/2 size-4 -translate-y-1/2 text-zinc-400" />
    </div>
  )
}
