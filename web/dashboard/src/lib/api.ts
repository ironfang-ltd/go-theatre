// TypeScript types matching the Go admin server JSON responses.

export interface ClusterStatus {
  host_id: string
  state: 'standalone' | 'clustered' | 'frozen' | 'draining'
  epoch?: number
  remaining_lease_ms?: number
  renewal_failures?: number
  active_actors: number
  pending_schedules: number
  registered_types: string[] | null
  placement_cache_size: number
  metrics: Record<string, number>
}

export interface HostEntry {
  host_id: string
  address: string
  admin_addr?: string
  epoch: number
  lease_expiry: string
}

export interface ClusterHosts {
  hosts: HostEntry[]
}

export interface ActorEntry {
  type: string
  id: string
  status: 'active' | 'inactive'
  last_message?: string
  inbox_size: number
  inbox_cap: number
}

export interface ClusterActors {
  actors: ActorEntry[]
}

export interface ActorDetail {
  type: string
  id: string
  found: boolean
  status?: string
  receiver_type?: string
  created_at?: string
  last_message?: string
  uptime_ms?: number
  messages_total?: number
  errors_total?: number
  inbox_size?: number
  inbox_cap?: number
  owner_host?: string
  owner_addr?: string
  epoch?: number
}

export interface ScheduleEntry {
  id: number
  actor_type: string
  actor_id: string
  body: string
  kind: 'one-shot' | 'cron'
  cron_expr?: string
  next_fire: string
  host_id?: string // added client-side when aggregating
}

export interface ClusterSchedules {
  schedules: ScheduleEntry[]
}

export interface ClusterTypes {
  types: string[]
}

// fetchAllClusterSchedules fans out to every known host and merges results.
export async function fetchAllClusterSchedules(): Promise<ClusterSchedules> {
  // Get host list from the local host.
  const hostsRes = await fetch('/cluster/hosts')
  if (!hostsRes.ok) throw new Error(`GET /cluster/hosts: ${hostsRes.status}`)
  const hostsData: ClusterHosts = await hostsRes.json()

  // Also get the local host ID so we can tag local schedules.
  const statusRes = await fetch('/cluster/status')
  if (!statusRes.ok) throw new Error(`GET /cluster/status: ${statusRes.status}`)
  const statusData: ClusterStatus = await statusRes.json()

  const hosts = hostsData.hosts ?? []
  const allSchedules: ScheduleEntry[] = []

  // Fan out to all hosts with admin addresses.
  const fetches = hosts
    .filter((h) => h.admin_addr)
    .map(async (h) => {
      try {
        const url =
          h.host_id === statusData.host_id
            ? '/cluster/schedules' // local — use relative URL (works with proxy)
            : `http://${h.admin_addr}/cluster/schedules`
        const res = await fetch(url)
        if (!res.ok) return
        const data: ClusterSchedules = await res.json()
        for (const s of data.schedules ?? []) {
          allSchedules.push({ ...s, host_id: h.host_id })
        }
      } catch {
        // Host unreachable — skip silently.
      }
    })

  await Promise.all(fetches)
  return { schedules: allSchedules }
}

export async function fetchClusterTypes(): Promise<ClusterTypes> {
  const res = await fetch('/cluster/types')
  if (!res.ok) throw new Error(`GET /cluster/types: ${res.status}`)
  return res.json()
}

export async function fetchActorDetail(
  type: string,
  id: string,
): Promise<ActorDetail> {
  const params = new URLSearchParams({ type, id })
  const res = await fetch(`/cluster/actor-detail?${params}`)
  if (!res.ok) throw new Error(`GET /cluster/actor-detail: ${res.status}`)
  return res.json()
}

export async function fetchClusterActors(): Promise<ClusterActors> {
  const res = await fetch('/cluster/actors')
  if (!res.ok) throw new Error(`GET /cluster/actors: ${res.status}`)
  return res.json()
}

export async function fetchClusterStatus(): Promise<ClusterStatus> {
  const res = await fetch('/cluster/status')
  if (!res.ok) throw new Error(`GET /cluster/status: ${res.status}`)
  return res.json()
}

export async function fetchClusterHosts(): Promise<ClusterHosts> {
  const res = await fetch('/cluster/hosts')
  if (!res.ok) throw new Error(`GET /cluster/hosts: ${res.status}`)
  return res.json()
}
