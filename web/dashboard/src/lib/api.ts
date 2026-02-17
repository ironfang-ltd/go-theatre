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

  // Runtime stats.
  goroutines: number
  heap_alloc_mb: number
  heap_sys_mb: number
  gc_pause_us: number
  num_gc: number

  // Channel depths (backpressure).
  outbox_depth: number
  outbox_cap: number
  inbox_depth: number
  inbox_cap: number

  // Transport stats.
  transport_peers: number
  transport_connections: number
  transport_send_queue: number
  transport_peers_detail?: PeerStats[]

  // Per-host breakdown (only present in all-status).
  hosts?: PerHostStatus[]
}

export interface PeerStats {
  host_id: string
  address: string
  connected: boolean
  messages_sent: number
  messages_received: number
  send_errors: number
  send_queue: number
  latency_us: number
}

export interface PerHostStatus {
  host_id: string
  state: string
  active_actors: number
  goroutines: number
  heap_alloc_mb: number
  gc_pause_us: number
  outbox_depth: number
  outbox_cap: number
  inbox_depth: number
  inbox_cap: number
  transport_peers: number
  transport_send_queue: number
  transport_peers_detail?: PeerStats[]
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
  host_id?: string
}

export interface ClusterActors {
  actors: ActorEntry[]
  total: number
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
  host_id?: string
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

export interface ErrorEntry {
  time: string
  level: 'error' | 'warn'
  source: string
  message: string
  actor?: string
  detail?: string
  host_id?: string
}

export interface ClusterErrors {
  errors: ErrorEntry[]
  total: number
}

// fetchAllClusterSchedules fetches cluster-wide schedules from the server-side aggregation endpoint.
export async function fetchAllClusterSchedules(): Promise<ClusterSchedules> {
  const res = await fetch('/cluster/all-schedules')
  if (!res.ok) throw new Error(`GET /cluster/all-schedules: ${res.status}`)
  return res.json()
}

// fetchAllClusterActors fetches cluster-wide actors with server-side pagination.
export async function fetchAllClusterActors(
  limit: number = 50,
  offset: number = 0,
): Promise<ClusterActors> {
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  })
  const res = await fetch(`/cluster/all-actors?${params}`)
  if (!res.ok) throw new Error(`GET /cluster/all-actors: ${res.status}`)
  return res.json()
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
  const res = await fetch(`/cluster/all-actor-detail?${params}`)
  if (!res.ok) throw new Error(`GET /cluster/all-actor-detail: ${res.status}`)
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

export async function fetchAllClusterStatus(): Promise<ClusterStatus> {
  const res = await fetch('/cluster/all-status')
  if (!res.ok) throw new Error(`GET /cluster/all-status: ${res.status}`)
  return res.json()
}

export async function fetchClusterHosts(): Promise<ClusterHosts> {
  const res = await fetch('/cluster/hosts')
  if (!res.ok) throw new Error(`GET /cluster/hosts: ${res.status}`)
  return res.json()
}

export async function fetchAllClusterErrors(limit: number = 50): Promise<ClusterErrors> {
  const params = new URLSearchParams({ limit: String(limit) })
  const res = await fetch(`/cluster/all-errors?${params}`)
  if (!res.ok) throw new Error(`GET /cluster/all-errors: ${res.status}`)
  return res.json()
}
