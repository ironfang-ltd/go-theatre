import { useQuery } from '@tanstack/react-query'
import { fetchAllClusterSchedules } from '../lib/api'

export function useClusterSchedules() {
  return useQuery({
    queryKey: ['cluster-schedules'],
    queryFn: fetchAllClusterSchedules,
    refetchInterval: 2000,
  })
}
