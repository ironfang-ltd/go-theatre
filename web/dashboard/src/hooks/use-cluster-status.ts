import { useQuery } from '@tanstack/react-query'
import { fetchClusterStatus } from '../lib/api'

export function useClusterStatus() {
  return useQuery({
    queryKey: ['cluster-status'],
    queryFn: fetchClusterStatus,
    refetchInterval: 2000,
  })
}
