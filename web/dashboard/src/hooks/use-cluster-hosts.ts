import { useQuery } from '@tanstack/react-query'
import { fetchClusterHosts } from '../lib/api'

export function useClusterHosts() {
  return useQuery({
    queryKey: ['cluster-hosts'],
    queryFn: fetchClusterHosts,
    refetchInterval: 2000,
  })
}
