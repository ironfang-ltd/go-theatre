import { useQuery } from '@tanstack/react-query'
import { fetchClusterActors } from '../lib/api'

export function useClusterActors() {
  return useQuery({
    queryKey: ['cluster-actors'],
    queryFn: fetchClusterActors,
    refetchInterval: 2000,
  })
}
