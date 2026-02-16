import { useQuery } from '@tanstack/react-query'
import { fetchAllClusterActors } from '../lib/api'

export function useClusterActors(limit: number = 50, offset: number = 0) {
  return useQuery({
    queryKey: ['cluster-actors', limit, offset],
    queryFn: () => fetchAllClusterActors(limit, offset),
    refetchInterval: 2000,
  })
}
