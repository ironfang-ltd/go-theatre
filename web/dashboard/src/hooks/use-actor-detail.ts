import { useQuery } from '@tanstack/react-query'
import { fetchActorDetail } from '../lib/api'

export function useActorDetail(type: string, id: string) {
  return useQuery({
    queryKey: ['actor-detail', type, id],
    queryFn: () => fetchActorDetail(type, id),
    refetchInterval: 2000,
  })
}
