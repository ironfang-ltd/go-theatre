import { useQuery } from '@tanstack/react-query'
import { fetchAllClusterErrors } from '../lib/api'

export function useClusterErrors() {
  return useQuery({
    queryKey: ['cluster-errors'],
    queryFn: () => fetchAllClusterErrors(100),
    refetchInterval: 2000,
  })
}
