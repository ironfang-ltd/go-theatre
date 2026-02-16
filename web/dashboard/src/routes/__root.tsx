import { Outlet, createRootRouteWithContext } from '@tanstack/react-router'

import TopBar from '../components/TopBar'

import type { QueryClient } from '@tanstack/react-query'

interface MyRouterContext {
  queryClient: QueryClient
}

export const Route = createRootRouteWithContext<MyRouterContext>()({
  component: () => (
    <div className="min-h-screen bg-zinc-950">
      <TopBar />
      <main className="mx-auto max-w-7xl py-6">
        <Outlet />
      </main>
    </div>
  ),
})
