import { create } from 'zustand'
import { persist } from 'zustand/middleware'

export interface PinnedQuery {
  id: string
  query: string
  sql?: string
  timestamp: Date
  result?: {
    columns: string[]
    rows: any[]
    rowCount: number
  }
  chartConfig?: any
}

interface PinnedQueriesState {
  pinnedQueries: PinnedQuery[]
  addPinnedQuery: (query: PinnedQuery) => void
  removePinnedQuery: (id: string) => void
  updatePinnedQuery: (id: string, updates: Partial<PinnedQuery>) => void
  getPinnedQuery: (id: string) => PinnedQuery | undefined
}

export const usePinnedQueriesStore = create<PinnedQueriesState>()(
  persist(
    (set, get) => ({
      pinnedQueries: [],
      
      addPinnedQuery: (query) => {
        set((state) => ({
          pinnedQueries: [query, ...state.pinnedQueries].slice(0, 20) // Max 20 pinned queries
        }))
      },
      
      removePinnedQuery: (id) => {
        set((state) => ({
          pinnedQueries: state.pinnedQueries.filter(q => q.id !== id)
        }))
      },
      
      updatePinnedQuery: (id, updates) => {
        set((state) => ({
          pinnedQueries: state.pinnedQueries.map(q => 
            q.id === id ? { ...q, ...updates } : q
          )
        }))
      },
      
      getPinnedQuery: (id) => {
        return get().pinnedQueries.find(q => q.id === id)
      }
    }),
    {
      name: 'amila-pinned-queries',
    }
  )
)
