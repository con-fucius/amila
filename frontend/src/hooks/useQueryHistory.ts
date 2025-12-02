import { useCallback, useState } from 'react'
import { apiService } from '@/services/apiService'
import { normalizeHistoryItems, NormalizedHistoryItem } from '@/utils/history'

export function useQueryHistory(sessionId: string | null, defaultLimit: number = 10) {
  const [items, setItems] = useState<NormalizedHistoryItem[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const loadHistory = useCallback(
    async (limit?: number) => {
      const effectiveLimit = limit ?? defaultLimit
      try {
        setLoading(true)
        setError(null)
        if (!sessionId) {
          // No session id yet: keep at most the last N items already loaded
          setItems((prev) => prev.slice(0, effectiveLimit))
          return
        }
        const res = await apiService.getQueryHistory(sessionId, effectiveLimit)
        const backendItems = res?.history || res?.items || []
        const normalized = normalizeHistoryItems(backendItems)
        setItems(normalized.slice(0, effectiveLimit))
      } catch (e: any) {
        setError(e?.message || 'Failed to load history')
        setItems((prev) => prev.slice(0, effectiveLimit))
      } finally {
        setLoading(false)
      }
    },
    [sessionId, defaultLimit],
  )

  return { items, setItems, loadHistory, loading, error }
}
