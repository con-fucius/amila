export type HistoryStatus = 'success' | 'error' | 'pending'

export interface NormalizedHistoryItem {
  id: string
  query: string
  status: HistoryStatus
  timestamp: Date
  executionTime?: number
  rowCount?: number
}

interface RawHistoryItem {
  [key: string]: any
}

/**
 * Normalize heterogeneous backend history entries into a small typed shape
 * used by both the chat interface and the Query Builder.
 */
export function normalizeHistoryItems(items: RawHistoryItem[] | unknown): NormalizedHistoryItem[] {
  if (!Array.isArray(items)) return []

  return items.map((raw, idx) => {
    const resultSummary = raw.result_summary || raw.metadata?.result_summary || {}
    const statusRaw = resultSummary.status || raw.status || raw.execution_status

    let status: HistoryStatus = 'success'
    if (statusRaw === 'error' || statusRaw === 'failed') status = 'error'
    else if (statusRaw === 'pending' || statusRaw === 'running' || statusRaw === 'processing') status = 'pending'

    const tsRaw = raw.timestamp || raw.time || raw.created_at
    const timestamp = tsRaw ? new Date(tsRaw) : new Date()

    const query = raw.user_query || raw.sql_query || raw.query || raw.original_query || ''
    const execMs = resultSummary.execution_time_ms ?? resultSummary.execution_time
    const rowCount = resultSummary.row_count ?? resultSummary.rows

    return {
      id: raw.entry_id || raw.id || raw.query_id || `${timestamp.getTime()}_${idx}`,
      query,
      status,
      timestamp,
      executionTime: typeof execMs === 'number' ? execMs : undefined,
      rowCount: typeof rowCount === 'number' ? rowCount : undefined,
    }
  })
}
