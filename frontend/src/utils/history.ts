export type HistoryStatus = 'success' | 'error' | 'pending' | 'rejected'

export interface NormalizedHistoryItem {
  id: string
  query: string
  status: HistoryStatus
  timestamp: Date
  executionTime?: number
  rowCount?: number
  sql?: string
  error?: string
  retryCount?: number
  databaseType?: 'oracle' | 'doris'
  columns?: string[]
  thinkingSteps?: string[]
  insights?: string[]
  nodeHistory?: Array<{ name: string; duration?: number; status?: string }>
}

interface RawHistoryItem {
  [key: string]: any
}

/**
 * Normalize heterogeneous backend history entries into a typed shape
 * used by both the chat interface and the Query Builder.
 * Enhanced to include SQL, errors, columns, and execution timeline.
 */
export function normalizeHistoryItems(items: RawHistoryItem[] | unknown): NormalizedHistoryItem[] {
  if (!Array.isArray(items)) return []

  return items.map((raw, idx) => {
    const resultSummary = raw.result_summary || raw.metadata?.result_summary || {}
    const statusRaw = resultSummary.status || raw.status || raw.execution_status

    let status: HistoryStatus = 'success'
    if (statusRaw === 'error' || statusRaw === 'failed') status = 'error'
    else if (statusRaw === 'pending' || statusRaw === 'running' || statusRaw === 'processing') status = 'pending'
    else if (statusRaw === 'rejected') status = 'rejected'

    const tsRaw = raw.timestamp || raw.time || raw.created_at
    const timestamp = tsRaw ? new Date(tsRaw) : new Date()

    const query = raw.user_query || raw.sql_query || raw.query || raw.original_query || ''
    const execMs = resultSummary.execution_time_ms ?? resultSummary.execution_time
    const rowCount = resultSummary.row_count ?? resultSummary.rows

    // Extract SQL query
    const sql = raw.generated_sql || raw.sql_query || raw.sql || resultSummary.sql || undefined

    // Extract error message
    const error = raw.error || raw.error_message || resultSummary.error || undefined

    // Extract database type
    const dbType = raw.database_type || raw.db_type || resultSummary.database_type
    const databaseType = dbType === 'oracle' || dbType === 'doris' ? dbType : undefined

    // Extract columns from results
    const columns = resultSummary.columns || raw.columns || raw.result?.columns || undefined

    // Extract thinking steps from LLM metadata
    const thinkingSteps = raw.llm_metadata?.thinking_steps || raw.thinking_steps || undefined

    // Extract insights
    const insights = raw.insights || resultSummary.insights || undefined

    // Extract node history for execution timeline
    const nodeHistory = raw.node_history || raw.execution_timeline || undefined

    // Extract retry count
    const retryCount = raw.repair_attempts || raw.retry_count || undefined

    return {
      id: raw.entry_id || raw.id || raw.query_id || `${timestamp.getTime()}_${idx}`,
      query,
      status,
      timestamp,
      executionTime: typeof execMs === 'number' ? execMs : undefined,
      rowCount: typeof rowCount === 'number' ? rowCount : undefined,
      sql,
      error,
      databaseType,
      columns: Array.isArray(columns) ? columns : undefined,
      thinkingSteps: Array.isArray(thinkingSteps) ? thinkingSteps : undefined,
      insights: Array.isArray(insights) ? insights : undefined,
      nodeHistory: Array.isArray(nodeHistory) ? nodeHistory : undefined,
      retryCount: typeof retryCount === 'number' ? retryCount : undefined,
    }
  })
}
