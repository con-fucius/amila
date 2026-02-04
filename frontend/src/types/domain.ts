export type DatabaseType = 'oracle' | 'doris' | 'postgres' | 'qlik' | 'superset'

export type ToolCallStatus = 'pending' | 'approved' | 'rejected' | 'completed' | 'error'

export interface ThinkingStep {
  id?: string
  name?: string
  stage?: string
  content?: string
  status?: 'pending' | 'in-progress' | 'completed' | 'failed'
  error?: string
  timestamp?: string
  details?: string
}

export interface TodoItem {
  id: string
  title: string
  status: 'pending' | 'in-progress' | 'completed' | 'failed'
  details?: string
}

export interface QueryResult {
  columns: string[]
  rows: any[]
  rowCount?: number
  executionTime?: number
  resultRef?: ResultReference
  resultsTruncated?: boolean
}

export interface ResultReference {
  queryId: string
  rowCount: number
  columns: string[]
  cacheStatus?: string
}

export type HistoryStatus = 'success' | 'error' | 'pending'
