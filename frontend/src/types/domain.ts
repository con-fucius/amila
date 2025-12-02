export type DatabaseType = 'oracle' | 'doris'

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
}

export type HistoryStatus = 'success' | 'error' | 'pending'
