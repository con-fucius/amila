import type { QueryResult } from '@/types/domain'

export function normalizeColumns(columns: any): string[] {
  if (!Array.isArray(columns)) return []
  return columns.map((c: any) =>
    typeof c === 'string' ? c : (c?.name != null ? String(c.name) : String(c))
  )
}

interface BackendResultLike {
  columns?: any
  rows?: any
  row_count?: number
  execution_time_ms?: number
}

export function normalizeBackendResult(result: BackendResultLike | null | undefined): QueryResult {
  if (!result) {
    return { columns: [], rows: [], rowCount: 0, executionTime: 0 }
  }
  const columns = normalizeColumns(result.columns || [])
  const rows = Array.isArray(result.rows) ? result.rows : []
  const rowCount = typeof result.row_count === 'number' ? result.row_count : rows.length
  const executionTime = typeof result.execution_time_ms === 'number'
    ? result.execution_time_ms / 1000
    : undefined
  return { columns, rows, rowCount, executionTime }
}

export type CellAccessor = (row: any, column: string, index: number) => any

/**
 * Detect numeric columns by sampling rows and using header-name heuristics.
 */
export function detectNumericColumnIndexes(
  columns: string[],
  rows: any[],
  getCell: CellAccessor,
  maxSample: number = 50,
): number[] {
  if (!Array.isArray(columns) || !Array.isArray(rows) || rows.length === 0) return []

  const numeric: number[] = []
  const sampleRows = rows.slice(0, maxSample)

  columns.forEach((col, colIndex) => {
    let hasValue = false
    let allNumeric = true

    for (const row of sampleRows) {
      const v = getCell(row, col, colIndex)
      if (v === null || v === undefined || v === '') continue
      hasValue = true
      const raw = typeof v === 'string' ? v.replace(/,/g, '') : v
      const n = typeof raw === 'number' ? raw : Number(raw)
      if (!Number.isFinite(n)) {
        allNumeric = false
        break
      }
    }

    if (!hasValue) return

    if (allNumeric) {
      numeric.push(colIndex)
      return
    }

    const upper = String(col).toUpperCase()
    if (/(AMOUNT|BALANCE|REVENUE|RESOURCE|RESOURCES|COUNT|TOTAL|CREDIT|DEBIT|MARGIN|RATE|PCT|PERCENT)/.test(upper)) {
      numeric.push(colIndex)
    }
  })

  return numeric
}

export function paginateRows<T>(rows: T[], page: number, rowsPerPage: number): { slice: T[]; start: number; end: number } {
  const start = page * rowsPerPage
  const end = start + rowsPerPage
  return { slice: rows.slice(start, end), start, end }
}
