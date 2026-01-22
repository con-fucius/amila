import { useState, useMemo, useEffect } from 'react'
import type { MouseEvent as ReactMouseEvent } from 'react'
import { Filter, ChevronDown, ArrowUpDown, ArrowUp, ArrowDown, MoreHorizontal, Check, Clock } from 'lucide-react'
import { ExportButtonsEnhanced } from './ExportButtonsEnhanced'
import { Card, CardContent, CardHeader } from './ui/card'
import { Button } from './ui/button'
import { Badge } from './ui/badge'
import { Input } from './ui/input'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './ui/table'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from './ui/dropdown-menu'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from './ui/tooltip'
import { detectNumericColumnIndexes, paginateRows } from '@/utils/results'
import { findCitedCells, type CellCitation } from '@/utils/citationMatcher'
import { isCellChanged, type CellDiff } from '@/utils/resultDiff'

interface QueryResultsTableProps {
  columns: string[]
  rows: any[]
  executionTime?: number
  rowCount?: number
  sql?: string
  timestamp?: string | Date
  assistantText?: string
  diffData?: CellDiff[]
  onRowAction?: (action: 'filter' | 'drilldown', context: { row: any; rowIndex: number; columns: string[] }) => void
}

export function QueryResultsTable({
  columns,
  rows,
  executionTime,
  rowCount,
  timestamp,
  assistantText,
  diffData,
  onRowAction,
}: QueryResultsTableProps) {
  const [sortColumn, setSortColumn] = useState<string | null>(null)
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc')
  const [filterText, setFilterText] = useState('')
  const [showFilter, setShowFilter] = useState(false)
  const [currentPage, setCurrentPage] = useState(1)
  const [rowsPerPage, setRowsPerPage] = useState(10) // Default 10 rows per page
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({})
  const [resizing, setResizing] = useState<{ column: string; startX: number; startWidth: number } | null>(null)
  const [density, setDensity] = useState<'compact' | 'standard' | 'comfortable'>('standard')

  // Data freshness calculation
  const freshnessInfo = useMemo(() => {
    if (!timestamp) return null
    const queryTime = typeof timestamp === 'string' ? new Date(timestamp) : timestamp
    if (isNaN(queryTime.getTime())) return null

    const now = new Date()
    const diffMs = now.getTime() - queryTime.getTime()
    const diffSec = Math.floor(diffMs / 1000)
    const diffMin = Math.floor(diffSec / 60)
    const diffHour = Math.floor(diffMin / 60)

    let relativeTime: string
    let freshness: 'fresh' | 'recent' | 'stale'

    if (diffSec < 60) {
      relativeTime = 'Just now'
      freshness = 'fresh'
    } else if (diffMin < 5) {
      relativeTime = `${diffMin}m ago`
      freshness = 'fresh'
    } else if (diffMin < 60) {
      relativeTime = `${diffMin}m ago`
      freshness = 'recent'
    } else if (diffHour < 24) {
      relativeTime = `${diffHour}h ago`
      freshness = 'recent'
    } else {
      relativeTime = queryTime.toLocaleDateString()
      freshness = 'stale'
    }

    return {
      relativeTime,
      freshness,
      absoluteTime: queryTime.toLocaleString(),
    }
  }, [timestamp])

  // Normalize columns defensively in case callers accidentally pass
  // rich metadata objects (e.g. { name, type }) instead of strings.
  const normalizedColumns: string[] = useMemo(
    () =>
      Array.isArray(columns)
        ? columns.map((c: any) =>
          typeof c === 'string' ? c : (c?.name != null ? String(c.name) : String(c))
        )
        : [],
    [columns],
  )

  // Helpers for mixed row shapes (array or object)
  const getCellValue = (row: any, column: string, index: number) => {
    if (Array.isArray(row)) return row[index]
    if (row && typeof row === 'object') {
      if (column in row) return (row as any)[column]
      if (index in (row as any)) return (row as any)[index]
    }
    return undefined
  }

  const rowValues = (row: any) => (Array.isArray(row) ? row : Object.values(row ?? {}))

  // Find cited cells based on assistant's text
  const citedCells = useMemo<CellCitation[]>(() => {
    if (!assistantText || !rows || rows.length === 0) return []

    // Convert rows to array format for citation matcher
    const arrayRows = rows.map(row => {
      if (Array.isArray(row)) return row
      return normalizedColumns.map((col, idx) => getCellValue(row, col, idx))
    })

    return findCitedCells(assistantText, normalizedColumns, arrayRows)
  }, [assistantText, rows, normalizedColumns])

  const numericColumnSet = useMemo(() => {
    const set = new Set<string>()
    if (!Array.isArray(normalizedColumns) || !rows || rows.length === 0) return set

    const numericIndexes = detectNumericColumnIndexes(
      normalizedColumns,
      rows,
      getCellValue,
      50,
    )
    numericIndexes.forEach((idx) => {
      const col = normalizedColumns[idx]
      if (col != null) set.add(col)
    })

    return set
  }, [normalizedColumns, rows])

  const formatValue = (val: any) => {
    if (val === null || val === undefined) return <span className="text-gray-400 dark:text-gray-500">NULL</span>
    if (typeof val === 'number') {
      return new Intl.NumberFormat(undefined, { maximumFractionDigits: 6 }).format(val)
    }
    const str = String(val)
    // Detect numeric strings
    if (/^-?\d{1,3}(,?\d{3})*(\.\d+)?$/.test(str) || /^-?\d+(\.\d+)?$/.test(str)) {
      const num = Number(str.replace(/,/g, ''))
      if (!isNaN(num)) return new Intl.NumberFormat(undefined, { maximumFractionDigits: 6 }).format(num)
    }
    // Detect ISO-like dates
    const parsed = Date.parse(str)
    if (!isNaN(parsed) && /\d{4}-\d{2}-\d{2}/.test(str)) {
      const d = new Date(parsed)
      return d.toLocaleString(undefined, { year: 'numeric', month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' })
    }
    return str
  }

  // Filtered rows
  const filteredRows = useMemo(() => {
    if (!filterText) return rows
    return rows.filter((row) =>
      rowValues(row).some((val) =>
        String(val).toLowerCase().includes(filterText.toLowerCase())
      )
    )
  }, [rows, filterText])

  // Sorted rows
  const sortedRows = useMemo(() => {
    if (!sortColumn) return filteredRows
    const colIndex = normalizedColumns.indexOf(sortColumn)
    if (colIndex === -1) return filteredRows

    return [...filteredRows].sort((a, b) => {
      const aVal = getCellValue(a, sortColumn, colIndex)
      const bVal = getCellValue(b, sortColumn, colIndex)

      // Handle null/undefined
      if (aVal == null && bVal == null) return 0
      if (aVal == null) return 1
      if (bVal == null) return -1

      // Numeric comparison
      const aNum = Number(aVal)
      const bNum = Number(bVal)
      if (!isNaN(aNum) && !isNaN(bNum)) {
        return sortDirection === 'asc' ? aNum - bNum : bNum - aNum
      }

      // String/date comparison
      const aStr = String(aVal).toLowerCase()
      const bStr = String(bVal).toLowerCase()
      if (aStr < bStr) return sortDirection === 'asc' ? -1 : 1
      if (aStr > bStr) return sortDirection === 'asc' ? 1 : -1
      return 0
    })
  }, [filteredRows, sortColumn, sortDirection, normalizedColumns])

  // Pagination calculations (1-based currentPage)
  const totalPages = Math.max(1, Math.ceil(sortedRows.length / rowsPerPage))
  const { slice: paginatedRows, start: startIndex } = useMemo(
    () => paginateRows(sortedRows, currentPage - 1, rowsPerPage),
    [sortedRows, currentPage, rowsPerPage],
  )

  // Reset to page 1 when filter or sort changes
  useMemo(() => {
    setCurrentPage(1)
  }, [filterText, sortColumn, sortDirection])

  const handlePageChange = (newPage: number) => {
    if (newPage >= 1 && newPage <= totalPages) {
      setCurrentPage(newPage)
    }
  }

  const handleResizeMouseDown = (event: ReactMouseEvent<HTMLDivElement>, column: string) => {
    event.preventDefault()
    event.stopPropagation()
    const headerEl = event.currentTarget.parentElement as HTMLElement | null
    const startWidth = columnWidths[column] || (headerEl ? headerEl.offsetWidth : 160)
    setResizing({ column, startX: event.clientX, startWidth })
  }

  useEffect(() => {
    if (!resizing) return

    const handleMouseMove = (event: MouseEvent) => {
      const delta = event.clientX - resizing.startX
      let nextWidth = resizing.startWidth + delta
      if (nextWidth < 80) nextWidth = 80
      if (nextWidth > 600) nextWidth = 600
      setColumnWidths((prev) => ({ ...prev, [resizing.column]: nextWidth }))
    }

    const handleMouseUp = () => {
      window.removeEventListener('mousemove', handleMouseMove)
      window.removeEventListener('mouseup', handleMouseUp)
      setResizing(null)
    }

    window.addEventListener('mousemove', handleMouseMove)
    window.addEventListener('mouseup', handleMouseUp)

    return () => {
      window.removeEventListener('mousemove', handleMouseMove)
      window.removeEventListener('mouseup', handleMouseUp)
    }
  }, [resizing])

  const handleRowsPerPageChange = (newRowsPerPage: number) => {
    setRowsPerPage(newRowsPerPage)
    setCurrentPage(1) // Reset to first page
  }

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc')
    } else {
      setSortColumn(column)
      setSortDirection('asc')
    }
  }



  return (
    <Card className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="text-base font-semibold">Query Results</div>
            <Badge variant="secondary" className="bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300">
              {rowCount || rows.length} rows
            </Badge>
            {executionTime !== undefined && (
              <Badge variant="outline" className="text-[11px]">
                {executionTime.toFixed(2)}s
              </Badge>
            )}
            {/* Citation indicator */}
            {citedCells.length > 0 && (
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Badge
                      variant="outline"
                      className="text-[11px] flex items-center gap-1 text-yellow-700 border-yellow-300 bg-yellow-50 dark:text-yellow-400 dark:border-yellow-700 dark:bg-yellow-950/30"
                    >
                      {citedCells.length} {citedCells.length === 1 ? 'citation' : 'citations'}
                    </Badge>
                  </TooltipTrigger>
                  <TooltipContent>
                    <p>Highlighted cells are referenced in the assistant's response</p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            )}
            {/* Data Freshness Indicator */}
            {freshnessInfo && (
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Badge
                      variant="outline"
                      className={`text-[11px] flex items-center gap-1 ${freshnessInfo.freshness === 'fresh'
                        ? 'text-green-600 border-green-200 bg-green-50 dark:text-green-400 dark:border-green-800 dark:bg-green-950/30'
                        : freshnessInfo.freshness === 'recent'
                          ? 'text-blue-600 border-blue-200 bg-blue-50 dark:text-blue-400 dark:border-blue-800 dark:bg-blue-950/30'
                          : 'text-gray-500 border-gray-200 bg-gray-50 dark:text-gray-400 dark:border-gray-700 dark:bg-gray-900/30'
                        }`}
                    >
                      <Clock className="h-3 w-3" />
                      {freshnessInfo.relativeTime}
                    </Badge>
                  </TooltipTrigger>
                  <TooltipContent>
                    <p>Query executed: {freshnessInfo.absoluteTime}</p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            )}
          </div>
          <div className="flex gap-2">
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm" className="h-8">
                  <span className="text-xs mr-1">Density</span>
                  <ChevronDown className="h-3 w-3" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onClick={() => setDensity('compact')}>
                  Compact {density === 'compact' && <Check className="ml-2 h-3 w-3" />}
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => setDensity('standard')}>
                  Standard {density === 'standard' && <Check className="ml-2 h-3 w-3" />}
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => setDensity('comfortable')}>
                  Comfortable {density === 'comfortable' && <Check className="ml-2 h-3 w-3" />}
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>

            <Button
              variant="outline"
              size="sm"
              onClick={() => setShowFilter(!showFilter)}
              className={showFilter ? 'bg-gray-100' : ''}
            >
              <Filter className="h-4 w-4 mr-1" />
              Filter
            </Button>
            <ExportButtonsEnhanced
              data={{
                columns,
                rows: sortedRows,
                metadata: {
                  timestamp: new Date().toISOString(),
                  row_count: sortedRows.length
                }
              }}
              filename="query_results"
            />
          </div>
        </div>
        {showFilter && (
          <div className="mt-3">
            <Input
              placeholder="Filter results..."
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              className="max-w-sm text-gray-900 dark:text-gray-100"
            />
            {filterText && (
              <div className="text-xs text-gray-600 dark:text-gray-300 mt-1">
                Showing {sortedRows.length} of {rows.length} rows
              </div>
            )}
          </div>
        )}
      </CardHeader>
      <CardContent>
        <div className="border rounded-lg max-h-[420px] overflow-x-auto overflow-y-auto">
          <Table className="table-auto min-w-max">
            <TableHeader>
              <TableRow className="bg-gray-50 dark:bg-slate-900/80 sticky top-0 z-10 shadow-sm border-b border-gray-200 dark:border-slate-700">
                {normalizedColumns.map((column) => {
                  const isNumeric = numericColumnSet.has(column)
                  const headAlign = isNumeric ? 'text-right' : 'text-left'
                  const paddingClass = density === 'compact' ? 'py-1' : density === 'comfortable' ? 'py-3' : 'py-2'
                  return (
                    <TableHead
                      key={column}
                      className={`relative font-semibold cursor-pointer hover:bg-gray-100 ${paddingClass} text-xs sm:text-[13px] whitespace-normal break-words leading-tight ${headAlign}`}
                      onClick={() => handleSort(column)}
                      style={{ width: columnWidths[column] ?? 160 }}
                    >
                      <div className={`flex items-center gap-1 ${isNumeric ? 'justify-end' : ''}`}>
                        <span className="break-words">{column}</span>
                        {sortColumn === column ? (
                          sortDirection === 'asc' ? (
                            <ArrowUp className="h-3 w-3" />
                          ) : (
                            <ArrowDown className="h-3 w-3" />
                          )
                        ) : (
                          <ArrowUpDown className="h-3 w-3 opacity-30" />
                        )}
                      </div>
                      <div
                        className="absolute top-0 right-0 h-full w-1 cursor-col-resize select-none"
                        onMouseDown={(event) => handleResizeMouseDown(event, column)}
                      />
                    </TableHead>
                  )
                })}
              </TableRow>
            </TableHeader>
            <TableBody>
              {paginatedRows.length === 0 ? (
                <TableRow>
                  <TableCell
                    colSpan={normalizedColumns.length}
                    className="h-24 text-center text-gray-500 dark:text-gray-400"
                  >
                    {filterText ? 'No results match your filter' : 'No results found'}
                  </TableCell>
                </TableRow>
              ) : (
                paginatedRows.map((row, idx) => (
                  <TableRow
                    key={startIndex + idx}
                    className="hover:bg-gray-50 dark:hover:bg-slate-900/60 text-[13px] group"
                  >
                    {normalizedColumns.map((column, colIdx) => {
                      const isNumeric = numericColumnSet.has(column)
                      const alignClass = isNumeric ? 'text-right font-mono tabular-nums' : 'text-left'
                      const paddingClass = density === 'compact' ? 'py-1 px-2' : density === 'comfortable' ? 'py-3 px-3' : 'py-1.5 px-2'

                      // Check if this cell is cited in the assistant's response
                      const citation = citedCells.find(c => c.rowIndex === startIndex + idx && c.columnIndex === colIdx)
                      const isCited = !!citation

                      // Check if this cell has changed (diff view)
                      const cellDiff = diffData ? isCellChanged(startIndex + idx, colIdx, diffData) : undefined

                      // Determine cell styling based on citation and diff
                      let cellClass = ''
                      if (cellDiff) {
                        // Diff highlighting takes precedence
                        if (cellDiff.changeType === 'added') {
                          cellClass = 'bg-green-100 dark:bg-green-900/30 border-l-2 border-green-500 dark:border-green-600'
                        } else if (cellDiff.changeType === 'removed') {
                          cellClass = 'bg-red-100 dark:bg-red-900/30 border-l-2 border-red-500 dark:border-red-600'
                        } else if (cellDiff.changeType === 'modified') {
                          cellClass = 'bg-blue-100 dark:bg-blue-900/30 border-l-2 border-blue-500 dark:border-blue-600'
                        }
                      } else if (isCited) {
                        cellClass = 'bg-yellow-100 dark:bg-yellow-900/30 border-l-2 border-yellow-400 dark:border-yellow-600'
                      }

                      const cellContent = (
                        <TableCell
                          key={`${startIndex + idx}-${column}`}
                          className={`whitespace-nowrap overflow-hidden text-ellipsis ${paddingClass} ${alignClass} ${cellClass}`}
                          style={{ width: columnWidths[column] ?? 160 }}
                        >
                          <div className="flex items-center justify-between gap-2">
                            <div className="flex-1 overflow-hidden text-ellipsis">
                              {formatValue(getCellValue(row, column, colIdx))}
                              {cellDiff && cellDiff.changeType === 'modified' && (
                                <div className="text-[10px] text-gray-500 dark:text-gray-400 italic">
                                  was: {formatValue(cellDiff.oldValue)}
                                </div>
                              )}
                            </div>
                            {colIdx === 0 && onRowAction && (
                              <DropdownMenu>
                                <DropdownMenuTrigger asChild>
                                  <Button
                                    variant="ghost"
                                    size="icon"
                                    className="h-6 w-6 p-0 opacity-0 group-hover:opacity-100 transition-opacity"
                                    onClick={(e) => e.stopPropagation()}
                                  >
                                    <MoreHorizontal className="h-3 w-3" />
                                  </Button>
                                </DropdownMenuTrigger>
                                <DropdownMenuContent align="end">
                                  <DropdownMenuItem
                                    onClick={(e) => {
                                      e.stopPropagation()
                                      onRowAction('filter', {
                                        row,
                                        rowIndex: startIndex + idx,
                                        columns: normalizedColumns,
                                      })
                                    }}
                                  >
                                    Filter by this value
                                  </DropdownMenuItem>
                                  <DropdownMenuItem
                                    onClick={(e) => {
                                      e.stopPropagation()
                                      onRowAction('drilldown', {
                                        row,
                                        rowIndex: startIndex + idx,
                                        columns: normalizedColumns,
                                      })
                                    }}
                                  >
                                    Drill down
                                  </DropdownMenuItem>
                                </DropdownMenuContent>
                              </DropdownMenu>
                            )}
                          </div>
                        </TableCell>
                      )

                      // Wrap cited cells with tooltip
                      if (isCited && citation) {
                        return (
                          <TooltipProvider key={`${startIndex + idx}-${column}`}>
                            <Tooltip>
                              <TooltipTrigger asChild>
                                {cellContent}
                              </TooltipTrigger>
                              <TooltipContent>
                                <p className="text-xs">Referenced in response: "{citation.matchedText}"</p>
                              </TooltipContent>
                            </Tooltip>
                          </TooltipProvider>
                        )
                      }

                      return cellContent
                    })}
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>

        {sortedRows.length > 0 && (
          <div className="flex items-center justify-between mt-4 text-sm">
            <div className="flex items-center gap-4">
              <div className="text-gray-600 dark:text-gray-300">
                {filterText && sortedRows.length !== rows.length && (
                  <div>
                    Filtered from {rows.length} {rows.length === 1 ? 'row' : 'rows'}
                  </div>
                )}
                {typeof rowCount === 'number' && rowCount > rows.length && (
                  <div className="mt-0.5 text-[11px] text-gray-500 dark:text-gray-400">
                    Backend reports {rowCount.toLocaleString()} total rows; displaying {rows.length.toLocaleString()} loaded rows.
                  </div>
                )}
              </div>
              <div className="flex items-center gap-2">
                <span className="text-gray-600 dark:text-gray-300">Rows per page:</span>
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button variant="outline" size="sm" className="h-8 w-16">
                      {rowsPerPage}
                      <ChevronDown className="h-3 w-3 ml-1" />
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="start">
                    {[5, 10, 20, 50, 100].map((size) => (
                      <DropdownMenuItem
                        key={size}
                        onClick={() => handleRowsPerPageChange(size)}
                        className={rowsPerPage === size ? 'bg-gray-100' : ''}
                      >
                        {size}
                      </DropdownMenuItem>
                    ))}
                  </DropdownMenuContent>
                </DropdownMenu>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => handlePageChange(currentPage - 1)}
                disabled={currentPage === 1}
              >
                Previous
              </Button>
              <div className="flex items-center gap-1">
                {/* Show page numbers */}
                {totalPages <= 7 ? (
                  // Show all pages if 7 or fewer
                  Array.from({ length: totalPages }, (_, i) => i + 1).map((page) => (
                    <Button
                      key={page}
                      variant={currentPage === page ? 'default' : 'outline'}
                      size="sm"
                      onClick={() => handlePageChange(page)}
                      className="w-8 h-8 p-0"
                    >
                      {page}
                    </Button>
                  ))
                ) : (
                  // Show ellipsis for many pages
                  <>
                    {currentPage > 3 && (
                      <>
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handlePageChange(1)}
                          className="w-8 h-8 p-0"
                        >
                          1
                        </Button>
                        {currentPage > 4 && <span className="px-1">...</span>}
                      </>
                    )}
                    {Array.from({ length: 5 }, (_, i) => {
                      const page = currentPage - 2 + i
                      if (page < 1 || page > totalPages) return null
                      return (
                        <Button
                          key={page}
                          variant={currentPage === page ? 'default' : 'outline'}
                          size="sm"
                          onClick={() => handlePageChange(page)}
                          className="w-8 h-8 p-0"
                        >
                          {page}
                        </Button>
                      )
                    })}
                    {currentPage < totalPages - 2 && (
                      <>
                        {currentPage < totalPages - 3 && <span className="px-1">...</span>}
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handlePageChange(totalPages)}
                          className="w-8 h-8 p-0"
                        >
                          {totalPages}
                        </Button>
                      </>
                    )}
                  </>
                )}
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={() => handlePageChange(currentPage + 1)}
                disabled={currentPage === totalPages}
              >
                Next
              </Button>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
