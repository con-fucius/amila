import { useState, useMemo, useCallback } from 'react'

import { Card, CardContent, CardHeader, CardTitle } from './ui/card'
import { Button } from './ui/button'
import { Input } from './ui/input'
import { Badge } from './ui/badge'
import {
  Search,
  History,
  Play,
  Edit3,
  CheckCircle,
  XCircle,
  Clock,
  Download,
  Copy,
  ChevronDown,
  ChevronUp,
  AlertTriangle,
  RefreshCw,
  FileText,
  Code,
  Timer,
  Rows3
} from 'lucide-react'

export interface QueryHistoryItem {
  id: string
  query: string
  status: 'success' | 'error' | 'pending' | 'rejected'
  timestamp: Date
  executionTime?: number
  rowCount?: number
  sql?: string
  error?: string
  retryCount?: number
  databaseType?: 'oracle' | 'doris' | 'postgres'
  columns?: string[]
  thinkingSteps?: string[]
  insights?: string[]
  nodeHistory?: Array<{ name: string; duration?: number; status?: string }>
}

interface QueryHistoryEnhancedProps {
  history: QueryHistoryItem[]
  onRerun: (query: string) => void
  onEditAndRun: (query: string) => void
  onClose?: () => void
  className?: string
  compact?: boolean
}

export function QueryHistoryEnhanced({
  history,
  onRerun,
  onEditAndRun,
  onClose,
  className = '',
  compact = false
}: QueryHistoryEnhancedProps) {
  const [searchQuery, setSearchQuery] = useState('')
  const [statusFilter, setStatusFilter] = useState<string>('all')
  const [expandedId, setExpandedId] = useState<string | null>(null)
  const [dbFilter, setDbFilter] = useState<string>('all')

  const filteredHistory = useMemo(() => {
    return history.filter((item) => {
      const matchesSearch = item.query.toLowerCase().includes(searchQuery.toLowerCase()) ||
        (item.sql && item.sql.toLowerCase().includes(searchQuery.toLowerCase()))
      const matchesStatus = statusFilter === 'all' || item.status === statusFilter
      const matchesDb = dbFilter === 'all' || item.databaseType === dbFilter
      return matchesSearch && matchesStatus && matchesDb
    })
  }, [history, searchQuery, statusFilter, dbFilter])

  const stats = useMemo(() => {
    const total = history.length
    const successful = history.filter(h => h.status === 'success').length
    const failed = history.filter(h => h.status === 'error').length
    const avgTime = history.filter(h => h.executionTime).reduce((acc, h) => acc + (h.executionTime || 0), 0) / (history.filter(h => h.executionTime).length || 1)
    return { total, successful, failed, avgTime: Math.round(avgTime) }
  }, [history])

  const handleCopySQL = useCallback((sql: string) => {
    navigator.clipboard.writeText(sql)
  }, [])

  const handleExportHistory = useCallback(() => {
    const exportData = filteredHistory.map(item => ({
      timestamp: item.timestamp.toISOString(),
      query: item.query,
      sql: item.sql || '',
      status: item.status,
      executionTime: item.executionTime,
      rowCount: item.rowCount,
      database: item.databaseType,
      error: item.error || '',
      columns: item.columns?.join(', ') || ''
    }))
    const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `query-history-${new Date().toISOString().split('T')[0]}.json`
    a.click()
    URL.revokeObjectURL(url)
  }, [filteredHistory])

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'success':
        return <CheckCircle size={12} className="text-green-600" />
      case 'error':
        return <XCircle size={12} className="text-red-600" />
      case 'pending':
        return <Clock size={12} className="text-yellow-600" />
      case 'rejected':
        return <AlertTriangle size={12} className="text-orange-600" />
      default:
        return <Clock size={12} className="text-gray-400" />
    }
  }

  const getStatusBadge = (status: string) => {
    const variants = {
      success: 'bg-green-100 text-green-800 border-green-200 dark:bg-green-900/30 dark:text-green-400 dark:border-green-800',
      error: 'bg-red-100 text-red-800 border-red-200 dark:bg-red-900/30 dark:text-red-400 dark:border-red-800',
      pending: 'bg-yellow-100 text-yellow-800 border-yellow-200 dark:bg-yellow-900/30 dark:text-yellow-400 dark:border-yellow-800',
      rejected: 'bg-orange-100 text-orange-800 border-orange-200 dark:bg-orange-900/30 dark:text-orange-400 dark:border-orange-800'
    }
    return variants[status as keyof typeof variants] || 'bg-gray-100 text-gray-800 border-gray-200 dark:bg-gray-800 dark:text-gray-400'
  }

  const getDbBadge = (db?: string) => {
    if (db === 'oracle') return 'bg-red-50 text-red-700 border-red-200 dark:bg-red-900/20 dark:text-red-400'
    if (db === 'doris') return 'bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-900/20 dark:text-blue-400'
    if (db === 'postgres') return 'bg-indigo-50 text-indigo-700 border-indigo-200 dark:bg-indigo-900/20 dark:text-indigo-400'
    return 'bg-gray-50 text-gray-600 border-gray-200 dark:bg-gray-800 dark:text-gray-400'
  }

  const formatTimestamp = (timestamp: Date) => {
    const now = new Date()
    const diff = now.getTime() - timestamp.getTime()
    const minutes = Math.floor(diff / 60000)
    const hours = Math.floor(diff / 3600000)
    const days = Math.floor(diff / 86400000)

    if (minutes < 1) return 'Just now'
    if (minutes < 60) return `${minutes}m ago`
    if (hours < 24) return `${hours}h ago`
    return `${days}d ago`
  }

  return (
    <Card className={`${className} flex flex-col`}>
      <CardHeader className="pb-2 flex-shrink-0">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2 text-base">
            <History size={16} />
            Query Audit Log
          </CardTitle>
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              variant="outline"
              onClick={handleExportHistory}
              className="h-7 text-xs gap-1"
              title="Export history as JSON"
            >
              <Download size={12} />
              Export
            </Button>
            {onClose && (
              <Button size="sm" variant="ghost" onClick={onClose} className="h-7 w-7 p-0">
                <XCircle size={14} />
              </Button>
            )}
          </div>
        </div>

        {/* Stats Summary */}
        {!compact && (
          <div className="flex gap-3 mt-2 text-[10px] text-gray-500 dark:text-gray-400">
            <span className="flex items-center gap-1">
              <FileText size={10} />
              {stats.total} queries
            </span>
            <span className="flex items-center gap-1 text-green-600">
              <CheckCircle size={10} />
              {stats.successful} success
            </span>
            <span className="flex items-center gap-1 text-red-600">
              <XCircle size={10} />
              {stats.failed} failed
            </span>
            <span className="flex items-center gap-1">
              <Timer size={10} />
              ~{stats.avgTime}ms avg
            </span>
          </div>
        )}

        {/* Search and Filters */}
        <div className="flex gap-2 mt-2 flex-wrap">
          <div className="relative flex-1 min-w-[150px]">
            <Search size={14} className="absolute left-2.5 top-1/2 transform -translate-y-1/2 text-gray-400" />
            <Input
              placeholder="Search queries or SQL..."
              value={searchQuery}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSearchQuery(e.target.value)}
              className="pl-8 h-8 text-xs"
            />
          </div>
          <select
            value={statusFilter}
            onChange={(e: React.ChangeEvent<HTMLSelectElement>) => setStatusFilter(e.target.value)}
            className="px-2 py-1 border rounded-md text-xs bg-white dark:bg-slate-900 dark:text-gray-100 dark:border-slate-700 h-8"
          >
            <option value="all">All Status</option>
            <option value="success">Success</option>
            <option value="error">Error</option>
            <option value="pending">Pending</option>
            <option value="rejected">Rejected</option>
          </select>
          <select
            value={dbFilter}
            onChange={(e: React.ChangeEvent<HTMLSelectElement>) => setDbFilter(e.target.value)}
            className="px-2 py-1 border rounded-md text-xs bg-white dark:bg-slate-900 dark:text-gray-100 dark:border-slate-700 h-8"
          >
            <option value="all">All DBs</option>
            <option value="oracle">Oracle</option>
            <option value="doris">Doris</option>
            <option value="postgres">Postgres</option>
          </select>
        </div>
      </CardHeader>

      <CardContent className="pt-0 flex-1 overflow-hidden">
        {filteredHistory.length === 0 ? (
          <div className="text-center py-8 text-gray-500">
            <History size={40} className="mx-auto mb-2 opacity-40" />
            <p className="text-sm">No queries found</p>
            <p className="text-xs text-gray-400 mt-1">Try adjusting your filters</p>
          </div>
        ) : (
          <div className="space-y-1.5 max-h-[60vh] overflow-y-auto pr-1">
            {filteredHistory.slice(0, 50).map((item) => {
              const isExpanded = expandedId === item.id
              return (
                <div
                  key={item.id}
                  className={`group border rounded-lg transition-all duration-200 ${isExpanded
                    ? 'bg-gray-50 dark:bg-slate-800/50 border-emerald-200 dark:border-emerald-800'
                    : 'hover:bg-gray-50 dark:hover:bg-slate-800/30'
                    }`}
                >
                  {/* Main Row */}
                  <div
                    className="p-2 cursor-pointer"
                    onClick={() => setExpandedId(isExpanded ? null : item.id)}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-1.5 mb-1 flex-wrap">
                          {getStatusIcon(item.status)}
                          <Badge variant="secondary" className={`text-[9px] px-1.5 py-0 ${getStatusBadge(item.status)}`}>
                            {item.status}
                          </Badge>
                          {item.databaseType && (
                            <Badge variant="outline" className={`text-[9px] px-1.5 py-0 ${getDbBadge(item.databaseType)}`}>
                              {item.databaseType}
                            </Badge>
                          )}
                          <span className="text-[10px] text-gray-400">
                            {formatTimestamp(item.timestamp)}
                          </span>
                          {item.executionTime !== undefined && (
                            <span className="text-[10px] text-gray-500 flex items-center gap-0.5">
                              <Timer size={9} />
                              {item.executionTime}ms
                            </span>
                          )}
                          {item.rowCount !== undefined && (
                            <span className="text-[10px] text-gray-500 flex items-center gap-0.5">
                              <Rows3 size={9} />
                              {item.rowCount.toLocaleString()}
                            </span>
                          )}
                          {item.retryCount && item.retryCount > 0 && (
                            <span className="text-[10px] text-amber-600 flex items-center gap-0.5">
                              <RefreshCw size={9} />
                              {item.retryCount}
                            </span>
                          )}
                        </div>

                        <p className="text-xs text-gray-700 dark:text-gray-300 line-clamp-2">
                          {item.query}
                        </p>
                      </div>

                      {/* Expand/Collapse & Actions */}
                      <div className="flex items-center gap-1 flex-shrink-0">
                        <div className="flex gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                          <Button
                            size="sm"
                            variant="ghost"
                            onClick={(e) => { e.stopPropagation(); onRerun(item.query); }}
                            className="h-6 w-6 p-0"
                            title="Rerun query"
                          >
                            <Play size={12} />
                          </Button>
                          <Button
                            size="sm"
                            variant="ghost"
                            onClick={(e) => { e.stopPropagation(); onEditAndRun(item.query); }}
                            className="h-6 w-6 p-0"
                            title="Edit and run"
                          >
                            <Edit3 size={12} />
                          </Button>
                        </div>
                        {isExpanded ? <ChevronUp size={14} className="text-gray-400" /> : <ChevronDown size={14} className="text-gray-400" />}
                      </div>
                    </div>
                  </div>

                  {/* Expanded Details */}
                  {isExpanded && (
                    <div className="px-2 pb-2 pt-0 border-t border-gray-100 dark:border-slate-700 space-y-2">
                      {/* Generated SQL */}
                      {item.sql && (
                        <div>
                          <div className="flex items-center justify-between mb-1">
                            <span className="text-[10px] font-medium text-gray-500 flex items-center gap-1">
                              <Code size={10} />
                              Generated SQL
                            </span>
                            <Button
                              size="sm"
                              variant="ghost"
                              onClick={() => handleCopySQL(item.sql!)}
                              className="h-5 px-1.5 text-[10px] gap-1"
                            >
                              <Copy size={10} />
                              Copy
                            </Button>
                          </div>
                          <pre className="text-[10px] bg-gray-900 text-gray-100 p-2 rounded overflow-x-auto max-h-32">
                            {item.sql}
                          </pre>
                        </div>
                      )}

                      {/* Error Details */}
                      {item.error && (
                        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded p-2">
                          <span className="text-[10px] font-medium text-red-700 dark:text-red-400 flex items-center gap-1 mb-1">
                            <AlertTriangle size={10} />
                            Error Details
                          </span>
                          <p className="text-[10px] text-red-600 dark:text-red-300">{item.error}</p>
                        </div>
                      )}

                      {/* Columns */}
                      {item.columns && item.columns.length > 0 && (
                        <div>
                          <span className="text-[10px] font-medium text-gray-500 mb-1 block">Result Columns</span>
                          <div className="flex flex-wrap gap-1">
                            {item.columns.slice(0, 10).map((col, i) => (
                              <Badge key={i} variant="outline" className="text-[9px] px-1 py-0">
                                {col}
                              </Badge>
                            ))}
                            {item.columns.length > 10 && (
                              <Badge variant="outline" className="text-[9px] px-1 py-0 text-gray-400">
                                +{item.columns.length - 10} more
                              </Badge>
                            )}
                          </div>
                        </div>
                      )}

                      {/* Execution Timeline */}
                      {item.nodeHistory && item.nodeHistory.length > 0 && (
                        <div>
                          <span className="text-[10px] font-medium text-gray-500 mb-1 block">Execution Timeline</span>
                          <div className="flex gap-1 overflow-x-auto pb-1">
                            {item.nodeHistory.map((node, i) => (
                              <div
                                key={i}
                                className={`flex-shrink-0 px-1.5 py-0.5 rounded text-[9px] ${node.status === 'error'
                                  ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400'
                                  : 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400'
                                  }`}
                              >
                                {node.name}
                                {node.duration && <span className="ml-1 opacity-70">{node.duration}ms</span>}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Full Timestamp */}
                      <div className="text-[9px] text-gray-400 pt-1 border-t border-gray-100 dark:border-slate-700">
                        {item.timestamp.toLocaleString()}
                      </div>
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        )}
      </CardContent>
    </Card>
  )
}