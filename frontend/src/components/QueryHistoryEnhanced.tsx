import { useState, useMemo } from 'react'

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
  Clock
} from 'lucide-react'

interface QueryHistoryItem {
  id: string
  query: string
  status: 'success' | 'error' | 'pending'
  timestamp: Date
  executionTime?: number
  rowCount?: number
}

interface QueryHistoryEnhancedProps {
  history: QueryHistoryItem[]
  onRerun: (query: string) => void
  onEditAndRun: (query: string) => void
  className?: string
}

export function QueryHistoryEnhanced({ 
  history, 
  onRerun, 
  onEditAndRun, 
  className = '' 
}: QueryHistoryEnhancedProps) {
  const [searchQuery, setSearchQuery] = useState('')
  const [statusFilter, setStatusFilter] = useState<string>('all')

  const filteredHistory = useMemo(() => {
    return history.filter((item) => {
      const matchesSearch = item.query.toLowerCase().includes(searchQuery.toLowerCase())
      const matchesStatus = statusFilter === 'all' || item.status === statusFilter
      return matchesSearch && matchesStatus
    })
  }, [history, searchQuery, statusFilter])

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'success':
        return <CheckCircle size={14} className="text-green-600" />
      case 'error':
        return <XCircle size={14} className="text-red-600" />
      case 'pending':
        return <Clock size={14} className="text-yellow-600" />
      default:
        return <Clock size={14} className="text-gray-400" />
    }
  }

  const getStatusBadge = (status: string) => {
    const variants = {
      success: 'bg-green-100 text-green-800 border-green-200',
      error: 'bg-red-100 text-red-800 border-red-200',
      pending: 'bg-yellow-100 text-yellow-800 border-yellow-200'
    }
    return variants[status as keyof typeof variants] || 'bg-gray-100 text-gray-800 border-gray-200'
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
    <Card className={className}>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-lg">
          <History size={18} />
          Query History
        </CardTitle>
        
        {/* Search and Filter */}
        <div className="flex gap-2 mt-2">
          <div className="relative flex-1">
            <Search size={16} className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
            <Input
              placeholder="Search queries..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10 text-sm"
            />
          </div>
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="px-3 py-2 border rounded-md text-sm bg-white dark:bg-slate-900 dark:text-gray-100 dark:border-slate-700"
          >
            <option value="all">All Status</option>
            <option value="success">Success</option>
            <option value="error">Error</option>
            <option value="pending">Pending</option>
          </select>
        </div>
      </CardHeader>

      <CardContent className="pt-0">
        {filteredHistory.length === 0 ? (
          <div className="text-center py-8 text-gray-500">
            <History size={48} className="mx-auto mb-2 opacity-50" />
            <p>No queries found</p>
          </div>
        ) : (
          <div className="space-y-2 max-h-96 overflow-y-auto">
            {filteredHistory.slice(0, 20).map((item) => (
              <div
                key={item.id}
                className="group p-3 border rounded-lg hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      {getStatusIcon(item.status)}
                      <Badge 
                        variant="secondary" 
                        className={`text-xs ${getStatusBadge(item.status)}`}
                      >
                        {item.status}
                      </Badge>
                      <span className="text-xs text-gray-500">
                        {formatTimestamp(item.timestamp)}
                      </span>
                      {item.executionTime && (
                        <span className="text-xs text-gray-500">
                          {item.executionTime}ms
                        </span>
                      )}
                      {item.rowCount !== undefined && (
                        <span className="text-xs text-gray-500">
                          {item.rowCount} rows
                        </span>
                      )}
                    </div>
                    
                    <p className="text-sm text-gray-700 truncate group-hover:text-clip group-hover:whitespace-normal">
                      {item.query}
                    </p>
                  </div>

                  {/* Action Buttons */}
                  <div className="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => onRerun(item.query)}
                      className="h-8 w-8 p-0"
                      title="Rerun query"
                    >
                      <Play size={14} />
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => onEditAndRun(item.query)}
                      className="h-8 w-8 p-0"
                      title="Edit and run"
                    >
                      <Edit3 size={14} />
                    </Button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}