import { useState } from 'react'
import { Lightbulb, ChevronDown, ChevronUp, Database, Filter, BarChart3, Clock, Target, Layers, Tag } from 'lucide-react'
import { Badge } from './ui/badge'
import { cn } from '@/utils/cn'

interface StructuredIntent {
  query_type: string
  complexity: string
  domain: string
  temporal: string
  expected_cardinality: string
  tables: string[]
  entities: Array<{
    name: string
    type: string
    confidence: number
  }>
  time_period?: string
  aggregations: Array<{
    function: string
    column: string
    alias?: string
  }>
  filters: Array<{
    column: string
    operator: string
    value?: string
  }>
  joins_count: number
  source: string
  confidence: number
  measures: string[]
  dimensions: string[]
}

interface QuestionUnderstandingPanelProps {
  intent: StructuredIntent | null
  userQuery: string
  className?: string
}

const queryTypeLabels: Record<string, { label: string; icon: typeof Database; color: string }> = {
  select: { label: 'Data Retrieval', icon: Database, color: 'text-blue-400' },
  aggregation: { label: 'Aggregation', icon: BarChart3, color: 'text-purple-400' },
  filtered: { label: 'Filtered Query', icon: Filter, color: 'text-amber-400' },
  joined: { label: 'Multi-Table Join', icon: Layers, color: 'text-emerald-400' },
  'time-series': { label: 'Time Series', icon: Clock, color: 'text-cyan-400' },
  ranked: { label: 'Ranked Results', icon: Target, color: 'text-rose-400' },
  comparative: { label: 'Comparison', icon: BarChart3, color: 'text-indigo-400' },
  nested: { label: 'Nested Query', icon: Layers, color: 'text-orange-400' },
}

const complexityColors: Record<string, string> = {
  simple: 'bg-green-500/20 text-green-400 border-green-500/30',
  medium: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
  complex: 'bg-red-500/20 text-red-400 border-red-500/30',
}

const confidenceColors = (confidence: number): string => {
  if (confidence >= 0.8) return 'text-emerald-400'
  if (confidence >= 0.6) return 'text-yellow-400'
  return 'text-red-400'
}

export function QuestionUnderstandingPanel({
  intent,

  className,
}: QuestionUnderstandingPanelProps) {
  const [isExpanded, setIsExpanded] = useState(false)
  const [activeTab, setActiveTab] = useState<'overview' | 'entities' | 'filters'>('overview')

  if (!intent) {
    return (
      <div className={cn('rounded-lg border border-slate-700/50 bg-slate-800/40 p-3', className)}>
        <div className="flex items-center gap-2 text-slate-500">
          <Lightbulb className="h-4 w-4" />
          <span className="text-sm">Understanding query...</span>
        </div>
      </div>
    )
  }

  const queryTypeInfo = queryTypeLabels[intent.query_type] || {
    label: 'Query',
    icon: Database,
    color: 'text-slate-400',
  }
  const QueryTypeIcon = queryTypeInfo.icon

  return (
    <div className={cn('rounded-lg border border-slate-700/50 bg-slate-800/40 overflow-hidden', className)}>
      {/* Header - Always visible */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between p-3 hover:bg-slate-800/60 transition-colors"
      >
        <div className="flex items-center gap-3">
          <div className={cn('p-1.5 rounded-md bg-slate-700/50', queryTypeInfo.color)}>
            <QueryTypeIcon className="h-4 w-4" />
          </div>
          <div className="text-left">
            <div className="text-sm font-medium text-slate-200">
              {queryTypeInfo.label}
            </div>
            <div className="text-xs text-slate-500">
              {intent.tables.length > 0
                ? `From: ${intent.tables.slice(0, 2).join(', ')}${intent.tables.length > 2 ? ` +${intent.tables.length - 2}` : ''
                }`
                : 'Tables not yet identified'}
            </div>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Badge
            variant="outline"
            className={cn(
              'text-xs',
              complexityColors[intent.complexity] || 'bg-slate-500/20 text-slate-400'
            )}
          >
            {intent.complexity}
          </Badge>
          <span
            className={cn(
              'text-xs font-medium',
              confidenceColors(intent.confidence)
            )}
          >
            {Math.round(intent.confidence * 100)}% confidence
          </span>
          {isExpanded ? (
            <ChevronUp className="h-4 w-4 text-slate-500" />
          ) : (
            <ChevronDown className="h-4 w-4 text-slate-500" />
          )}
        </div>
      </button>

      {/* Expanded Content */}
      {isExpanded && (
        <div className="border-t border-slate-700/50">
          {/* Tabs */}
          <div className="flex border-b border-slate-700/50">
            {[
              { id: 'overview', label: 'Overview' },
              { id: 'entities', label: `Entities (${intent.entities.length})` },
              { id: 'filters', label: `Filters (${intent.filters.length})` },
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id as typeof activeTab)}
                className={cn(
                  'px-4 py-2 text-xs font-medium transition-colors',
                  activeTab === tab.id
                    ? 'text-emerald-400 border-b-2 border-emerald-400'
                    : 'text-slate-500 hover:text-slate-300'
                )}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {/* Tab Content */}
          <div className="p-3 space-y-3">
            {activeTab === 'overview' && (
              <>
                {/* Classification Grid */}
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div className="flex justify-between py-1 border-b border-slate-700/30">
                    <span className="text-slate-500">Domain:</span>
                    <span className="text-slate-300 capitalize">{intent.domain}</span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-slate-700/30">
                    <span className="text-slate-500">Temporal:</span>
                    <span className="text-slate-300 capitalize">{intent.temporal.replace('_', ' ')}</span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-slate-700/30">
                    <span className="text-slate-500">Expected Results:</span>
                    <span className="text-slate-300 capitalize">{intent.expected_cardinality}</span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-slate-700/30">
                    <span className="text-slate-500">Joins:</span>
                    <span className="text-slate-300">{intent.joins_count}</span>
                  </div>
                </div>

                {/* Tables */}
                {intent.tables.length > 0 && (
                  <div>
                    <div className="text-xs font-medium text-slate-500 mb-1.5">Tables:</div>
                    <div className="flex flex-wrap gap-1.5">
                      {intent.tables.map((table) => (
                        <Badge
                          key={table}
                          variant="outline"
                          className="text-xs bg-slate-700/30 text-slate-300 border-slate-600/30"
                        >
                          <Database className="h-3 w-3 mr-1" />
                          {table}
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}

                {/* Aggregations */}
                {intent.aggregations.length > 0 && (
                  <div>
                    <div className="text-xs font-medium text-slate-500 mb-1.5">Aggregations:</div>
                    <div className="flex flex-wrap gap-1.5">
                      {intent.aggregations.map((agg, idx) => (
                        <Badge
                          key={idx}
                          variant="outline"
                          className="text-xs bg-purple-500/10 text-purple-300 border-purple-500/30"
                        >
                          <BarChart3 className="h-3 w-3 mr-1" />
                          {agg.function}({agg.column})
                          {agg.alias && ` → ${agg.alias}`}
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}

                {/* Time Period */}
                {intent.time_period && (
                  <div className="flex items-center gap-2 text-xs">
                    <Clock className="h-3.5 w-3.5 text-cyan-400" />
                    <span className="text-slate-500">Time Period:</span>
                    <span className="text-cyan-300">{intent.time_period}</span>
                  </div>
                )}

                {/* Source indicator */}
                <div className="flex items-center gap-2 text-xs pt-2 border-t border-slate-700/30">
                  <Tag className="h-3 w-3 text-slate-500" />
                  <span className="text-slate-500">Classification source:</span>
                  <Badge
                    variant="outline"
                    className={cn(
                      'text-xs',
                      intent.source === 'llm'
                        ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30'
                        : 'bg-amber-500/10 text-amber-400 border-amber-500/30'
                    )}
                  >
                    {intent.source === 'llm' ? 'AI Classified' : 'Rule-based'}
                  </Badge>
                </div>
              </>
            )}

            {activeTab === 'entities' && (
              <div className="space-y-2">
                {intent.entities.length > 0 ? (
                  intent.entities.map((entity, idx) => (
                    <div
                      key={idx}
                      className="flex items-center justify-between py-1.5 px-2 rounded bg-slate-700/30"
                    >
                      <div className="flex items-center gap-2">
                        <span className="text-sm text-slate-200">{entity.name}</span>
                        <Badge variant="outline" className="text-[10px] bg-slate-800/50">
                          {entity.type}
                        </Badge>
                      </div>
                      <div className="flex items-center gap-1">
                        <div className="w-12 h-1.5 bg-slate-700 rounded-full overflow-hidden">
                          <div
                            className={cn(
                              'h-full rounded-full',
                              entity.confidence >= 0.8 ? 'bg-emerald-500' : 'bg-yellow-500'
                            )}
                            style={{ width: `${entity.confidence * 100}%` }}
                          />
                        </div>
                        <span className="text-[10px] text-slate-500">
                          {Math.round(entity.confidence * 100)}%
                        </span>
                      </div>
                    </div>
                  ))
                ) : (
                  <div className="text-sm text-slate-500 text-center py-4">
                    No entities identified yet
                  </div>
                )}
              </div>
            )}

            {activeTab === 'filters' && (
              <div className="space-y-2">
                {intent.filters.length > 0 ? (
                  intent.filters.map((filter, idx) => (
                    <div
                      key={idx}
                      className="flex items-center gap-2 py-1.5 px-2 rounded bg-slate-700/30"
                    >
                      <Filter className="h-3.5 w-3.5 text-amber-400" />
                      <span className="text-sm text-slate-200">{filter.column}</span>
                      <Badge variant="outline" className="text-[10px] bg-slate-800/50">
                        {filter.operator}
                      </Badge>
                      {filter.value && (
                        <span className="text-sm text-amber-300">{filter.value}</span>
                      )}
                    </div>
                  ))
                ) : (
                  <div className="text-sm text-slate-500 text-center py-4">
                    No filters specified
                  </div>
                )}

                {/* Measures */}
                {intent.measures.length > 0 && (
                  <div className="pt-3 border-t border-slate-700/30">
                    <div className="text-xs font-medium text-slate-500 mb-2">Measures:</div>
                    <div className="flex flex-wrap gap-1.5">
                      {intent.measures.map((measure, idx) => (
                        <Badge
                          key={idx}
                          variant="outline"
                          className="text-xs bg-blue-500/10 text-blue-300 border-blue-500/30"
                        >
                          {measure}
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}

                {/* Dimensions */}
                {intent.dimensions.length > 0 && (
                  <div className="pt-2">
                    <div className="text-xs font-medium text-slate-500 mb-2">Dimensions:</div>
                    <div className="flex flex-wrap gap-1.5">
                      {intent.dimensions.map((dim, idx) => (
                        <Badge
                          key={idx}
                          variant="outline"
                          className="text-xs bg-indigo-500/10 text-indigo-300 border-indigo-500/30"
                        >
                          {dim}
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
