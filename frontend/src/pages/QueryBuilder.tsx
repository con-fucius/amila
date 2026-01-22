import { useEffect, useState } from 'react'
import { Play, Database } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent } from '@/components/ui/card'
import { MonacoSQLEditor } from '@/components/MonacoSQLEditor'
import { QueryResultsTable } from '@/components/QueryResultsTable'
import { QueryHistoryEnhanced } from '@/components/QueryHistoryEnhanced'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { apiService } from '@/services/apiService'
import { useQueryHistory } from '@/hooks/useQueryHistory'
import { normalizeBackendResult } from '@/utils/results'
import { useDatabaseType } from '@/stores/chatStore'
import type { QueryResult } from '@/types/domain'
import { estimateCost, assessImpact, extractLineage, type CostEstimate, type ImpactAssessment } from '@/utils/sqlAnalyzer'
import { ScheduleDialog } from '@/components/ScheduleDialog'
import { ExecutionTimeline } from '@/components/ExecutionTimeline'
import { LineageView } from '@/components/LineageView'
import { AlertTriangle, Info } from 'lucide-react'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'

export function QueryBuilder() {
  const [sql, setSQL] = useState<string>('')
  const [results, setResults] = useState<QueryResult | null>(null)
  const [executing, setExecuting] = useState(false)
  const [connections, setConnections] = useState<Array<{ name: string; type?: string }>>([])
  const [connection, setConnection] = useState<string | undefined>(undefined)
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'editor' | 'results' | 'history'>('editor')
  const { items: historyItems, loadHistory } = useQueryHistory('qb_session', 10)

  // Batch Two Analysis State
  const [analysis, setAnalysis] = useState<{ cost: CostEstimate; impact: ImpactAssessment } | null>(null)
  const [lastExecutedSql, setLastExecutedSql] = useState<string>('')

  // Use global database type from store
  const databaseType = useDatabaseType()

  useEffect(() => {
    try {
      const params = new URLSearchParams(window.location.search)
      const initialSql = params.get('sql')
      if (initialSql) {
        setSQL(initialSql)
      }
      // Read database_type from URL if passed from HITL dialog
      const urlDbType = params.get('database_type')
      if (urlDbType === 'oracle' || urlDbType === 'doris') {
        // Import setDatabaseType if needed to sync with global store
        // For now, the global store is used, but URL param can inform user
        console.log(`[QueryBuilder] database_type from URL: ${urlDbType}`)
      }
    } catch {
      // ignore URL parsing issues
    }
  }, [])


  const handleRerunHistoryQuery = async (queryText: string) => {
    setSQL(queryText)
    setActiveTab('editor')
    try {
      await handleExecuteWithSQL(queryText)
    } catch {
      // Errors are already surfaced via error state
    }
  }

  const handleEditAndRunHistoryQuery = (queryText: string) => {
    setSQL(queryText)
    setActiveTab('editor')
  }

  useEffect(() => {
    let mounted = true
      ; (async () => {
        try {
          const res = await apiService.listConnections()
          if (mounted) {
            setConnections(res.connections || [])
            if (!connection && res.connections?.[0]?.name) setConnection(res.connections[0].name)
          }
        } catch (e: any) {
          if (mounted) setError(e.message || 'Failed to load connections')
        }
      })()
    return () => { mounted = false }
  }, [])

  useEffect(() => {
    if (activeTab === 'history') {
      loadHistory()
    }
  }, [activeTab, loadHistory])

  // Real-time analysis effect
  useEffect(() => {
    const timer = setTimeout(() => {
      if (sql.trim()) {
        setAnalysis({
          cost: estimateCost(sql),
          impact: assessImpact(sql)
        })
      } else {
        setAnalysis(null)
      }
    }, 500)
    return () => clearTimeout(timer)
  }, [sql])

  const handleExecute = async () => {
    await handleExecuteWithSQL(sql)
  }

  async function handleExecuteWithSQL(sqlText: string) {
    try {
      setExecuting(true)
      setError(null)
      // Use global database type from store
      const resp = await apiService.submitSQL(sqlText, connection, databaseType)
      setLastExecutedSql(sqlText)
      setActiveTab('results')
      if (resp.status === 'success') {
        const normalized = normalizeBackendResult(resp.results as any)
        setResults(normalized)
      } else {
        setResults(null)
        setError(resp.message || resp.error || 'SQL execution failed')
      }
    } catch (e: any) {
      setError(e.message || 'SQL execution failed')
    } finally {
      setExecuting(false)
    }
  }

  return (
    <div className="h-screen flex flex-col bg-gradient-to-b from-slate-50 via-slate-50 to-slate-100 dark:from-slate-950 dark:via-slate-950 dark:to-slate-900">
      <header className="bg-white/80 dark:bg-slate-900/80 backdrop-blur-md border-b border-gray-200/70 dark:border-slate-800/70 px-6 py-3">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="chat-header-subtitle font-semibold text-gray-900">Advanced Query Builder</h1>
            <p className="section-subtext text-gray-500 mt-1">Write and execute custom SQL queries</p>
          </div>
          <div className="flex items-center gap-3 text-sm">
            <Badge variant="outline" className="text-xs">
              <Database className="h-3 w-3 mr-1" />
              {databaseType === 'oracle' ? 'Oracle' : 'Doris'}
            </Badge>
            <label className="text-gray-600 dark:text-gray-300">Connection</label>
            <select
              value={connection || ''}
              onChange={(e) => setConnection(e.target.value || undefined)}
              className="border rounded px-2 py-1 text-sm bg-white dark:bg-slate-900 border-gray-300 dark:border-slate-700"
            >
              {connections.map((c) => (
                <option key={c.name} value={c.name}>{c.name}</option>
              ))}
            </select>
          </div>
        </div>
      </header>

      <div className="flex-1 flex overflow-hidden">
        <aside className="w-64 bg-white/80 dark:bg-slate-900/80 border-r border-gray-200/70 dark:border-slate-800/70 overflow-y-auto backdrop-blur-md">
          <div className="p-4">
            <h2 className="text-sm font-semibold text-gray-700 mb-3">Saved Queries</h2>
            <div className="text-xs text-gray-500">No saved queries yet.</div>
          </div>
        </aside>

        <div className="flex-1 flex flex-col overflow-hidden">
          <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as any)} className="flex-1 flex flex-col">
            <div className="bg-white/80 dark:bg-slate-900/80 border-b border-gray-200/70 dark:border-slate-800/70 px-4 py-2 backdrop-blur-md">
              <TabsList>
                <TabsTrigger value="editor">SQL Editor</TabsTrigger>
                <TabsTrigger value="results">Results</TabsTrigger>
                <TabsTrigger value="history">History</TabsTrigger>
              </TabsList>
            </div>

            <TabsContent value="editor" className="flex-1 flex flex-col m-0 p-4">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  {connection && <Badge variant="secondary">{connection}</Badge>}
                </div>
                <div className="flex items-center gap-2">
                  <ScheduleDialog
                    sql={sql}
                    connection={connection}
                    databaseType={databaseType}
                  />
                  <Button
                    onClick={handleExecute}
                    disabled={executing}
                    className="bg-gradient-to-r from-emerald-500 to-green-600 hover:from-emerald-600 hover:to-green-700"
                  >
                    <Play className="h-4 w-4 mr-2" />
                    {executing ? 'Executing...' : 'Execute Query'}
                  </Button>
                </div>
              </div>

              {/* Analysis Bar */}
              {analysis && (
                <div className="mb-3 flex items-center gap-4 bg-slate-50 dark:bg-slate-800/50 p-2 rounded-lg border border-slate-200 dark:border-slate-700">
                  <div className="flex items-center gap-2 text-xs">
                    <span className="font-semibold text-slate-500">Est. Cost:</span>
                    <Badge variant="outline" className={
                      analysis.cost.complexity === 'High' ? 'text-red-500 border-red-200 bg-red-50' :
                        analysis.cost.complexity === 'Medium' ? 'text-amber-500 border-amber-200 bg-amber-50' :
                          'text-green-500 border-green-200 bg-green-50'
                    }>
                      {analysis.cost.complexity} ({analysis.cost.cost})
                    </Badge>
                  </div>

                  <div className="flex items-center gap-2 text-xs">
                    <span className="font-semibold text-slate-500">Impact:</span>
                    <TooltipProvider>
                      <Tooltip>
                        <TooltipTrigger>
                          <Badge variant="outline" className={
                            analysis.impact.level === 'Critical' ? 'text-red-600 border-red-200 bg-red-100 font-bold' :
                              analysis.impact.level === 'Moderate' ? 'text-amber-600 border-amber-200 bg-amber-100' :
                                'text-blue-600 border-blue-200 bg-blue-100'
                          }>
                            {analysis.impact.level === 'Critical' && <AlertTriangle className="w-3 h-3 mr-1" />}
                            {analysis.impact.level}
                          </Badge>
                        </TooltipTrigger>
                        <TooltipContent>
                          <p>{analysis.impact.description}</p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  </div>

                  {analysis.cost.reason && (
                    <div className="flex items-center gap-1 text-xs text-slate-500 ml-auto">
                      <Info className="w-3 h-3" />
                      {analysis.cost.reason}
                    </div>
                  )}
                </div>
              )}

              <div className="flex-1 overflow-hidden">
                <MonacoSQLEditor
                  value={sql}
                  onChange={(value) => setSQL(value || '')}
                  readOnly={false}
                  height="100%"
                />
              </div>
            </TabsContent>

            <TabsContent value="results" className="m-0 p-3 overflow-auto">
              {error && (
                <Card className="border-red-300 bg-red-50 mb-3">
                  <CardContent className="p-3 text-sm text-red-700 whitespace-pre-wrap break-words max-h-[240px] overflow-auto">{error}</CardContent>
                </Card>
              )}
              {results ? (
                <div className="mt-0 space-y-4">
                  <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                    <div className="lg:col-span-2">
                      {lastExecutedSql && <LineageView lineage={extractLineage(lastExecutedSql)} />}
                    </div>
                    <div>
                      {results.executionTime !== undefined && (
                        <ExecutionTimeline totalTimeMs={results.executionTime} />
                      )}
                    </div>
                  </div>

                  <QueryResultsTable
                    columns={results.columns}
                    rows={results.rows}
                    executionTime={results.executionTime}
                    rowCount={results.rowCount}
                  />
                </div>
              ) : (
                <Card className="border border-gray-200 dark:border-slate-800 bg-white/80 dark:bg-slate-900/80 backdrop-blur-md">
                  <CardContent className="flex items-center justify-center h-64">
                    <div className="text-center text-gray-500 dark:text-gray-400 text-sm">Execute a query to see results</div>
                  </CardContent>
                </Card>
              )}
            </TabsContent>

            <TabsContent value="history" className="m-0 p-4 overflow-auto">
              <QueryHistoryEnhanced
                history={historyItems}
                onRerun={handleRerunHistoryQuery}
                onEditAndRun={handleEditAndRunHistoryQuery}
              />
            </TabsContent>
          </Tabs>
        </div>
      </div>
    </div>
  )
}
