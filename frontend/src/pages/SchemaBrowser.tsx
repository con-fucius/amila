import { useEffect, useMemo, useState } from 'react'
import { cn } from '@/utils/cn'
import { Search, Table, Key, RefreshCw, Database, BarChart3, Loader2 } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { apiService } from '@/services/apiService'
import { useDatabaseType } from '@/stores/chatStore'
import { useBackendHealth } from '@/hooks/useBackendHealth'
import { DatabaseSelector } from '@/components/DatabaseSelector'

interface ColumnInfo {
  name: string
  dataType: string
  nullable: boolean
  isPrimaryKey: boolean
  isForeignKey: boolean
}

interface TableInfo {
  name: string
  type: 'TABLE' | 'VIEW'
  rowCount: number
  columns: ColumnInfo[]
}

interface ColumnStat {
  column: string
  type: string
  distinct_count?: number
  null_count?: number
  min?: any
  max?: any
  error?: string
}

export function SchemaBrowser() {
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null)
  const [tables, setTables] = useState<TableInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Stats state
  const [stats, setStats] = useState<ColumnStat[]>([])
  const [statsLoading, setStatsLoading] = useState(false)
  const [showStatsWarning, setShowStatsWarning] = useState(false)

  const databaseType = useDatabaseType()
  const { components } = useBackendHealth(10000)

  const fetchSchema = async () => {
    try {
      setLoading(true)
      setError(null)
      const res = await apiService.getSchema({ use_cache: true, database_type: databaseType })
      if (res.status && res.status !== 'success') {
        setTables([])
        setSelectedTable(null)
        setError((res as any).error || 'Failed to load schema')
        return
      }
      const schema = (res as any).schema || (res as any).schema_data || {}
      const tbls: TableInfo[] = Object.keys(schema.tables || {}).map((t) => ({
        name: t,
        type: 'TABLE',
        rowCount: (schema.tables[t] || []).length,
        columns: (schema.tables[t] || []).map((c: any) => ({
          name: c.name || c[0],
          dataType: c.type || c[1] || 'UNKNOWN',
          nullable: !!(c.nullable ?? true),
          isPrimaryKey: false,
          isForeignKey: false,
        })),
      }))
      setTables(tbls)
      if (tbls.length && !selectedTable) setSelectedTable(tbls[0])
    } catch (e: any) {
      setError(e.message || 'Failed to load schema')
    } finally {
      setLoading(false)
    }
  }



  const fetchStats = async (tableName: string) => {
    try {
      setStatsLoading(true)
      setStats([])
      const res = await apiService.getTableStats(tableName, databaseType)
      if (res.status === 'success') {
        setStats(res.stats)
      }
    } catch (e: any) {
      console.error('Failed to fetch stats:', e)
    } finally {
      setStatsLoading(false)
    }
  }

  useEffect(() => { fetchSchema() }, [databaseType])

  useEffect(() => {
    if (selectedTable) {
      setStats([])
    }
  }, [selectedTable])

  const filteredTables = useMemo(() => tables.filter((table) =>
    table.name.toLowerCase().includes(searchQuery.toLowerCase())
  ), [tables, searchQuery])


  return (
    <div className="h-screen flex flex-col bg-gray-50 dark:bg-slate-950">
      {/* Header */}
      <header className="bg-white dark:bg-slate-900 border-b border-gray-200 dark:border-slate-800 px-6 py-6 backdrop-blur-md">
        <div className="flex items-center justify-between">
          <div className="space-y-1">
            <h1 className="text-2xl font-bold tracking-tight text-gray-900 dark:text-gray-50">Schema Browser</h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">Explore database tables, columns, and relationships</p>
          </div>
          <div className="flex items-center gap-4">
            <DatabaseSelector variant="header" className="mr-2" />
            <Button
              variant="outline"
              size="sm"
              onClick={fetchSchema}
              disabled={loading}
              className="h-9 px-4 border-gray-200 dark:border-slate-700 hover:bg-gray-50 dark:hover:bg-slate-800"
            >
              <RefreshCw className={cn("h-4 w-4 mr-2", loading && "animate-spin")} />
              {loading ? 'Refreshing...' : 'Refresh Schema'}
            </Button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left Panel - Table List */}
        <aside className="w-80 bg-white dark:bg-slate-900/80 border-r border-gray-200 dark:border-slate-800 overflow-y-auto">
          <div className="p-4">
            <div className="mb-6">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400 dark:text-gray-500" />
                <Input
                  placeholder="Search tables..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-10 h-10 bg-gray-50 dark:bg-slate-800 border-gray-200 dark:border-slate-700 focus:ring-emerald-500 rounded-xl"
                />
              </div>
            </div>

            <div className="space-y-1">
              {filteredTables.map((table) => (
                <button
                  key={table.name}
                  onClick={() => setSelectedTable(table)}
                  className={cn(
                    "w-full flex items-center gap-3 px-4 py-3 rounded-xl text-left transition-all duration-200 group relative",
                    selectedTable?.name === table.name
                      ? "bg-emerald-50 dark:bg-emerald-900/20 text-emerald-700 dark:text-emerald-400 ring-1 ring-emerald-200 dark:ring-emerald-800/50"
                      : "text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-slate-800"
                  )}
                >
                  <div className={cn(
                    "p-1.5 rounded-lg transition-colors",
                    selectedTable?.name === table.name
                      ? "bg-emerald-100 dark:bg-emerald-900/40 text-emerald-600 dark:text-emerald-400"
                      : "bg-gray-100 dark:bg-slate-800 text-gray-400 group-hover:text-gray-600 dark:group-hover:text-gray-300"
                  )}>
                    <Table className="h-4 w-4" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-semibold truncate uppercase tracking-tight">
                      {table.name}
                    </p>
                    <p className="text-[10px] opacity-60">
                      {table.rowCount} rows detected
                    </p>
                  </div>
                  {selectedTable?.name === table.name && (
                    <div className="absolute right-3 w-1.5 h-1.5 rounded-full bg-emerald-500" />
                  )}
                </button>
              ))}

              {filteredTables.length === 0 && (
                <div className="text-center py-10 px-4">
                  <Table className="h-10 w-10 mx-auto text-gray-300 dark:text-gray-700 mb-3 opacity-50" />
                  <p className="text-sm text-gray-500 dark:text-gray-400">No tables found matching your search</p>
                </div>
              )}
            </div>
          </div>
        </aside>

        {/* Center - Table Details */}
        <div className="flex-1 overflow-auto p-6 relative">
          <div className="absolute top-6 right-6 z-10 hidden sm:flex">
          </div>
          {error && (
            <div className="mb-4">
              <Card className="border-red-300 bg-red-50 dark:border-red-800 dark:bg-red-950/40">
                <CardContent className="p-3 text-sm text-red-700 dark:text-red-200">
                  {error}
                </CardContent>
              </Card>
            </div>
          )}
          {selectedTable ? (
            <div className="space-y-4">
              <Card className="overflow-hidden border-gray-200 dark:border-slate-800 shadow-sm">
                <div className="h-1 bg-gradient-to-r from-emerald-500 to-green-500" />
                <CardHeader className="py-5">
                  <div className="flex items-center justify-between">
                    <div>
                      <div className="flex items-center gap-2 mb-1">
                        <Database className="h-4 w-4 text-emerald-500" />
                        <h2 className="text-xl font-bold text-gray-900 dark:text-gray-50 tracking-tight">{selectedTable.name}</h2>
                      </div>
                      <p className="text-xs text-gray-500 dark:text-gray-400">
                        {selectedTable.type} &bull; {selectedTable.columns.length} columns defined
                      </p>
                    </div>
                    <div className="flex flex-col items-end gap-1">
                      <Badge variant="secondary" className="bg-emerald-100/50 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300 border-emerald-200/50 dark:border-emerald-800/30 font-semibold px-2 py-0.5">
                        Active Schema
                      </Badge>
                    </div>
                  </div>
                </CardHeader>
              </Card>

              <Tabs defaultValue="columns">
                <TabsList className="bg-gray-100/50 dark:bg-slate-900/50 p-1">
                  <TabsTrigger
                    value="columns"
                    className="data-[state=active]:bg-white dark:data-[state=active]:bg-slate-800 data-[state=active]:text-emerald-600 dark:data-[state=active]:text-emerald-400"
                  >
                    Columns
                  </TabsTrigger>
                  <TabsTrigger
                    value="stats"
                    onClick={(e) => {
                      if (!stats.length) {
                        e.preventDefault();
                        setShowStatsWarning(true);
                      }
                    }}
                    className="data-[state=active]:bg-white dark:data-[state=active]:bg-slate-800 data-[state=active]:text-emerald-600 dark:data-[state=active]:text-emerald-400"
                  >
                    Statistics
                  </TabsTrigger>
                </TabsList>

                <TabsContent value="columns" className="mt-4">
                  <Card>
                    <CardContent className="p-0">
                      <div className="overflow-x-auto">
                        <table className="w-full">
                          <thead className="bg-gray-50/50 dark:bg-slate-900/50 border-b border-gray-200 dark:border-slate-800">
                            <tr>
                              <th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Column Name</th>
                              <th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Data Type</th>
                              <th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Nullable</th>
                              <th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Keys</th>
                            </tr>
                          </thead>
                          <tbody>
                            {selectedTable.columns.map((column, idx) => (
                              <tr key={idx} className="border-b border-gray-100 dark:border-slate-800/60 hover:bg-gray-50/30 dark:hover:bg-slate-900/40 transition-colors">
                                <td className="px-5 py-3 text-sm font-mono text-gray-800 dark:text-gray-100 font-medium tracking-tight">
                                  {column.name}
                                </td>
                                <td className="px-5 py-3 text-[11px] text-gray-500 dark:text-gray-400 font-mono italic">
                                  {column.dataType}
                                </td>
                                <td className="px-5 py-3">
                                  {column.nullable ? (
                                    <Badge variant="outline" className="text-[9px] h-5 font-normal border-gray-200 dark:border-slate-700 text-gray-400">NULLABLE</Badge>
                                  ) : (
                                    <Badge variant="secondary" className="text-[9px] h-5 font-bold bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300">NOT NULL</Badge>
                                  )}
                                </td>
                                <td className="px-5 py-3 text-sm">
                                  <div className="flex gap-1">
                                    {column.isPrimaryKey && (
                                      <Badge className="text-[9px] h-5 bg-amber-100 text-amber-700 border-amber-200 hover:bg-amber-100 flex items-center gap-1">
                                        <Key className="h-2.5 w-2.5" /> PRIMARY
                                      </Badge>
                                    )}
                                    {column.isForeignKey && (
                                      <Badge variant="outline" className="text-[9px] h-5 border-blue-200 text-blue-600 flex items-center gap-1">
                                        <Key className="h-2.5 w-2.5" /> FOREIGN
                                      </Badge>
                                    )}
                                  </div>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </CardContent>
                  </Card>
                </TabsContent>


                <TabsContent value="stats" className="mt-4">
                  <Card>
                    <CardContent className="p-4">
                      {statsLoading ? (
                        <div className="flex items-center justify-center py-8">
                          <Loader2 className="h-6 w-6 animate-spin text-emerald-500" />
                          <span className="ml-2 text-gray-500">Loading statistics...</span>
                        </div>
                      ) : stats.length > 0 ? (
                        <div className="overflow-x-auto">
                          <table className="w-full">
                            <thead className="bg-gray-50/50 dark:bg-slate-900/50 border-b">
                              <tr>
                                <th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Column</th>
                                <th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Type</th>
                                <th className="px-5 py-3 text-right text-[11px] font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Distinct</th>
                                <th className="px-5 py-3 text-right text-[11px] font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Nulls</th>
                                <th className="px-5 py-3 text-right text-[11px] font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Min</th>
                                <th className="px-5 py-3 text-right text-[11px] font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Max</th>
                              </tr>
                            </thead>
                            <tbody>
                              {stats.map((stat, idx) => (
                                <tr key={idx} className="border-b border-gray-100 dark:border-slate-800/60 hover:bg-gray-50/30 dark:hover:bg-slate-900/40 transition-colors">
                                  <td className="px-5 py-3 text-sm font-mono font-medium text-gray-800 dark:text-gray-100">{stat.column}</td>
                                  <td className="px-5 py-3 text-sm text-gray-500 dark:text-gray-400 font-mono text-xs">{stat.type}</td>
                                  <td className="px-5 py-3 text-sm text-right font-mono">{stat.distinct_count?.toLocaleString() ?? '-'}</td>
                                  <td className="px-5 py-3 text-sm text-right font-mono text-gray-500">{stat.null_count?.toLocaleString() ?? '-'}</td>
                                  <td className="px-5 py-3 text-sm text-right font-mono text-blue-600 dark:text-blue-400 truncate max-w-[120px]">{stat.min ?? '-'}</td>
                                  <td className="px-5 py-3 text-sm text-right font-mono text-purple-600 dark:text-purple-400 truncate max-w-[120px]">{stat.max ?? '-'}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : (
                        <div className="text-center py-8">
                          <BarChart3 className="h-12 w-12 mx-auto text-gray-300 mb-3" />
                          <p className="text-gray-500">Click to load column statistics</p>
                          <Button
                            variant="outline"
                            size="sm"
                            className="mt-3"
                            onClick={() => fetchStats(selectedTable.name)}
                          >
                            Load Statistics
                          </Button>
                        </div>
                      )}
                    </CardContent>
                  </Card>
                </TabsContent>
              </Tabs>
            </div>
          ) : (
            <Card>
              <CardContent className="flex items-center justify-center h-96">
                <div className="text-center text-gray-500 dark:text-gray-400">
                  <Database className="h-16 w-16 mx-auto mb-4 opacity-30" />
                  <p className="text-lg font-medium text-gray-700 dark:text-gray-100">Select a table to view details</p>
                  <p className="text-sm mt-1">Choose from the list on the left</p>
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      </div >

      <Dialog open={showStatsWarning} onOpenChange={setShowStatsWarning}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Load Table Statistics?</DialogTitle>
            <DialogDescription>
              Calculating statistics (distinct counts, nulls, min/max) can be an expensive operation for large tables.
              This may take a few moments.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowStatsWarning(false)}>Cancel</Button>
            <Button onClick={() => {
              setShowStatsWarning(false)
              if (selectedTable) fetchStats(selectedTable.name)
            }}>
              Proceed
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div >
  )
}
