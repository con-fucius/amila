import { useEffect, useMemo, useState } from 'react'
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
      <header className="bg-white dark:bg-slate-900 border-b border-gray-200 dark:border-slate-800 px-6 py-4">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="chat-header-subtitle font-semibold text-gray-900 dark:text-gray-50">Schema Browser</h1>
            <p className="section-subtext text-gray-500 dark:text-gray-400 mt-1">Explore database tables and relationships</p>
          </div>
          <div className="flex items-center gap-3">
            <Badge variant="outline" className="text-xs">
              <Database className="h-3 w-3 mr-1" />
              {databaseType === 'oracle' ? 'Oracle' : 'Doris'}
            </Badge>
            <Button variant="outline" size="sm" onClick={fetchSchema} disabled={loading}>
              <RefreshCw className="h-4 w-4 mr-1" />
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
            <div className="mb-4">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400 dark:text-gray-500" />
                <Input
                  placeholder="Search tables..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-9"
                />
              </div>
            </div>

            <div className="space-y-2">
              {filteredTables.map((table) => (
                <button
                  key={table.name}
                  onClick={() => setSelectedTable(table)}
                  className={`w-full text-left p-3 rounded-lg border transition-all ${selectedTable?.name === table.name
                    ? 'border-green-500 bg-green-50 dark:bg-emerald-900/20'
                    : 'hover:bg-gray-50 dark:hover:bg-slate-800 border-transparent'
                    }`}
                >
                  <div className="flex items-center gap-2 mb-1">
                    <Table className="h-4 w-4 text-gray-600 dark:text-gray-300" />
                    <span className="font-medium text-sm text-gray-800 dark:text-gray-100">{table.name}</span>
                    <Badge variant="secondary" className="text-xs ml-auto">
                      {table.type}
                    </Badge>
                  </div>
                  <div className="text-xs text-gray-500">
                    {table.columns.length} columns
                  </div>
                </button>
              ))}
            </div>
          </div>
        </aside>

        {/* Center - Table Details */}
        <div className="flex-1 overflow-auto p-6">
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
              <Card>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <div>
                      <h2 className="text-xl font-bold text-gray-900 dark:text-gray-50">{selectedTable.name}</h2>
                      <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                        {selectedTable.columns.length} columns
                      </p>
                    </div>
                    <Badge variant="secondary">{selectedTable.type}</Badge>
                  </div>
                </CardHeader>
              </Card>

              <Tabs defaultValue="columns">
                <TabsList>
                  <TabsTrigger value="columns">Columns</TabsTrigger>
                  <TabsTrigger value="stats" onClick={() => !stats.length && setShowStatsWarning(true)}>
                    Statistics
                  </TabsTrigger>
                </TabsList>

                <TabsContent value="columns" className="mt-4">
                  <Card>
                    <CardContent className="p-0">
                      <div className="overflow-x-auto">
                        <table className="w-full">
                          <thead className="bg-gray-50 dark:bg-slate-900/70 border-b border-gray-200 dark:border-slate-800">
                            <tr>
                              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-200">Column Name</th>
                              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-200">Data Type</th>
                              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-200">Nullable</th>
                              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-200">Keys</th>
                            </tr>
                          </thead>
                          <tbody>
                            {selectedTable.columns.map((column, idx) => (
                              <tr key={idx} className="border-b border-gray-100 dark:border-slate-800 hover:bg-gray-50 dark:hover:bg-slate-900/60">
                                <td className="px-4 py-3 text-sm font-mono text-gray-800 dark:text-gray-100">{column.name}</td>
                                <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-300">{column.dataType}</td>
                                <td className="px-4 py-3 text-sm">
                                  {column.nullable ? (
                                    <Badge variant="outline" className="text-xs">NULL</Badge>
                                  ) : (
                                    <Badge variant="secondary" className="text-xs">NOT NULL</Badge>
                                  )}
                                </td>
                                <td className="px-4 py-3 text-sm">
                                  <div className="flex gap-1">
                                    {column.isPrimaryKey && (
                                      <Badge variant="default" className="text-xs flex items-center gap-1">
                                        <Key className="h-3 w-3" />PK
                                      </Badge>
                                    )}
                                    {column.isForeignKey && (
                                      <Badge variant="outline" className="text-xs flex items-center gap-1">
                                        <Key className="h-3 w-3" />FK
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
                            <thead className="bg-gray-50 dark:bg-slate-900/70 border-b">
                              <tr>
                                <th className="px-4 py-3 text-left text-xs font-semibold">Column</th>
                                <th className="px-4 py-3 text-left text-xs font-semibold">Type</th>
                                <th className="px-4 py-3 text-right text-xs font-semibold">Distinct</th>
                                <th className="px-4 py-3 text-right text-xs font-semibold">Nulls</th>
                                <th className="px-4 py-3 text-right text-xs font-semibold">Min</th>
                                <th className="px-4 py-3 text-right text-xs font-semibold">Max</th>
                              </tr>
                            </thead>
                            <tbody>
                              {stats.map((stat, idx) => (
                                <tr key={idx} className="border-b border-gray-100 dark:border-slate-800">
                                  <td className="px-4 py-3 text-sm font-mono">{stat.column}</td>
                                  <td className="px-4 py-3 text-sm text-gray-500">{stat.type}</td>
                                  <td className="px-4 py-3 text-sm text-right">{stat.distinct_count?.toLocaleString() ?? '-'}</td>
                                  <td className="px-4 py-3 text-sm text-right">{stat.null_count?.toLocaleString() ?? '-'}</td>
                                  <td className="px-4 py-3 text-sm text-right font-mono">{stat.min ?? '-'}</td>
                                  <td className="px-4 py-3 text-sm text-right font-mono">{stat.max ?? '-'}</td>
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
      </div>

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
    </div>
  )
}
