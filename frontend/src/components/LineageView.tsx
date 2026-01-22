
import { ArrowRight, Database, Table, FileInput, GitMerge } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import type { LineageInfo } from '@/utils/sqlAnalyzer'

export function LineageView({ lineage }: { lineage: LineageInfo }) {
    if (!lineage || lineage.tables.length === 0) return null

    const getJoinBadgeColor = (joinType: string) => {
        switch (joinType) {
            case 'INNER': return 'bg-blue-100 text-blue-700 border-blue-200 dark:bg-blue-900/30 dark:text-blue-300 dark:border-blue-700'
            case 'LEFT': return 'bg-purple-100 text-purple-700 border-purple-200 dark:bg-purple-900/30 dark:text-purple-300 dark:border-purple-700'
            case 'RIGHT': return 'bg-orange-100 text-orange-700 border-orange-200 dark:bg-orange-900/30 dark:text-orange-300 dark:border-orange-700'
            case 'FULL': return 'bg-red-100 text-red-700 border-red-200 dark:bg-red-900/30 dark:text-red-300 dark:border-red-700'
            case 'CROSS': return 'bg-yellow-100 text-yellow-700 border-yellow-200 dark:bg-yellow-900/30 dark:text-yellow-300 dark:border-yellow-700'
            default: return 'bg-gray-100 text-gray-700 border-gray-200 dark:bg-gray-900/30 dark:text-gray-300 dark:border-gray-700'
        }
    }

    return (
        <Card className="mb-4 border-gray-200 dark:border-slate-800 bg-white/50 dark:bg-slate-900/50">
            <CardHeader className="py-3 px-4">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                    <FileInput className="w-4 h-4 text-blue-500" />
                    Data Lineage Trail
                    {lineage.joins && lineage.joins.length > 0 && (
                        <Badge variant="outline" className="ml-2 text-[10px]">
                            {lineage.joins.length} {lineage.joins.length === 1 ? 'JOIN' : 'JOINs'}
                        </Badge>
                    )}
                </CardTitle>
            </CardHeader>
            <CardContent className="py-2 px-4">
                <div className="flex flex-wrap items-center gap-3 text-sm">
                    {lineage.tables.map((table, i) => {
                        const joinInfo = lineage.joins?.find(j => j.table === table)
                        
                        return (
                            <div key={table} className="flex items-center gap-2 animate-in fade-in zoom-in duration-300" style={{ animationDelay: `${i * 100}ms` }}>
                                <div className="flex flex-col gap-1">
                                    <div className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md bg-blue-50 dark:bg-blue-900/20 border border-blue-100 dark:border-blue-800 text-blue-700 dark:text-blue-300">
                                        <Table className="w-3.5 h-3.5" />
                                        <span className="font-mono text-xs">{table}</span>
                                    </div>
                                    {joinInfo && (
                                        <div className="flex items-center gap-1 justify-center">
                                            <GitMerge className="w-2.5 h-2.5 text-gray-400" />
                                            <Badge variant="outline" className={`text-[9px] px-1 py-0 h-4 ${getJoinBadgeColor(joinInfo.type)}`}>
                                                {joinInfo.type}
                                            </Badge>
                                        </div>
                                    )}
                                </div>
                                {i < lineage.tables.length - 1 && (
                                    <ArrowRight className="w-3 h-3 text-gray-400" />
                                )}
                            </div>
                        )
                    })}

                    <div className="flex items-center gap-2">
                        <ArrowRight className="w-3 h-3 text-gray-400" />
                        <div className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md bg-purple-50 dark:bg-purple-900/20 border border-purple-100 dark:border-purple-800 text-purple-700 dark:text-purple-300 font-medium">
                            <Database className="w-3.5 h-3.5" />
                            <span>Query Execution</span>
                        </div>
                        <ArrowRight className="w-3 h-3 text-gray-400" />
                        <div className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md bg-emerald-50 dark:bg-emerald-900/20 border border-emerald-100 dark:border-emerald-800 text-emerald-700 dark:text-emerald-300 font-medium">
                            <span>Result Set</span>
                        </div>
                    </div>
                </div>
            </CardContent>
        </Card>
    )
}
