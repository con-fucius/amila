import { Table, Database, Eye, ChevronDown, ChevronRight, Layers } from 'lucide-react'
import { StatusCapsule } from './StatusCapsule'
import { useState } from 'react'
import { Card, CardContent } from './ui/card'
import { cn } from '@/utils/cn'

interface TableSchema {
  tableName: string
  columns: Array<{
    columnName: string
    dataType: string
    nullable: boolean
  }>
  sampleRows?: Array<Record<string, any>>
  rowCount?: number
}

interface SchemaPreviewProps {
  schemas: TableSchema[]
  visible?: boolean
}

export function SchemaPreview({ schemas, visible = true }: SchemaPreviewProps) {
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set()) // All collapsed by default

  if (!visible || schemas.length === 0) {
    return null
  }

  const toggleTable = (tableName: string) => {
    setExpandedTables((prev) => {
      const next = new Set(prev)
      if (next.has(tableName)) {
        next.delete(tableName)
      } else {
        next.add(tableName)
      }
      return next
    })
  }

  return (
    <Card className="border-purple-200 bg-purple-50">
      <CardContent className="p-4">
        <div className="flex items-center gap-2 mb-3">
          <Database className="h-4 w-4 text-purple-600" />
          <span className="font-semibold text-purple-900 text-sm">Schema Exploration</span>
          <span className="font-semibold text-purple-900 text-sm">Schema Exploration</span>
          <StatusCapsule
            status="active"
            label={`${schemas.length} ${schemas.length === 1 ? 'table' : 'tables'} inspected`}
            icon={Layers}
            size="sm"
            className="bg-purple-100 text-purple-700 border-purple-200"
          />
        </div>

        <div className="space-y-2">
          {schemas.map((schema) => {
            const isExpanded = expandedTables.has(schema.tableName)

            return (
              <div key={schema.tableName} className="border border-purple-200 rounded-lg bg-white">
                <button
                  onClick={() => toggleTable(schema.tableName)}
                  className="w-full flex items-center justify-between p-3 hover:bg-purple-50 transition-colors"
                >
                  <div className="flex items-center gap-2">
                    {isExpanded ? (
                      <ChevronDown className="h-4 w-4 text-purple-600" />
                    ) : (
                      <ChevronRight className="h-4 w-4 text-purple-600" />
                    )}
                    <Table className="h-4 w-4 text-purple-600" />
                    <span className="font-medium text-sm text-gray-800">{schema.tableName}</span>
                    <span className="font-medium text-sm text-gray-800">{schema.tableName}</span>
                    <StatusCapsule
                      status="info"
                      label={`${schema.columns.length} columns`}
                      showLabel={true}
                      size="sm"
                      className="h-5 px-1.5 bg-gray-100 text-gray-600 border-gray-200"
                    />
                    {schema.rowCount && (
                      <StatusCapsule
                        status="success"
                        label={`~${schema.rowCount.toLocaleString()} rows`}
                        showLabel={true}
                        size="sm"
                        className="h-5 px-1.5 bg-green-50 text-green-700 border-green-200"
                      />
                    )}
                  </div>
                  <Eye className="h-4 w-4 text-gray-400" />
                </button>

                {isExpanded && (
                  <div className="border-t border-purple-200 p-3 space-y-3">
                    {/* Columns */}
                    <div>
                      <div className="text-xs font-semibold text-gray-600 mb-2">Columns</div>
                      <div className="grid grid-cols-2 gap-2">
                        {schema.columns.slice(0, 10).map((col) => (
                          <div
                            key={col.columnName}
                            className="flex items-center justify-between text-xs bg-gray-50 px-2 py-1 rounded"
                          >
                            <span className="font-mono text-gray-700">{col.columnName}</span>
                            <span className="text-gray-500">{col.dataType}</span>
                          </div>
                        ))}
                        {schema.columns.length > 10 && (
                          <div className="text-xs text-gray-500 col-span-2">
                            + {schema.columns.length - 10} more columns
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Sample Data */}
                    {schema.sampleRows && schema.sampleRows.length > 0 && (
                      <div>
                        <div className="text-xs font-semibold text-gray-600 mb-2">Sample Data</div>
                        <div className="overflow-x-auto">
                          <table className="w-full text-xs border border-gray-200 rounded">
                            <thead className="bg-gray-100">
                              <tr>
                                {Object.keys(schema.sampleRows[0]).slice(0, 5).map((key) => (
                                  <th key={key} className="px-2 py-1 text-left font-medium text-gray-700">
                                    {key}
                                  </th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {schema.sampleRows.slice(0, 3).map((row, idx) => (
                                <tr key={idx} className={cn('border-t', idx % 2 === 0 ? 'bg-white' : 'bg-gray-50')}>
                                  {Object.values(row).slice(0, 5).map((val, valIdx) => (
                                    <td key={valIdx} className="px-2 py-1 text-gray-600">
                                      {val !== null && val !== undefined ? String(val) : (
                                        <span className="text-gray-400 italic">null</span>
                                      )}
                                    </td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )
          })}
        </div>
      </CardContent>
    </Card>
  )
}
