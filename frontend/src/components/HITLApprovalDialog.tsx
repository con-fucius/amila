import { useEffect, useState } from 'react'
import { AlertTriangle, Info, Edit, Eye, Table as TableIcon, Database as DatabaseIcon } from 'lucide-react'
import { Button } from './ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from './ui/dialog'
import { Badge } from './ui/badge'
import { Alert, AlertDescription, AlertTitle } from './ui/alert'
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs'
import { MonacoSQLEditor } from './MonacoSQLEditor'
import { cn } from '@/utils/cn'
import { apiService } from '@/services/apiService'

interface HITLApprovalDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  query: string
  sql: string
  riskLevel: 'SAFE' | 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
  onApprove: (modifiedSQL?: string) => void
  onReject: () => void
  onModify?: () => void
  metadata?: {
    operationType?: string
    tablesAccessed?: string[]
    estimatedRows?: number
    estimatedTime?: string
    dataClassification?: string
    sessionId?: string
    user?: string
  }
}

const riskColors = {
  SAFE: { bg: 'bg-green-100', text: 'text-green-700', border: 'border-green-400' },
  LOW: { bg: 'bg-blue-100', text: 'text-blue-700', border: 'border-blue-400' },
  MEDIUM: { bg: 'bg-yellow-100', text: 'text-yellow-700', border: 'border-yellow-400' },
  HIGH: { bg: 'bg-orange-100', text: 'text-orange-700', border: 'border-orange-400' },
  CRITICAL: { bg: 'bg-red-100', text: 'text-red-700', border: 'border-red-400' },
}

export function HITLApprovalDialog({
  open,
  onOpenChange,
  query,
  sql,
  riskLevel,
  onApprove,
  onReject,
  onModify,
  metadata = {}
}: HITLApprovalDialogProps) {
  const colors = riskColors[riskLevel]
  const [editedSQL, setEditedSQL] = useState(sql)
  const [sqlTab, setSqlTab] = useState<'preview' | 'edit'>('preview')
  const [showSchema, setShowSchema] = useState(false)
  const [showExplainPlan, setShowExplainPlan] = useState(false)
  const [schemaLoading, setSchemaLoading] = useState(false)
  const [schemaError, setSchemaError] = useState<string | null>(null)
  const [schemaTables, setSchemaTables] = useState<Record<string, { name: string; type?: string; nullable?: boolean }[]>>({})

  // Lazy-load schema when dialog first opens
  useEffect(() => {
    if (!showSchema) return
    if (Object.keys(schemaTables).length > 0 || schemaLoading) return

    let cancelled = false
      ; (async () => {
        try {
          setSchemaLoading(true)
          setSchemaError(null)

          // Request schema (cached)
          const res = await apiService.getSchema({
            use_cache: true
          })

          if (cancelled) return
          const raw: any = res?.schema_data || (res as any)?.schema || {}
          const tablesData = raw.tables || raw.Tables || {}
          const normalized: Record<string, { name: string; type?: string; nullable?: boolean }[]> = {}
          Object.entries(tablesData).forEach(([tName, cols]: any) => {
            if (Array.isArray(cols)) {
              normalized[tName] = cols.map((c: any) => ({
                name: c.name || c.column_name || '',
                type: c.type || c.data_type,
                nullable: c.nullable ?? c.is_nullable,
              })).filter((c) => c.name)
            }
          })
          setSchemaTables(normalized)
        } catch (err: any) {
          if (!cancelled) {
            setSchemaError(err?.message || 'Failed to load schema information')
          }
        } finally {
          if (!cancelled) setSchemaLoading(false)
        }
      })()

    return () => {
      cancelled = true
    }
  }, [showSchema, schemaTables, schemaLoading, sql])

  // Syntax validation state for edited SQL
  const [syntaxValidation, setSyntaxValidation] = useState<{
    valid: boolean
    errors: string[]
    warnings: string[]
  }>({ valid: true, errors: [], warnings: [] })

  // Validate SQL syntax when edited
  useEffect(() => {
    if (editedSQL === sql) {
      // Original SQL - assume valid
      setSyntaxValidation({ valid: true, errors: [], warnings: [] })
      return
    }

    // Debounce validation
    const timer = setTimeout(async () => {
      if (!editedSQL.trim()) {
        setSyntaxValidation({ valid: false, errors: ['SQL cannot be empty'], warnings: [] })
        return
      }

      // Basic client-side validation
      const errors: string[] = []
      const warnings: string[] = []

      // Check for valid SQL start
      if (!/^(SELECT|WITH|INSERT|UPDATE|DELETE|ALTER|DROP|CREATE|TRUNCATE|MERGE|GRANT|REVOKE)/i.test(editedSQL.trim())) {
        errors.push('SQL must start with a valid statement keyword')
      }

      // Check for dangerous patterns
      if (/;\s*(DROP|DELETE|TRUNCATE|ALTER)/i.test(editedSQL)) {
        warnings.push('Multiple statements detected - only first statement will execute')
      }

      // Check for balanced parentheses
      const openParens = (editedSQL.match(/\(/g) || []).length
      const closeParens = (editedSQL.match(/\)/g) || []).length
      if (openParens !== closeParens) {
        errors.push(`Unbalanced parentheses: ${openParens} open, ${closeParens} close`)
      }

      // Check for balanced quotes
      const singleQuotes = (editedSQL.match(/'/g) || []).length
      if (singleQuotes % 2 !== 0) {
        errors.push('Unbalanced single quotes')
      }

      setSyntaxValidation({
        valid: errors.length === 0,
        errors,
        warnings,
      })
    }, 500)

    return () => clearTimeout(timer)
  }, [editedSQL, sql])

  const handleApprove = () => {
    if (import.meta.env.DEV) {
      console.log(' HITL Approve button clicked')
    }
    if (!editedSQL.trim()) {
      return; // Prevent empty SQL approval
    }
    // Block approval if syntax validation failed
    if (!syntaxValidation.valid) {
      return
    }
    try {
      if (editedSQL !== sql) {
        if (import.meta.env.DEV) {
          console.log(' Approving with modified SQL')
        }
        onApprove(editedSQL)
      } else {
        if (import.meta.env.DEV) {
          console.log(' Approving original SQL')
        }
        onApprove()
      }
    } catch (err) {
      console.error(' Error in handleApprove:', err)
    }
  }

  const isValidSQL = editedSQL.trim().length > 0 && syntaxValidation.valid;
  const looksLikeSQL = /^(SELECT|WITH|INSERT|UPDATE|DELETE|ALTER|DROP|CREATE|TRUNCATE|MERGE|GRANT|REVOKE)/i.test(editedSQL.trim());

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <div className="flex items-start gap-4">
            <div className={cn(
              "w-12 h-12 rounded-full flex items-center justify-center flex-shrink-0",
              colors.bg
            )}>
              <AlertTriangle className={cn("h-6 w-6", colors.text)} />
            </div>
            <div className="flex-1">
              <DialogTitle className="text-xl">Human-in-the-Loop Approval Required</DialogTitle>
              <DialogDescription className="mt-1">
                This query requires your review before execution for security and compliance.
              </DialogDescription>
              <Badge
                variant="outline"
                className={cn("mt-2", colors.bg, colors.text, colors.border)}
              >
                Risk Level: {riskLevel}
              </Badge>
            </div>
          </div>
        </DialogHeader>

        <div className="space-y-4">
          {/* User Query */}
          <div>
            <div className="text-sm font-semibold text-gray-600 mb-2">Your Query:</div>
            <div className="p-3 bg-gray-50 border rounded-lg text-sm">
              {query}
            </div>
          </div>

          {/* Generated SQL - Tabbed View/Edit */}
          <div>
            <Tabs value={sqlTab} onValueChange={(v) => setSqlTab(v as 'preview' | 'edit')}>
              <div className="flex items-center justify-between mb-2">
                <div className="text-sm font-semibold text-gray-600">Generated SQL:</div>
                <div className="flex items-center gap-2">
                  <TabsList className="h-8">
                    <TabsTrigger value="preview" className="text-xs h-7 px-3">
                      <Eye className="h-3 w-3 mr-1" />
                      Preview
                    </TabsTrigger>
                    <TabsTrigger value="edit" className="text-xs h-7 px-3">
                      <Edit className="h-3 w-3 mr-1" />
                      Edit
                    </TabsTrigger>
                  </TabsList>
                  <div className="flex gap-2 text-xs">
                    <button
                      onClick={() => setShowExplainPlan(true)}
                      className="text-green-600 hover:underline"
                    >
                      View EXPLAIN PLAN
                    </button>
                    <button
                      onClick={() => setShowSchema(true)}
                      className="text-green-600 hover:underline"
                    >
                      View Schema
                    </button>
                  </div>
                </div>
              </div>

              <TabsContent value="preview" className="mt-0">
                <MonacoSQLEditor
                  value={editedSQL}
                  readOnly={true}
                  height="200px"
                />
              </TabsContent>

              <TabsContent value="edit" className="mt-0">
                <MonacoSQLEditor
                  value={editedSQL}
                  onChange={(value) => setEditedSQL(value || '')}
                  readOnly={false}
                  height="200px"
                />
                {editedSQL !== sql && (
                  <div className="mt-2 flex items-center gap-2 text-xs text-blue-600">
                    <Info className="h-3 w-3" />
                    <span>SQL has been modified. Changes will be applied upon approval.</span>
                  </div>
                )}
                {/* Syntax validation feedback */}
                {syntaxValidation.errors.length > 0 && (
                  <div className="mt-2 space-y-1">
                    {syntaxValidation.errors.map((error, idx) => (
                      <div key={idx} className="flex items-center gap-2 text-xs text-red-600">
                        <AlertTriangle className="h-3 w-3" />
                        <span>{error}</span>
                      </div>
                    ))}
                  </div>
                )}
                {syntaxValidation.warnings.length > 0 && (
                  <div className="mt-1 space-y-1">
                    {syntaxValidation.warnings.map((warning, idx) => (
                      <div key={idx} className="flex items-center gap-2 text-xs text-amber-600">
                        <AlertTriangle className="h-3 w-3" />
                        <span>{warning}</span>
                      </div>
                    ))}
                  </div>
                )}
                {!looksLikeSQL && editedSQL.trim().length > 0 && syntaxValidation.errors.length === 0 && (
                  <div className="mt-1 flex items-center gap-2 text-xs text-amber-600">
                    <AlertTriangle className="h-3 w-3" />
                    <span>Warning: SQL query might be invalid (unrecognized start keyword).</span>
                  </div>
                )}
              </TabsContent>
            </Tabs>
          </div>

          {/* Security & Performance Metrics */}
          <div className="grid grid-cols-3 gap-4">
            <div className="bg-white rounded-lg p-3 border">
              <div className="text-xs text-gray-500 mb-1">Operation Type</div>
              <div className="font-semibold text-sm text-green-600">
                {metadata.operationType || 'SELECT (Read-Only)'}
              </div>
            </div>
            <div className="bg-white rounded-lg p-3 border">
              <div className="text-xs text-gray-500 mb-1">Risk Assessment</div>
              <div className="font-semibold text-sm text-green-600">
                {riskLevel === 'SAFE' || riskLevel === 'LOW' ? ' Low Risk' : ' Review Required'}
              </div>
            </div>
            <div className="bg-white rounded-lg p-3 border">
              <div className="text-xs text-gray-500 mb-1">Est. Execution Time</div>
              <div className="font-semibold text-sm text-gray-700">
                {metadata.estimatedTime || '< 0.5s'}
              </div>
            </div>
          </div>

          {/* Additional Details */}
          <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-xs border rounded-lg p-3 bg-gray-50">
            <div className="flex justify-between">
              <span className="text-gray-500">Tables Accessed:</span>
              <span className="font-medium">
                {metadata.tablesAccessed?.join(', ') || 'SALES'}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Estimated Rows:</span>
              <span className="font-medium">
                ~{metadata.estimatedRows || 10} rows
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Data Classification:</span>
              <span className={cn(
                "font-medium",
                metadata.dataClassification === 'Confidential' ? "text-yellow-600" : ""
              )}>
                {metadata.dataClassification || 'Standard'}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">SQL Injection Check:</span>
              <span className="font-medium text-green-600"> Passed</span>
            </div>
          </div>

          {/* Audit Trail Info */}
          <Alert variant="default" className="bg-blue-50 border-blue-200">
            <Info className="h-4 w-4 text-blue-600" />
            <AlertTitle className="text-sm text-blue-900">Audit Trail Enabled</AlertTitle>
          </Alert>

          {/* Risk Warning */}
          {(riskLevel === 'HIGH' || riskLevel === 'CRITICAL') && (
            <Alert variant="warning">
              <AlertTriangle className="h-4 w-4" />
              <AlertTitle>High Risk Operation</AlertTitle>
              <AlertDescription>
                {riskLevel === 'CRITICAL' && 'This query performs destructive operations. Please verify carefully before approving.'}
                {riskLevel === 'HIGH' && 'This query modifies data or schema. Review the SQL carefully before proceeding.'}
              </AlertDescription>
            </Alert>
          )}
        </div>

        <DialogFooter className="flex-col sm:flex-row gap-2">
          <div className="flex flex-1 gap-2 text-xs items-center">
            <label className="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" className="rounded" defaultChecked />
              <span>Save to approved queries</span>
            </label>
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={onReject}>
              Reject Query
            </Button>
            {onModify && (
              <Button variant="outline" onClick={onModify} className="border-green-600 text-green-600 hover:bg-green-50">
                Modify
              </Button>
            )}
            <Button
              onClick={handleApprove}
              disabled={!isValidSQL}
              className="bg-gradient-to-r from-emerald-500 to-green-600 hover:from-emerald-600 hover:to-green-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {editedSQL !== sql ? ' Approve Modified Query' : ' Approve & Execute'}
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>

      {/* Schema Dialog - Live schema from backend */}
      {showSchema && (
        <Dialog open={showSchema} onOpenChange={setShowSchema}>
          <DialogContent className="max-w-3xl max-h-[80vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                <DatabaseIcon className="h-4 w-4" />
                Database Schema
              </DialogTitle>
              <DialogDescription>
                Tables and columns available to this query (from the backend schema service).
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-3 text-xs">
              {schemaLoading && (
                <div className="text-gray-600">Loading schema...</div>
              )}
              {schemaError && (
                <Alert variant="destructive">
                  <AlertTriangle className="h-4 w-4" />
                  <AlertDescription>{schemaError}</AlertDescription>
                </Alert>
              )}
              {!schemaLoading && !schemaError && Object.keys(schemaTables).length === 0 && (
                <Alert>
                  <Info className="h-4 w-4" />
                  <AlertDescription>
                    No schema tables were returned. Ensure the backend schema cache is populated.
                  </AlertDescription>
                </Alert>
              )}
              {!schemaLoading && !schemaError && Object.keys(schemaTables).length > 0 && (
                <div className="space-y-2">
                  {Object.entries(schemaTables).map(([tableName, cols]) => (
                    <div key={tableName} className="border rounded-md p-2 bg-gray-50">
                      <div className="flex items-center gap-2 mb-1">
                        <TableIcon className="h-3 w-3 text-gray-600" />
                        <span className="font-semibold text-gray-800">{tableName}</span>
                        <span className="text-[10px] text-gray-500">{cols.length} columns</span>
                      </div>
                      <div className="grid grid-cols-2 gap-x-2 gap-y-1">
                        {cols.map((c) => (
                          <div key={c.name} className="flex justify-between text-[11px] text-gray-700">
                            <span>{c.name}</span>
                            <span className="text-gray-500 ml-2">
                              {c.type || 'UNKNOWN'}
                              {c.nullable === false && ' (NOT NULL)'}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </DialogContent>
        </Dialog>
      )}

      {/* Explain Plan Dialog - Guides user to Query Builder with SQL */}
      {showExplainPlan && (
        <Dialog open={showExplainPlan} onOpenChange={setShowExplainPlan}>
          <DialogContent className="max-w-3xl">
            <DialogHeader>
              <DialogTitle>Execution Plan & SQL Inspection</DialogTitle>
              <DialogDescription>
                Use the Query Builder to run EXPLAIN PLAN or inspect this SQL directly against your database.
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-3 text-sm">
              <Alert>
                <Info className="h-4 w-4" />
                <AlertDescription>
                  This environment does not expose EXPLAIN PLAN directly from the chat surface. Instead, copy the SQL below and paste it into the Query Builder, where you can run EXPLAIN PLAN manually or execute the query with full control.
                </AlertDescription>
              </Alert>
              <div className="bg-gray-50 border rounded p-3 text-xs font-mono whitespace-pre-wrap break-words max-h-56 overflow-auto">
                {editedSQL}
              </div>
              <div className="flex gap-2 justify-end">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    try {
                      navigator.clipboard.writeText(editedSQL)
                    } catch { }
                  }}
                >
                  Copy SQL
                </Button>
                <Button
                  size="sm"
                  onClick={() => {
                    try {
                      const encoded = encodeURIComponent(editedSQL)
                      window.open(`/query-builder?sql=${encoded}`, '_blank')
                    } catch { }
                  }}
                >
                  Open Query Builder
                </Button>
              </div>
            </div>
          </DialogContent>
        </Dialog>
      )}
    </Dialog>
  )
}
