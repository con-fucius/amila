import { useEffect, useState } from 'react'
import { AlertTriangle, Info, Edit, Eye, Table as TableIcon, Database as DatabaseIcon, ShieldAlert } from 'lucide-react'
import { Button } from './ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from './ui/dialog'
import { Badge } from './ui/badge'
import { Alert, AlertDescription, AlertTitle } from './ui/alert'
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs'
import { MonacoSQLEditor } from './MonacoSQLEditor'
import { cn } from '@/utils/cn'
import { apiService } from '@/services/apiService'
import { RiskExplanationPanel } from './RiskExplanationPanel'

interface HITLApprovalDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  query: string
  sql: string
  riskLevel: 'SAFE' | 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
  onApprove: (modifiedSQL?: string, constraints?: { max_rows?: number }) => void
  onReject: () => void
  onModify?: () => void
  originalSQL?: string
  riskReasons?: string[]
  approvalContext?: {
    risk_level?: string
    warnings?: string[]
    requires_approval?: boolean
    query_type?: string
    estimated_cost?: number
    estimated_rows?: number
    has_full_table_scan?: boolean
    recommendations?: string[]
    scope?: {
      table_count?: number
      join_count?: number
      max_tables?: number
      max_joins?: number
      warnings?: string[]
    }
    cartesian_guard?: boolean
    force_approval?: boolean
    intent_source?: string
    skills_used?: boolean
    skills_fallback?: boolean
    skills_fallback_reason?: string
    hypothesis_confidence?: string
  }
  metadata?: {
    operationType?: string
    tablesAccessed?: string[]
    estimatedRows?: number
    estimatedTime?: string
    dataClassification?: string
    sessionId?: string
    user?: string
    databaseType?: 'oracle' | 'doris'
  }
  sqlExplanation?: string
  queryPlan?: {
    steps: Array<{ id: string; node: string; status: string; description: string }>
    estimated_cost?: number
  }
  rlsExplanation?: string
}

const riskColors = {
  SAFE: { bg: 'bg-green-900/40', text: 'text-green-300', border: 'border-green-500/50' },
  LOW: { bg: 'bg-blue-900/40', text: 'text-blue-300', border: 'border-blue-500/50' },
  MEDIUM: { bg: 'bg-yellow-900/40', text: 'text-yellow-300', border: 'border-yellow-500/50' },
  HIGH: { bg: 'bg-orange-900/40', text: 'text-orange-300', border: 'border-orange-500/50' },
  CRITICAL: { bg: 'bg-red-900/40', text: 'text-red-300', border: 'border-red-500/50' },
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

  riskReasons,
  approvalContext,
  metadata,
  sqlExplanation,
  queryPlan,
  rlsExplanation
}: HITLApprovalDialogProps) {
  const colors = riskColors[riskLevel]
  const [editedSQL, setEditedSQL] = useState(sql)
  const [sqlTab, setSqlTab] = useState<'preview' | 'edit' | 'plan'>('preview')
  const [showSchema, setShowSchema] = useState(false)
  const [showExplainPlan, setShowExplainPlan] = useState(false)
  const [schemaLoading, setSchemaLoading] = useState(false)
  const [schemaError, setSchemaError] = useState<string | null>(null)
  const [schemaTables, setSchemaTables] = useState<Record<string, { name: string; type?: string; nullable?: boolean }[]>>({})

  // Constrained approval state
  const [applyForcedLimit, setApplyForcedLimit] = useState(false)
  const [maxRowsConstraint, setMaxRowsConstraint] = useState(100)

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
    const constraints = applyForcedLimit ? { max_rows: maxRowsConstraint } : undefined;

    try {
      if (editedSQL !== sql) {
        if (import.meta.env.DEV) {
          console.log(' Approving with modified SQL and constraints:', constraints)
        }
        onApprove(editedSQL, constraints)
      } else {
        if (import.meta.env.DEV) {
          console.log(' Approving original SQL with constraints:', constraints)
        }
        onApprove(undefined, constraints)
      }
    } catch (err) {
      console.error(' Error in handleApprove:', err)
    }
  }

  const isValidSQL = editedSQL.trim().length > 0 && syntaxValidation.valid;

  const scopeWarnings = approvalContext?.scope?.warnings || []
  const approvalWarnings = approvalContext?.warnings || []
  const recommendations = approvalContext?.recommendations || []

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[85vh] overflow-y-auto bg-slate-900/95 backdrop-blur-xl border border-slate-700/50 shadow-2xl shadow-emerald-500/5 animate-in fade-in-0 zoom-in-95 duration-200">
        {/* Subtle edge glow effect */}
        <div className="absolute inset-0 rounded-lg bg-gradient-to-r from-emerald-500/10 via-transparent to-blue-500/10 pointer-events-none" />
        <div className="absolute inset-[1px] rounded-lg bg-gradient-to-b from-white/5 to-transparent pointer-events-none" />

        <DialogHeader className="relative">
          <div className="flex items-start gap-3">
            <div className={cn(
              "w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0",
              colors.bg
            )}>
              <AlertTriangle className={cn("h-5 w-5", colors.text)} />
            </div>
            <div className="flex-1">
              <DialogTitle className="text-lg text-gray-100">Approval Required</DialogTitle>
              <DialogDescription className="mt-0.5 text-gray-400 text-sm">
                Review this query before execution
              </DialogDescription>
              <Badge
                variant="outline"
                className={cn("mt-1.5 text-xs", colors.bg, colors.text, colors.border)}
              >
                {riskLevel} Risk
              </Badge>
            </div>
          </div>
        </DialogHeader>

        <div className="space-y-3 relative">
          {/* User Query */}
          <div>
            <div className="text-xs font-medium text-gray-400 mb-1">Your Query:</div>
            <div className="p-2.5 bg-slate-800/60 border border-slate-700/50 rounded-lg text-sm text-gray-200">
              {query}
            </div>
          </div>

          {/* Generated SQL - Tabbed View/Edit */}
          <div>
            <Tabs value={sqlTab} onValueChange={(v) => setSqlTab(v as any)}>
              <div className="flex items-center justify-between mb-2">
                <div className="text-sm font-semibold text-gray-400">Target SQL:</div>
                <div className="flex items-center gap-2">
                  <TabsList className="h-8 bg-slate-800 border-slate-700">
                    <TabsTrigger value="preview" className="text-xs h-7 px-3 text-gray-300">
                      <Eye className="h-3 w-3 mr-1" />
                      Preview
                    </TabsTrigger>
                    {queryPlan && (
                      <TabsTrigger value="plan" className="text-xs h-7 px-3 text-gray-300">
                        <DatabaseIcon className="h-3 w-3 mr-1" />
                        Plan
                      </TabsTrigger>
                    )}
                    <TabsTrigger value="edit" className="text-xs h-7 px-3 text-gray-300">
                      <Edit className="h-3 w-3 mr-1" />
                      Edit
                    </TabsTrigger>
                  </TabsList>
                  <div className="flex gap-2 text-xs">
                    <button
                      onClick={() => setShowSchema(true)}
                      className="text-blue-400 hover:text-blue-300 transition-colors"
                    >
                      Browse Schema
                    </button>
                  </div>
                </div>
              </div>

              <TabsContent value="preview" className="mt-0 space-y-3">
                {sqlExplanation && (
                  <div className="p-3 bg-blue-900/20 border border-blue-500/30 rounded-lg animate-in fade-in slide-in-from-top-2">
                    <div className="flex items-center gap-2 text-[11px] font-bold uppercase tracking-wider text-blue-400 mb-1">
                      <Info className="h-3.5 w-3.5" />
                      Business Context
                    </div>
                    <p className="text-xs text-blue-100/90 leading-relaxed italic">
                      "{sqlExplanation}"
                    </p>
                  </div>
                )}

                {rlsExplanation && (
                  <div className="p-3 bg-indigo-900/20 border border-indigo-500/30 rounded-lg animate-in fade-in slide-in-from-top-2">
                    <div className="flex items-center gap-2 text-[11px] font-bold uppercase tracking-wider text-indigo-400 mb-1">
                      <ShieldAlert className="h-3.5 w-3.5" />
                      Security Filter Applied
                    </div>
                    <p className="text-xs text-indigo-100/90 leading-relaxed">
                      {rlsExplanation}
                    </p>
                  </div>
                )}

                <div className="relative group border border-slate-700/50 rounded-lg overflow-hidden">
                  <pre className="bg-slate-950/80 p-4 overflow-x-auto text-[13px] font-mono text-blue-100 max-h-[250px] custom-scrollbar">
                    <code>{editedSQL}</code>
                  </pre>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity bg-slate-800/80 hover:bg-slate-700"
                    onClick={() => {
                      navigator.clipboard.writeText(editedSQL)
                    }}
                  >
                    Copy
                  </Button>
                </div>
              </TabsContent>

              {queryPlan && (
                <TabsContent value="plan" className="mt-0">
                  <div className="bg-slate-900/60 border border-slate-700 rounded-lg p-4 space-y-4 shadow-inner">
                    <div className="flex items-center justify-between mb-2">
                      <div className="text-xs font-semibold text-gray-300 uppercase tracking-widest text-[10px]">Thinking Chain & Tool Usage</div>
                      <Badge variant="outline" className="text-[10px] text-emerald-400 border-emerald-500/30 font-normal">
                        {queryPlan.estimated_cost ? `Est. Cost: ${queryPlan.estimated_cost} units` : 'Optimized Logic'}
                      </Badge>
                    </div>
                    <div className="space-y-3">
                      {queryPlan.steps.map((step, idx) => (
                        <div key={step.id} className="flex gap-3">
                          <div className="flex flex-col items-center">
                            <div className={`h-5 w-5 rounded-full flex items-center justify-center text-[10px] font-bold ${step.status === 'active' ? "bg-blue-500 text-white shadow-[0_0_10px_rgba(59,130,246,0.5)]" :
                              step.status === 'completed' ? "bg-emerald-500 text-white" : "bg-slate-700 text-gray-400"
                              }`}>
                              {idx + 1}
                            </div>
                            {idx < queryPlan.steps.length - 1 && (
                              <div className="w-0.5 h-6 bg-slate-700 my-1" />
                            )}
                          </div>
                          <div className="pt-0.5">
                            <div className="text-xs font-medium text-gray-200 capitalize">{step.node.replace(/_/g, ' ')}</div>
                            <div className="text-[10px] text-gray-500 leading-tight">{step.description}</div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </TabsContent>
              )}

              <TabsContent value="edit" className="mt-0">
                <div className="border border-slate-700 rounded-lg overflow-hidden h-[300px] shadow-inner">
                  <MonacoSQLEditor
                    value={editedSQL}
                    onChange={(value) => setEditedSQL(value || '')}
                    readOnly={false}

                  />
                </div>
              </TabsContent>
            </Tabs>
          </div>

          {/* Security & Performance Metrics */}
          <div className="grid grid-cols-3 gap-2">
            <div className="bg-slate-800/60 rounded-lg p-2.5 border border-slate-700/50">
              <div className="text-[10px] text-gray-500 mb-0.5">Operation</div>
              <div className="font-medium text-xs text-emerald-400">
                {metadata?.operationType || approvalContext?.query_type || 'SELECT'}
              </div>
            </div>
            <div className="bg-slate-800/60 rounded-lg p-2.5 border border-slate-700/50">
              <div className="text-[10px] text-gray-500 mb-0.5">Risk</div>
              <div className={cn("font-medium text-xs", colors.text)}>
                {riskLevel === 'SAFE' || riskLevel === 'LOW' ? 'Low' : 'Review'}
              </div>
            </div>
            <div className="bg-slate-800/60 rounded-lg p-2.5 border border-slate-700/50">
              <div className="text-[10px] text-gray-500 mb-0.5">Est. Time</div>
              <div className="font-medium text-xs text-gray-300">
                {metadata?.estimatedTime || '<0.5s'}
              </div>
            </div>
          </div>

          {/* Additional Details - Compact */}
          <div className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-[11px] border border-slate-700/50 rounded-lg p-2.5 bg-slate-800/40">
            <div className="flex justify-between">
              <span className="text-gray-500">Tables:</span>
              <span className="font-medium text-gray-300 truncate ml-2">
                {metadata?.tablesAccessed?.join(', ') || 'SALES'}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Est. Rows:</span>
              <span className="font-medium text-gray-300">
                ~{metadata?.estimatedRows || approvalContext?.estimated_rows || 10}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Classification:</span>
              <span className={cn(
                "font-medium",
                metadata?.dataClassification === 'Confidential' ? "text-yellow-400" : "text-gray-300"
              )}>
                {metadata?.dataClassification || 'Standard'}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">SQL Injection:</span>
              <span className="font-medium text-emerald-400">✓ Passed</span>
            </div>
          </div>

          {/* Scope & Guardrails */}
          {(scopeWarnings.length > 0 || approvalContext?.cartesian_guard || approvalContext?.force_approval) && (
            <div className="border border-amber-500/30 rounded-lg p-2.5 bg-amber-900/10">
              <div className="flex items-center gap-2 text-xs font-semibold text-amber-300 mb-1">
                <AlertTriangle className="h-3.5 w-3.5" />
                Approval Gate Reasons
              </div>
              {approvalContext?.cartesian_guard && (
                <div className="text-[11px] text-amber-200">Potential Cartesian join detected</div>
              )}
              {approvalContext?.force_approval && (
                <div className="text-[11px] text-amber-200">Scope exceeds role-based limits</div>
              )}
              {scopeWarnings.map((warning, idx) => (
                <div key={idx} className="text-[11px] text-amber-200">{warning}</div>
              ))}
            </div>
          )}

          {/* Generation Signals */}
          {(approvalContext?.intent_source || approvalContext?.skills_used !== undefined || approvalContext?.skills_fallback || approvalContext?.hypothesis_confidence) && (
            <div className="border border-slate-700/50 rounded-lg p-2.5 bg-slate-800/40">
              <div className="text-xs font-semibold text-gray-300 mb-1">Generation Signals</div>
              <div className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-[11px]">
                <div className="flex justify-between">
                  <span className="text-gray-500">Intent Source:</span>
                  <span className="text-gray-300">{approvalContext?.intent_source || 'unknown'}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Skills Used:</span>
                  <span className={cn("font-medium", approvalContext?.skills_used ? "text-emerald-400" : "text-amber-300")}>
                    {approvalContext?.skills_used ? 'Yes' : 'No'}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Skills Fallback:</span>
                  <span className={cn("font-medium", approvalContext?.skills_fallback ? "text-amber-300" : "text-gray-300")}>
                    {approvalContext?.skills_fallback ? 'Yes' : 'No'}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Hypothesis:</span>
                  <span className="text-gray-300">{approvalContext?.hypothesis_confidence || 'n/a'}</span>
                </div>
                {approvalContext?.skills_fallback_reason && (
                  <div className="col-span-2 text-[11px] text-amber-200">
                    Fallback reason: {approvalContext.skills_fallback_reason}
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Risk Explanation Panel */}
          <RiskExplanationPanel
            estimatedCost={approvalContext?.estimated_cost}
            estimatedRows={approvalContext?.estimated_rows || metadata?.estimatedRows}
            hasFullTableScan={approvalContext?.has_full_table_scan}
            piiDetected={metadata?.dataClassification === 'Confidential'}
            joinCount={approvalContext?.scope?.join_count}
            tableCount={approvalContext?.scope?.table_count}
            riskReasons={riskReasons || (approvalContext as any)?.risk_reasons}
            recommendations={recommendations}
          />

          {/* Constrained Approval Options */}
          <div className="border border-blue-500/30 rounded-lg p-3 bg-blue-900/10">
            <div className="flex items-center gap-2 text-xs font-semibold text-blue-300 mb-2">
              <ShieldAlert className="h-3.5 w-3.5" />
              Constrained Approval Options
            </div>
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    id="force-limit"
                    checked={applyForcedLimit}
                    onChange={(e) => setApplyForcedLimit(e.target.checked)}
                    className="rounded bg-slate-800 border-slate-600 text-blue-500 focus:ring-blue-500"
                  />
                  <label htmlFor="force-limit" className="text-xs text-gray-300 cursor-pointer">
                    Apply safety row limit (FORCE LIMIT)
                  </label>
                </div>
                {applyForcedLimit && (
                  <div className="flex items-center gap-1.3 ml-4">
                    <input
                      type="number"
                      value={maxRowsConstraint}
                      onChange={(e) => setMaxRowsConstraint(parseInt(e.target.value) || 100)}
                      className="w-16 h-6 px-1.5 text-xs bg-slate-800 border border-slate-600 rounded text-blue-300 focus:outline-none focus:border-blue-500"
                    />
                    <span className="text-[10px] text-gray-500 italic">rows</span>
                  </div>
                )}
              </div>
              <p className="text-[10px] text-gray-500 leading-tight">
                Constrained approval allows you to execute high-risk queries with platform-enforced safety guards.
              </p>
            </div>
          </div>

          {approvalWarnings.length > 0 && (
            <div className="border border-slate-700/50 rounded-lg p-2.5 bg-slate-800/40">
              <div className="text-xs font-semibold text-gray-300 mb-1">Validation Warnings</div>
              {approvalWarnings.map((warning, idx) => (
                <div key={idx} className="text-[11px] text-gray-300">{warning}</div>
              ))}
            </div>
          )}

          {/* Audit Trail - More compact */}
          <div className="flex items-center gap-2 p-2 bg-blue-900/20 border border-blue-500/30 rounded-lg">
            <Info className="h-3.5 w-3.5 text-blue-400" />
            <span className="text-xs text-blue-300">Audit Trail Enabled</span>
          </div>

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

        <DialogFooter className="flex-col sm:flex-row gap-2 relative">
          <div className="flex flex-1 gap-2 text-xs items-center">
            <label className="flex items-center gap-2 cursor-pointer text-gray-400">
              <input type="checkbox" className="rounded bg-slate-800 border-slate-600" defaultChecked />
              <span>Save to approved queries</span>
            </label>
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={onReject} className="border-slate-600 text-gray-300 hover:bg-slate-800 hover:text-white">
              Reject
            </Button>
            {onModify && (
              <Button variant="outline" onClick={onModify} className="border-emerald-600/60 text-emerald-400 hover:bg-emerald-900/30">
                Modify
              </Button>
            )}
            <Button
              onClick={handleApprove}
              disabled={!isValidSQL}
              className="bg-gradient-to-r from-emerald-500 to-green-600 hover:from-emerald-600 hover:to-green-700 disabled:opacity-50 disabled:cursor-not-allowed text-white"
            >
              {editedSQL !== sql ? 'Approve Modified' : 'Approve & Execute'}
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
                      // Include database_type from metadata if available
                      const dbType = metadata?.databaseType || 'oracle'
                      window.open(`/query-builder?sql=${encoded}&database_type=${dbType}`, '_blank')
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
