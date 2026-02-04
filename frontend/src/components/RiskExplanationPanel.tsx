import { ShieldAlert, Table2, Users, AlertTriangle, Info, CheckCircle2 } from 'lucide-react'
import { Badge } from './ui/badge'
import { cn } from '@/utils/cn'

interface RiskFactor {
  name: string
  level: 'low' | 'medium' | 'high' | 'critical'
  value: string | number
  description: string
  recommendation?: string
}

interface RiskExplanationPanelProps {
  estimatedCost?: number
  estimatedRows?: number
  hasFullTableScan?: boolean
  piiDetected?: boolean
  joinCount?: number
  tableCount?: number
  riskFactors?: RiskFactor[]
  riskReasons?: string[]
  recommendations?: string[]
  className?: string
}

const riskLevelColors = {
  low: 'text-emerald-400 bg-emerald-500/10 border-emerald-500/30',
  medium: 'text-yellow-400 bg-yellow-500/10 border-yellow-500/30',
  high: 'text-orange-400 bg-orange-500/10 border-orange-500/30',
  critical: 'text-red-400 bg-red-500/10 border-red-500/30',
}

const riskLevelIcons = {
  low: CheckCircle2,
  medium: Info,
  high: AlertTriangle,
  critical: ShieldAlert,
}

export function RiskExplanationPanel({
  estimatedCost,
  estimatedRows,
  hasFullTableScan,
  piiDetected,
  joinCount,
  tableCount,
  riskFactors = [],
  riskReasons = [],
  recommendations = [],
  className,
}: RiskExplanationPanelProps) {
  // Calculate overall risk score
  const calculateRiskScore = (): { score: number; level: 'low' | 'medium' | 'high' | 'critical' } => {
    let score = 0

    // Cost factor (0-30 points)
    if (estimatedCost) {
      if (estimatedCost > 1000) score += 30
      else if (estimatedCost > 500) score += 20
      else if (estimatedCost > 100) score += 10
    }

    // Row count factor (0-25 points)
    if (estimatedRows) {
      if (estimatedRows > 1000000) score += 25
      else if (estimatedRows > 100000) score += 15
      else if (estimatedRows > 10000) score += 5
    }

    // Full table scan (20 points)
    if (hasFullTableScan) score += 20

    // PII detection (25 points)
    if (piiDetected) score += 25

    // Join complexity (0-20 points)
    if (joinCount) {
      if (joinCount > 5) score += 20
      else if (joinCount > 3) score += 10
      else if (joinCount > 1) score += 5
    }

    // Determine level
    let level: 'low' | 'medium' | 'high' | 'critical' = 'low'
    if (score >= 70) level = 'critical'
    else if (score >= 50) level = 'high'
    else if (score >= 25) level = 'medium'

    return { score: Math.min(score, 100), level }
  }

  const { score, level } = calculateRiskScore()
  // const RiskIcon = riskLevelIcons[level]

  // Auto-generate risk factors if not provided
  const displayRiskFactors: RiskFactor[] =
    riskFactors.length > 0
      ? riskFactors
      : [
        ...(estimatedCost !== undefined
          ? [
            {
              name: 'Estimated Cost',
              level: (estimatedCost > 500 ? 'high' : estimatedCost > 100 ? 'medium' : 'low') as 'high' | 'medium' | 'low',
              value: `$${estimatedCost.toFixed(2)}`,
              description:
                estimatedCost > 500
                  ? 'High computational cost'
                  : estimatedCost > 100
                    ? 'Moderate resource usage'
                    : 'Low cost query',
              recommendation:
                estimatedCost > 500 ? 'Consider adding filters or reducing date range' : undefined,
            },
          ]
          : []),
        ...(estimatedRows !== undefined
          ? [
            {
              name: 'Estimated Rows',
              level:
                (estimatedRows > 100000 ? 'high' : estimatedRows > 10000 ? 'medium' : 'low') as 'high' | 'medium' | 'low',
              value: estimatedRows.toLocaleString(),
              description:
                estimatedRows > 100000
                  ? 'Large result set'
                  : estimatedRows > 10000
                    ? 'Moderate result size'
                    : 'Small result set',
              recommendation: estimatedRows > 100000 ? 'Consider adding LIMIT or pagination' : undefined,
            },
          ]
          : []),
        ...(hasFullTableScan
          ? [
            {
              name: 'Full Table Scan',
              level: 'high' as const,
              value: 'Yes',
              description: 'Query scans entire table without index',
              recommendation: 'Add indexes or filter conditions',
            },
          ]
          : []),
        ...(piiDetected
          ? [
            {
              name: 'PII Data',
              level: 'critical' as const,
              value: 'Detected',
              description: 'Personal identifiable information may be accessed',
              recommendation: 'Verify data access permissions',
            },
          ]
          : []),
        ...(joinCount !== undefined && joinCount > 3
          ? [
            {
              name: 'Join Complexity',
              level: (joinCount > 5 ? 'high' : 'medium') as 'high' | 'medium',
              value: `${joinCount} joins`,
              description: `Query joins ${joinCount} tables`,
              recommendation: joinCount > 5 ? 'Consider denormalization or query simplification' : undefined,
            },
          ]
          : []),
      ]

  return (
    <div className={cn('rounded-lg border border-slate-700/50 bg-slate-800/40 overflow-hidden', className)}>
      {/* Header with Overall Risk */}
      <div className="p-3 border-b border-slate-700/50">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <ShieldAlert className={cn('h-5 w-5', riskLevelColors[level].split(' ')[0])} />
            <span className="text-sm font-medium text-slate-200">Risk Assessment</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-24 h-2 bg-slate-700 rounded-full overflow-hidden">
              <div
                className={cn(
                  'h-full rounded-full transition-all',
                  level === 'critical'
                    ? 'bg-red-500 w-full'
                    : level === 'high'
                      ? 'bg-orange-500'
                      : level === 'medium'
                        ? 'bg-yellow-500'
                        : 'bg-emerald-500'
                )}
                style={{ width: `${score}%` }}
              />
            </div>
            <Badge variant="outline" className={cn('text-xs', riskLevelColors[level])}>
              {level.toUpperCase()}
            </Badge>
          </div>
        </div>
        <p className="text-xs text-slate-500 mt-1">
          Risk score: {score}/100 • Based on cost, data volume, and security factors
        </p>
      </div>

      {/* Specific Risk Reasons (from Backend) */}
      {riskReasons && riskReasons.length > 0 && (
        <div className="p-3 bg-amber-500/5 border-b border-slate-700/50">
          <div className="flex items-center gap-2 mb-2 text-amber-400">
            <AlertTriangle className="h-4 w-4" />
            <span className="text-xs font-semibold uppercase tracking-wider">Platform Guardrails</span>
          </div>
          <ul className="space-y-1">
            {riskReasons.map((reason, idx) => (
              <li key={idx} className="text-xs text-amber-200/80 flex items-start gap-1.5">
                <span className="mt-1 w-1 h-1 rounded-full bg-amber-500 flex-shrink-0" />
                {reason}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Risk Factors Grid */}
      <div className="p-3 space-y-2">
        {displayRiskFactors.length > 0 ? (
          displayRiskFactors.map((factor, idx) => {
            const FactorIcon = riskLevelIcons[factor.level]
            return (
              <div
                key={idx}
                className="flex items-start gap-3 p-2.5 rounded-lg bg-slate-700/30 hover:bg-slate-700/50 transition-colors"
              >
                <div
                  className={cn(
                    'p-1.5 rounded-md flex-shrink-0',
                    riskLevelColors[factor.level].split(' ').slice(1).join(' ')
                  )}
                >
                  <FactorIcon className={cn('h-4 w-4', riskLevelColors[factor.level].split(' ')[0])} />
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-sm font-medium text-slate-200">{factor.name}</span>
                    <Badge variant="outline" className={cn('text-[10px]', riskLevelColors[factor.level])}>
                      {factor.level}
                    </Badge>
                  </div>
                  <div className="text-sm text-slate-300 mt-0.5">{factor.value}</div>
                  <p className="text-xs text-slate-500 mt-0.5">{factor.description}</p>
                  {factor.recommendation && (
                    <div className="flex items-start gap-1.5 mt-1.5 text-xs">
                      <Info className="h-3 w-3 text-amber-400 flex-shrink-0 mt-0.5" />
                      <span className="text-amber-300/80">{factor.recommendation}</span>
                    </div>
                  )}
                </div>
              </div>
            )
          })
        ) : (
          <div className="flex items-center justify-center py-6 text-slate-500">
            <CheckCircle2 className="h-4 w-4 mr-2 text-emerald-400" />
            <span className="text-sm">No significant risk factors detected</span>
          </div>
        )}
      </div>

      {/* Recommendations */}
      {recommendations.length > 0 && (
        <div className="p-3 border-t border-slate-700/50 bg-slate-800/60">
          <div className="flex items-center gap-2 mb-2">
            <Info className="h-4 w-4 text-blue-400" />
            <span className="text-sm font-medium text-slate-200">Recommendations</span>
          </div>
          <ul className="space-y-1.5">
            {recommendations.map((rec, idx) => (
              <li key={idx} className="flex items-start gap-2 text-xs text-slate-400">
                <span className="text-blue-400 mt-0.5">•</span>
                {rec}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Quick Stats */}
      {(tableCount || joinCount) && (
        <div className="grid grid-cols-2 gap-2 p-3 border-t border-slate-700/50">
          {tableCount !== undefined && (
            <div className="flex items-center gap-2 p-2 rounded bg-slate-700/30">
              <Table2 className="h-4 w-4 text-slate-500" />
              <div>
                <div className="text-[10px] text-slate-500">Tables</div>
                <div className="text-sm font-medium text-slate-300">{tableCount}</div>
              </div>
            </div>
          )}
          {joinCount !== undefined && (
            <div className="flex items-center gap-2 p-2 rounded bg-slate-700/30">
              <Users className="h-4 w-4 text-slate-500" />
              <div>
                <div className="text-[10px] text-slate-500">Joins</div>
                <div className="text-sm font-medium text-slate-300">{joinCount}</div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
