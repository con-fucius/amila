/**
 * Budget Forecasting Component
 * Displays budget forecasts, cost anomalies, alerts, and optimization recommendations.
 */

import { useEffect, useState, useCallback } from 'react'
import {
  TrendingUp,
  TrendingDown,
  Minus,
  AlertTriangle,
  AlertCircle,
  CheckCircle,
  DollarSign,
  Calendar,
  BarChart3,
  Lightbulb,
  RefreshCw,
  ChevronDown,
  ChevronUp
} from 'lucide-react'
import { cn } from '@/utils/cn'
import { apiService } from '@/services/apiService'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Progress } from '@/components/ui/progress'

// Types
interface BudgetForecast {
  current_period: string
  current_usage: number
  forecasted_usage: number
  budget_limit: number
  projected_overrun: number | null
  confidence_interval_low: number
  confidence_interval_high: number
  days_remaining: number
  trend_direction: string
  daily_average: number
  recommended_daily_budget: number
}

interface CostAnomaly {
  date: string
  cost: number
  expected_cost: number
  deviation_percentage: number
  severity: string
  description: string
}

interface BudgetAlert {
  alert_level: string
  message: string
  current_usage: number
  budget_limit: number
  percentage_used: number
  recommended_action: string
  triggered_at: string
}

interface OptimizationRecommendation {
  type: string
  priority: string
  message: string
  details: string
  potential_savings: string
}

interface BudgetData {
  forecast: BudgetForecast | null
  anomalies: CostAnomaly[]
  alerts: BudgetAlert[]
  recommendations: OptimizationRecommendation[]
}

type TabType = 'overview' | 'anomalies' | 'alerts' | 'recommendations'

export function BudgetForecasting() {
  const [data, setData] = useState<BudgetData>({
    forecast: null,
    anomalies: [],
    alerts: [],
    recommendations: []
  })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<TabType>('overview')
  const [expandedAnomalies, setExpandedAnomalies] = useState<Set<number>>(new Set())

  const fetchBudgetData = useCallback(async () => {
    setLoading(true)
    setError(null)

    try {
      const [forecast, anomalies, alerts, recommendations] = await Promise.all([
        apiService.getBudgetForecast(),
        apiService.getCostAnomalies(30),
        apiService.getBudgetAlerts(),
        apiService.getCostOptimizationRecommendations()
      ])

      setData({
        forecast,
        anomalies: anomalies || [],
        alerts: alerts || [],
        recommendations: recommendations || []
      })
    } catch (err: any) {
      console.error('Failed to fetch budget data:', err)
      setError(err.message || 'Failed to load budget data')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchBudgetData()
  }, [fetchBudgetData])

  // Helper functions
  const getTrendIcon = (direction: string) => {
    switch (direction) {
      case 'increasing':
        return <TrendingUp className="w-4 h-4 text-red-500" />
      case 'decreasing':
        return <TrendingDown className="w-4 h-4 text-emerald-500" />
      default:
        return <Minus className="w-4 h-4 text-gray-500" />
    }
  }

  const getAlertIcon = (level: string) => {
    switch (level) {
      case 'critical':
        return <AlertCircle className="w-5 h-5 text-red-500" />
      case 'warning':
        return <AlertTriangle className="w-5 h-5 text-amber-500" />
      default:
        return <CheckCircle className="w-5 h-5 text-blue-500" />
    }
  }

  const getAlertBadgeVariant = (level: string): 'default' | 'secondary' | 'destructive' | 'outline' => {
    switch (level) {
      case 'critical':
        return 'destructive'
      case 'warning':
        return 'default'
      default:
        return 'secondary'
    }
  }

  const getPriorityColor = (priority: string) => {
    switch (priority) {
      case 'critical':
        return 'text-red-600 bg-red-50 dark:bg-red-900/20'
      case 'high':
        return 'text-orange-600 bg-orange-50 dark:bg-orange-900/20'
      case 'medium':
        return 'text-amber-600 bg-amber-50 dark:bg-amber-900/20'
      default:
        return 'text-blue-600 bg-blue-50 dark:bg-blue-900/20'
    }
  }

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
      maximumFractionDigits: 4
    }).format(amount)
  }

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric'
    })
  }

  // Loading state
  if (loading) {
    return (
      <Card className="w-full">
        <CardContent className="p-8">
          <div className="flex items-center justify-center space-x-2">
            <RefreshCw className="w-5 h-5 animate-spin text-gray-400" />
            <span className="text-gray-600 dark:text-gray-400">Loading budget data...</span>
          </div>
        </CardContent>
      </Card>
    )
  }

  // Error state
  if (error) {
    return (
      <Card className="w-full">
        <CardContent className="p-8">
          <div className="text-center space-y-4">
            <AlertCircle className="w-12 h-12 text-red-500 mx-auto" />
            <p className="text-red-600 dark:text-red-400">{error}</p>
            <Button onClick={fetchBudgetData} variant="outline" size="sm">
              <RefreshCw className="w-4 h-4 mr-2" />
              Retry
            </Button>
          </div>
        </CardContent>
      </Card>
    )
  }

  const { forecast, anomalies, alerts, recommendations } = data
  const percentageUsed = forecast ? (forecast.current_usage / forecast.budget_limit) * 100 : 0
  const hasOverrun = forecast?.projected_overrun !== null && forecast?.projected_overrun !== undefined

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Budget Forecasting</h2>
          <p className="text-gray-600 dark:text-gray-400">
            Monitor your query spending and optimize costs
          </p>
        </div>
        <Button onClick={fetchBudgetData} variant="outline" size="sm">
          <RefreshCw className="w-4 h-4 mr-2" />
          Refresh
        </Button>
      </div>

      {/* Budget Overview Card */}
      {forecast && (
        <Card className={cn(
          "border-l-4",
          hasOverrun ? "border-l-red-500" : percentageUsed >= 80 ? "border-l-amber-500" : "border-l-emerald-500"
        )}>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <DollarSign className="w-5 h-5" />
              Budget Overview
              <Badge variant={hasOverrun ? 'destructive' : percentageUsed >= 80 ? 'default' : 'secondary'}>
                {forecast.current_period}
              </Badge>
            </CardTitle>
            <CardDescription>
              {hasOverrun
                ? `Projected overrun of ${formatCurrency(forecast.projected_overrun!)}`
                : `${Math.round(percentageUsed)}% of budget used`
              }
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-6">
            {/* Progress Bar */}
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span className="text-gray-600 dark:text-gray-400">Budget Usage</span>
                <span className={cn(
                  "font-medium",
                  hasOverrun ? "text-red-600" : percentageUsed >= 80 ? "text-amber-600" : "text-blue-600"
                )}>
                  {formatCurrency(forecast.current_usage)} / {formatCurrency(forecast.budget_limit)}
                </span>
              </div>
              <Progress
                value={Math.min(percentageUsed, 100)}
                className={cn(
                  "h-2",
                  hasOverrun ? "[&>div]:bg-red-500" : percentageUsed >= 80 ? "[&>div]:bg-amber-500" : "[&>div]:bg-blue-500"
                )}
              />
              {hasOverrun && (
                <p className="text-sm text-red-600 dark:text-red-400">
                  Projected to exceed budget by {formatCurrency(forecast.projected_overrun!)}
                </p>
              )}
            </div>

            {/* Key Metrics Grid */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div className="bg-gray-50 dark:bg-slate-800 p-4 rounded-lg">
                <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
                  <Calendar className="w-4 h-4" />
                  <span className="text-xs tracking-wider">Days remaining</span>
                </div>
                <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                  {forecast.days_remaining}
                </p>
              </div>

              <div className="bg-gray-50 dark:bg-slate-800 p-4 rounded-lg">
                <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
                  <BarChart3 className="w-4 h-4" />
                  <span className="text-xs tracking-wider">Daily average</span>
                </div>
                <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                  {formatCurrency(forecast.daily_average)}
                </p>
              </div>

              <div className="bg-gray-50 dark:bg-slate-800 p-4 rounded-lg">
                <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
                  {getTrendIcon(forecast.trend_direction)}
                  <span className="text-xs tracking-wider">Trend</span>
                </div>
                <p className="text-lg font-bold text-gray-900 dark:text-gray-100 capitalize">
                  {forecast.trend_direction}
                </p>
              </div>

              <div className="bg-gray-50 dark:bg-slate-800 p-4 rounded-lg">
                <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
                  <DollarSign className="w-4 h-4" />
                  <span className="text-xs tracking-wider">Recommended daily</span>
                </div>
                <p className="text-lg font-bold text-blue-600 dark:text-blue-400">
                  {formatCurrency(forecast.recommended_daily_budget)}
                </p>
              </div>
            </div>

            {/* Forecast Chart */}
            <div className="bg-gray-50 dark:bg-slate-800 p-4 rounded-lg">
              <h4 className="text-sm font-medium text-gray-900 dark:text-gray-100 mb-3">30-Day Forecast</h4>
              <div className="relative h-24 flex items-end gap-1">
                {/* Current usage bar */}
                <div
                  className="flex-1 bg-blue-500 rounded-t"
                  style={{ height: `${(forecast.current_usage / forecast.forecasted_usage) * 100}%` }}
                  title={`Current: ${formatCurrency(forecast.current_usage)}`}
                />
                {/* Projected usage bar */}
                <div
                  className={cn(
                    "flex-1 rounded-t",
                    hasOverrun ? "bg-red-500" : "bg-blue-500"
                  )}
                  style={{ height: '100%' }}
                  title={`Forecasted: ${formatCurrency(forecast.forecasted_usage)}`}
                />
              </div>
              <div className="flex justify-between mt-2 text-xs text-gray-600 dark:text-gray-400">
                <span>Current: {formatCurrency(forecast.current_usage)}</span>
                <span>Forecasted: {formatCurrency(forecast.forecasted_usage)}</span>
              </div>
              <div className="mt-2 text-xs text-gray-500 dark:text-gray-400">
                Confidence interval: {formatCurrency(forecast.confidence_interval_low)} - {formatCurrency(forecast.confidence_interval_high)}
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Tabs */}
      <div className="border-b border-gray-200 dark:border-slate-700">
        <nav className="flex gap-4">
          {[
            { key: 'overview', label: 'Overview', count: alerts.length },
            { key: 'anomalies', label: 'Anomalies', count: anomalies.length },
            { key: 'alerts', label: 'Alerts', count: alerts.length },
            { key: 'recommendations', label: 'Recommendations', count: recommendations.length }
          ].map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key as TabType)}
              className={cn(
                "pb-2 px-1 text-sm font-medium border-b-2 transition-colors",
                activeTab === tab.key
                  ? "border-blue-500 text-blue-600 dark:text-blue-400"
                  : "border-transparent text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200"
              )}
            >
              {tab.label}
              {tab.count > 0 && (
                <Badge variant="secondary" className="ml-2 text-xs">
                  {tab.count}
                </Badge>
              )}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab Content */}
      <div className="space-y-4">
        {/* Overview Tab */}
        {activeTab === 'overview' && (
          <div className="grid md:grid-cols-2 gap-4">
            {/* Quick Stats */}
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Quick Stats</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex justify-between items-center py-2 border-b border-gray-100 dark:border-slate-800">
                  <span className="text-gray-600 dark:text-gray-400">Active Alerts</span>
                  <Badge variant={alerts.length > 0 ? 'destructive' : 'secondary'}>
                    {alerts.length}
                  </Badge>
                </div>
                <div className="flex justify-between items-center py-2 border-b border-gray-100 dark:border-slate-800">
                  <span className="text-gray-600 dark:text-gray-400">Detected Anomalies</span>
                  <Badge variant={anomalies.length > 0 ? 'default' : 'secondary'}>
                    {anomalies.length}
                  </Badge>
                </div>
                <div className="flex justify-between items-center py-2">
                  <span className="text-gray-600 dark:text-gray-400">Recommendations</span>
                  <Badge variant="secondary">{recommendations.length}</Badge>
                </div>
              </CardContent>
            </Card>

            {/* Latest Alert */}
            {alerts.length > 0 && (
              <Card className="bg-gray-900 text-white border-gray-700">
                <CardHeader>
                  <CardTitle className="text-lg flex items-center gap-2 text-white">
                    <AlertCircle className="w-5 h-5 text-red-500" />
                    Latest Alert
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="font-medium text-white">
                    {alerts[0].message}
                  </p>
                  <p className="text-sm text-gray-400 mt-2">
                    {alerts[0].recommended_action}
                  </p>
                  <p className="text-xs text-gray-500 mt-2">
                    Triggered: {new Date(alerts[0].triggered_at).toLocaleString()}
                  </p>
                </CardContent>
              </Card>
            )}
          </div>
        )}

        {/* Anomalies Tab */}
        {activeTab === 'anomalies' && (
          <div className="space-y-3">
            {anomalies.length === 0 ? (
              <Card>
                <CardContent className="p-8 text-center">
                  <CheckCircle className="w-12 h-12 text-emerald-500 mx-auto mb-4" />
                  <p className="text-gray-600 dark:text-gray-400">No cost anomalies detected</p>
                </CardContent>
              </Card>
            ) : (
              anomalies.map((anomaly, index) => (
                <Card key={index} className="overflow-hidden">
                  <CardHeader
                    className="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800 transition-colors"
                    onClick={() => {
                      const newExpanded = new Set(expandedAnomalies)
                      if (newExpanded.has(index)) {
                        newExpanded.delete(index)
                      } else {
                        newExpanded.add(index)
                      }
                      setExpandedAnomalies(newExpanded)
                    }}
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-3">
                        {getAlertIcon(anomaly.severity)}
                        <div>
                          <CardTitle className="text-base">{formatDate(anomaly.date)}</CardTitle>
                          <CardDescription>{anomaly.description}</CardDescription>
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        <Badge variant={getAlertBadgeVariant(anomaly.severity)}>
                          {anomaly.severity}
                        </Badge>
                        {expandedAnomalies.has(index) ? (
                          <ChevronUp className="w-4 h-4 text-gray-400" />
                        ) : (
                          <ChevronDown className="w-4 h-4 text-gray-400" />
                        )}
                      </div>
                    </div>
                  </CardHeader>
                  {expandedAnomalies.has(index) && (
                    <CardContent className="border-t border-gray-100 dark:border-slate-800">
                      <div className="grid grid-cols-3 gap-4 text-sm">
                        <div>
                          <span className="text-gray-500 dark:text-gray-400">Actual Cost</span>
                          <p className="font-medium text-gray-900 dark:text-gray-100">
                            {formatCurrency(anomaly.cost)}
                          </p>
                        </div>
                        <div>
                          <span className="text-gray-500 dark:text-gray-400">Expected</span>
                          <p className="font-medium text-gray-900 dark:text-gray-100">
                            {formatCurrency(anomaly.expected_cost)}
                          </p>
                        </div>
                        <div>
                          <span className="text-gray-500 dark:text-gray-400">Deviation</span>
                          <p className={cn(
                            "font-medium",
                            anomaly.deviation_percentage > 0 ? "text-red-600" : "text-emerald-600"
                          )}>
                            {anomaly.deviation_percentage > 0 ? '+' : ''}
                            {anomaly.deviation_percentage.toFixed(1)}%
                          </p>
                        </div>
                      </div>
                    </CardContent>
                  )}
                </Card>
              ))
            )}
          </div>
        )}

        {/* Alerts Tab */}
        {activeTab === 'alerts' && (
          <div className="space-y-3">
            {alerts.length === 0 ? (
              <Card>
                <CardContent className="p-8 text-center">
                  <CheckCircle className="w-12 h-12 text-emerald-500 mx-auto mb-4" />
                  <p className="text-gray-600 dark:text-gray-400">No active alerts</p>
                </CardContent>
              </Card>
            ) : (
              alerts.map((alert, index) => (
                <Card key={index} className={cn(
                  "border-l-4",
                  alert.alert_level === 'critical' ? "border-l-red-500" : "border-l-amber-500"
                )}>
                  <CardHeader>
                    <div className="flex items-start justify-between">
                      <div className="flex items-start gap-3">
                        {getAlertIcon(alert.alert_level)}
                        <div>
                          <CardTitle className="text-base">{alert.message}</CardTitle>
                          <CardDescription className="mt-1">
                            {alert.recommended_action}
                          </CardDescription>
                        </div>
                      </div>
                      <Badge variant={getAlertBadgeVariant(alert.alert_level)}>
                        {alert.alert_level}
                      </Badge>
                    </div>
                  </CardHeader>
                  <CardContent>
                    <div className="flex justify-between text-sm text-gray-600 dark:text-gray-400">
                      <span>Usage: {alert.percentage_used.toFixed(1)}%</span>
                      <span>{new Date(alert.triggered_at).toLocaleString()}</span>
                    </div>
                  </CardContent>
                </Card>
              ))
            )}
          </div>
        )}

        {/* Recommendations Tab */}
        {activeTab === 'recommendations' && (
          <div className="space-y-3">
            {recommendations.length === 0 ? (
              <Card>
                <CardContent className="p-8 text-center">
                  <CheckCircle className="w-12 h-12 text-emerald-500 mx-auto mb-4" />
                  <p className="text-gray-600 dark:text-gray-400">No recommendations at this time</p>
                </CardContent>
              </Card>
            ) : (
              recommendations.map((rec, index) => (
                <Card key={index}>
                  <CardHeader>
                    <div className="flex items-start justify-between">
                      <div className="flex items-start gap-3">
                        <Lightbulb className="w-5 h-5 text-amber-500 mt-0.5" />
                        <div>
                          <CardTitle className="text-base">{rec.message}</CardTitle>
                          <CardDescription className="mt-1">{rec.details}</CardDescription>
                        </div>
                      </div>
                      <Badge className={getPriorityColor(rec.priority)}>
                        {rec.priority}
                      </Badge>
                    </div>
                  </CardHeader>
                  <CardContent>
                    <div className="flex items-center gap-2 text-sm">
                      <DollarSign className="w-4 h-4 text-emerald-500" />
                      <span className="text-emerald-600 dark:text-emerald-400 font-medium">
                        Potential savings: {rec.potential_savings}
                      </span>
                    </div>
                  </CardContent>
                </Card>
              ))
            )}
          </div>
        )}
      </div>
    </div>
  )
}
