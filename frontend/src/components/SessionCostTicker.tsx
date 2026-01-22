import { useEffect, useState } from 'react'
import { Clock, Zap, DollarSign } from 'lucide-react'
import { cn } from '@/utils/cn'
import { useMessages } from '@/stores/chatStore'

interface SessionMetrics {
    totalExecutionTimeMs: number
    totalTokens: number
    queryCount: number
    estimatedCost: number
}

export function SessionCostTicker() {
    const messages = useMessages()
    const [metrics, setMetrics] = useState<SessionMetrics>({
        totalExecutionTimeMs: 0,
        totalTokens: 0,
        queryCount: 0,
        estimatedCost: 0
    })
    const [isExpanded, setIsExpanded] = useState(false)

    useEffect(() => {
        // Calculate metrics from messages
        let totalTime = 0
        let totalTokens = 0
        let queryCount = 0

        messages.forEach(msg => {
            if (msg.toolCall?.summary) {
                const summary = msg.toolCall.summary
                if (summary.executionTimeMs) {
                    totalTime += summary.executionTimeMs
                }
                if (msg.toolCall.status === 'completed') {
                    queryCount++
                }
            }
            // Extract token usage from metadata if available
            if (msg.toolCall?.metadata?.tokens) {
                totalTokens += msg.toolCall.metadata.tokens
            }
        })

        // Rough cost estimation (adjust based on actual pricing)
        // Gemini: ~$0.00025 per 1K tokens (input) + $0.0005 per 1K tokens (output)
        // Average: ~$0.000375 per 1K tokens
        const estimatedCost = (totalTokens / 1000) * 0.000375

        setMetrics({
            totalExecutionTimeMs: totalTime,
            totalTokens,
            queryCount,
            estimatedCost
        })
    }, [messages])

    const formatTime = (ms: number): string => {
        if (ms < 1000) return `${ms}ms`
        if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
        return `${(ms / 60000).toFixed(1)}m`
    }

    const formatTokens = (tokens: number): string => {
        if (tokens < 1000) return `${tokens}`
        if (tokens < 1000000) return `${(tokens / 1000).toFixed(1)}K`
        return `${(tokens / 1000000).toFixed(1)}M`
    }

    if (metrics.queryCount === 0) return null

    return (
        <div
            className={cn(
                "fixed bottom-4 right-4 z-40 transition-all duration-200",
                isExpanded ? "w-80" : "w-auto"
            )}
            onMouseEnter={() => setIsExpanded(true)}
            onMouseLeave={() => setIsExpanded(false)}
        >
            <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-700 rounded-lg shadow-lg overflow-hidden">
                {/* Compact View */}
                {!isExpanded && (
                    <div className="px-3 py-2 flex items-center gap-2 text-xs">
                        <Zap className="w-3.5 h-3.5 text-emerald-500" />
                        <span className="font-medium text-gray-700 dark:text-gray-300">
                            {metrics.queryCount} {metrics.queryCount === 1 ? 'query' : 'queries'}
                        </span>
                        <span className="text-gray-400">•</span>
                        <span className="text-gray-600 dark:text-gray-400">
                            {formatTime(metrics.totalExecutionTimeMs)}
                        </span>
                    </div>
                )}

                {/* Expanded View */}
                {isExpanded && (
                    <div className="p-4">
                        <div className="flex items-center justify-between mb-3">
                            <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 flex items-center gap-2">
                                <Zap className="w-4 h-4 text-emerald-500" />
                                Session Metrics
                            </h3>
                            <span className="text-xs text-gray-500 dark:text-gray-400">
                                Live
                            </span>
                        </div>

                        <div className="space-y-2">
                            {/* Query Count */}
                            <div className="flex items-center justify-between py-1.5 border-b border-gray-100 dark:border-slate-800">
                                <span className="text-xs text-gray-600 dark:text-gray-400">Queries Executed</span>
                                <span className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                                    {metrics.queryCount}
                                </span>
                            </div>

                            {/* Execution Time */}
                            <div className="flex items-center justify-between py-1.5 border-b border-gray-100 dark:border-slate-800">
                                <span className="text-xs text-gray-600 dark:text-gray-400 flex items-center gap-1">
                                    <Clock className="w-3 h-3" />
                                    DB Execution Time
                                </span>
                                <span className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                                    {formatTime(metrics.totalExecutionTimeMs)}
                                </span>
                            </div>

                            {/* Token Usage */}
                            {metrics.totalTokens > 0 && (
                                <div className="flex items-center justify-between py-1.5 border-b border-gray-100 dark:border-slate-800">
                                    <span className="text-xs text-gray-600 dark:text-gray-400">LLM Tokens</span>
                                    <span className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                                        {formatTokens(metrics.totalTokens)}
                                    </span>
                                </div>
                            )}

                            {/* Estimated Cost */}
                            {metrics.estimatedCost > 0 && (
                                <div className="flex items-center justify-between py-1.5">
                                    <span className="text-xs text-gray-600 dark:text-gray-400 flex items-center gap-1">
                                        <DollarSign className="w-3 h-3" />
                                        Est. Cost
                                    </span>
                                    <span className="text-sm font-semibold text-emerald-600 dark:text-emerald-400">
                                        ${metrics.estimatedCost.toFixed(4)}
                                    </span>
                                </div>
                            )}
                        </div>

                        {/* Average per query */}
                        {metrics.queryCount > 0 && (
                            <div className="mt-3 pt-3 border-t border-gray-100 dark:border-slate-800">
                                <div className="text-[10px] text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-1">
                                    Average per Query
                                </div>
                                <div className="flex items-center justify-between text-xs">
                                    <span className="text-gray-600 dark:text-gray-400">Time</span>
                                    <span className="font-medium text-gray-700 dark:text-gray-300">
                                        {formatTime(metrics.totalExecutionTimeMs / metrics.queryCount)}
                                    </span>
                                </div>
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    )
}
