import { cn } from '@/utils/cn'
import { Database, Server, Share2, AlertTriangle, ChevronDown, ChevronRight } from 'lucide-react'
import { StatusCapsule } from './StatusCapsule'
import { useState } from 'react'

interface SystemHealthMonitorProps {
    components: any
    diagnostics?: any
    className?: string
    collapsed?: boolean
}

export function SystemHealthMonitor({ components, diagnostics, className, collapsed }: SystemHealthMonitorProps) {
    const [isExpanded, setIsExpanded] = useState(false)
    
    if (!components) return null

    // Extract status from objects if necessary
    const getStatus = (comp: any) => {
        if (!comp) return 'unknown'
        if (typeof comp === 'string') return comp.toLowerCase()
        return comp.status?.toLowerCase() || 'unknown'
    }

    const redisStatus = getStatus(components.redis)
    const dorisStatus = getStatus(components.doris || components.doris_mcp)
    const oracleStatus = getStatus(components.sqlcl_pool || components.mcp_client)
    const postgresStatus = getStatus(components.postgres)
    const orchestratorStatus = getStatus(components.orchestrator)
    const checkpointerStatus = getStatus(components.langgraph_checkpointer)


    const alerts = diagnostics?.alerts

    // Backend composite status
    const backendStatus = (
        dorisStatus === 'connected' ||
        oracleStatus === 'active' ||
        orchestratorStatus === 'ready' ||
        orchestratorStatus === 'initialized' ||
        checkpointerStatus === 'operational' ||
        components.mcp_client === 'connected'
    ) ? (alerts?.critical > 0 ? 'critical' : (alerts?.warning > 0 ? 'warning' : 'active')) : 'degraded'

    if (collapsed) {
        return (
            <div className={cn("flex flex-col items-center gap-2", className)}>
                <StatusCapsule label="Backend" status={backendStatus} icon={Server} collapsed={true} />
                <StatusCapsule label="Oracle" status={oracleStatus} icon={Database} collapsed={true} />
                <StatusCapsule label="Doris" status={dorisStatus} icon={Database} collapsed={true} />
                <StatusCapsule label="Postgres" status={postgresStatus} icon={Database} collapsed={true} />
            </div>
        )
    }

    return (
        <div className={cn("space-y-1.5", className)}>
            <button
                onClick={() => setIsExpanded(!isExpanded)}
                className="w-full text-[11px] font-semibold text-gray-400 mb-1 pl-0.5 flex items-center gap-1.5 tracking-wide hover:text-gray-300 transition-colors"
            >
                {isExpanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                <span>Status</span>
                {alerts?.total_active > 0 && (
                    <span className="text-[9px] text-amber-400">({alerts.total_active})</span>
                )}
            </button>

            {isExpanded && (
                <>
                    <div className="grid grid-cols-1 gap-1">
                        {/* Core Architecture */}
                        <StatusCapsule
                            label="Backend"
                            status={backendStatus}
                            icon={Server}
                            details={alerts?.total_active > 0 ? `${alerts.total_active} active alerts` : `Orchestrator: ${orchestratorStatus}`}
                        />

                        <StatusCapsule
                            label="Redis"
                            status={redisStatus}
                            icon={Share2}
                            details="Cache, Sessions & Task Queue"
                        />

                        {/* Database Layer */}
                        <StatusCapsule
                            label="Oracle"
                            status={oracleStatus}
                            icon={Database}
                            details="Primary Analytics Store (SQLcl)"
                        />

                        <StatusCapsule
                            label="Doris"
                            status={dorisStatus}
                            icon={Database}
                            details="Federated Query Engine (MCP)"
                        />

                        <StatusCapsule
                            label="Postgres"
                            status={postgresStatus}
                            icon={Database}
                            details="PostgreSQL Database (psycopg3)"
                        />

                        <StatusCapsule
                            label="Checkpointer"
                            status={checkpointerStatus}
                            icon={Share2}
                            details="LangGraph State Persistence (SQLite)"
                        />
                    </div>

                    {/* Alerts Section (Subtle) */}
                    {alerts?.total_active > 0 && (
                        <div className="mt-3">
                            <div className="flex items-center gap-1.5 px-1 py-1 text-[10px] text-amber-400 font-medium bg-amber-400/5 rounded border border-amber-400/10">
                                <AlertTriangle className="w-3 h-3" />
                                <span>{alerts.total_active} System Alert{alerts.total_active > 1 ? 's' : ''} Active</span>
                            </div>
                        </div>
                    )}
                </>
            )}
        </div>
    )
}
