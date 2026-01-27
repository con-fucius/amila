import { cn } from '@/utils/cn'
import { Activity, Database, Server, Share2 } from 'lucide-react'
import { StatusCapsule } from './StatusCapsule'

interface SystemHealthMonitorProps {
    components: any
    className?: string
    collapsed?: boolean
}

export function SystemHealthMonitor({ components, className, collapsed }: SystemHealthMonitorProps) {
    if (!components) return null

    // Extract status from objects if necessary
    const getStatus = (comp: any) => {
        if (!comp) return 'unknown'
        if (typeof comp === 'string') return comp.toLowerCase()
        return comp.status?.toLowerCase() || 'unknown'
    }

    const redisStatus = getStatus(components.redis)
    const dorisStatus = getStatus(components.doris)
    const oracleStatus = getStatus(components.sqlcl_pool)
    const graphStatus = getStatus(components.graphiti)
    const orchestratorStatus = getStatus(components.orchestrator)
    const pgStatus = getStatus(components.postgres)
    const qlikStatus = getStatus(components.qlik)
    const supersetStatus = getStatus(components.superset)

    // Backend composite status
    const backendStatus = (
        dorisStatus === 'connected' ||
        oracleStatus === 'active' ||
        orchestratorStatus === 'ready' ||
        orchestratorStatus === 'initialized' ||
        components.mcp_client === 'connected'
    ) ? 'active' : 'degraded'

    if (collapsed) {
        return (
            <div className={cn("flex flex-col items-center gap-2", className)}>
                <StatusCapsule label="Backend" status={backendStatus} icon={Server} collapsed={true} />
                <StatusCapsule label="Doris" status={dorisStatus} icon={Database} collapsed={true} />
                <StatusCapsule label="Oracle" status={oracleStatus} icon={Database} collapsed={true} />
            </div>
        )
    }

    return (
        <div className={cn("space-y-1.5", className)}>
            <div className="text-[11px] font-semibold text-gray-400 mb-1 pl-0.5 flex items-center gap-1.5 tracking-wide">
                <span>Status</span>
            </div>

            <div className="grid grid-cols-1 gap-1">
                {/* Core Architecture */}
                <StatusCapsule
                    label="Backend"
                    status={backendStatus}
                    icon={Server}
                    details={`Orchestrator: ${orchestratorStatus}`}
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

                {pgStatus !== 'disabled' && (
                    <StatusCapsule
                        label="Postgre"
                        status={pgStatus}
                        icon={Database}
                        details="Relational Storage"
                    />
                )}

                <StatusCapsule
                    label="Graph"
                    status={graphStatus}
                    icon={Share2}
                    details="Knowledge Graph (FalkorDB)"
                />

                {/* BI Integration Layer */}
                {qlikStatus !== 'not_configured' && (
                    <StatusCapsule
                        label="Qlik"
                        status={qlikStatus}
                        icon={Activity}
                        details="Qlik Sense Integration"
                    />
                )}

                {supersetStatus !== 'not_configured' && (
                    <StatusCapsule
                        label="Superset"
                        status={supersetStatus}
                        icon={Activity}
                        details="Apache Superset Integration"
                    />
                )}
            </div>
        </div>
    )
}
