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

    // Normalize statuses
    const redisStatus = components.redis?.toLowerCase() || 'unknown'
    const dorisStatus = components.doris_mcp?.toLowerCase() || 'unknown'
    const oracleStatus = components.sqlcl_pool?.toLowerCase() || 'unknown'
    const graphStatus = components.graphiti?.toLowerCase() || 'unknown'

    // Backend composite status
    const backendStatus = (
        dorisStatus === 'connected' ||
        oracleStatus === 'active' ||
        components.mcp_client === 'connected' ||
        components.mcp_client === 'ready'
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
                {/* Backend / Orchestrator */}
                <StatusCapsule
                    label="Backend"
                    status={backendStatus}
                    icon={Server}
                    details="FastAPI, LangGraph Orchestrator"
                />

                {/* Database Layer */}
                <StatusCapsule
                    label="Oracle"
                    status={oracleStatus}
                    icon={Database}
                    details="Oracle SQLcl Pool"
                />

                <StatusCapsule
                    label="Doris"
                    status={dorisStatus}
                    icon={Database}
                    details="Doris MCP Server (Streamable HTTP)"
                />

                <StatusCapsule
                    label="Redis"
                    status={redisStatus}
                    icon={Share2}
                    details="Cache & Pub/Sub Layer"
                />

                <StatusCapsule
                    label="Graph"
                    status={graphStatus}
                    icon={Share2}
                    details="Graphiti / FalkorDB Knowledge Graph"
                />
            </div>
        </div>
    )
}
