import React from 'react'
import { motion } from 'framer-motion'
import { cn } from '@/utils/cn'
import { Activity, Database, Server, Share2, AlertTriangle, CheckCircle2, XCircle, Slash } from 'lucide-react'
import {
    Tooltip,
    TooltipContent,
    TooltipProvider,
    TooltipTrigger,
} from "@/components/ui/tooltip"

interface HealthComponentProps {
    label: string
    status: string
    icon: React.ElementType
    details?: string
    collapsed?: boolean
}

const statusConfig: Record<string, { color: string, bg: string, border: string, icon: React.ElementType, label: string }> = {
    active: { color: 'text-emerald-400', bg: 'bg-emerald-950/30', border: 'border-emerald-500/30', icon: CheckCircle2, label: 'Active' },
    healthy: { color: 'text-emerald-400', bg: 'bg-emerald-950/30', border: 'border-emerald-500/30', icon: CheckCircle2, label: 'Healthy' },
    connected: { color: 'text-emerald-400', bg: 'bg-emerald-950/30', border: 'border-emerald-500/30', icon: CheckCircle2, label: 'Connected' },
    available: { color: 'text-emerald-400', bg: 'bg-emerald-950/30', border: 'border-emerald-500/30', icon: CheckCircle2, label: 'Available' },
    ready: { color: 'text-emerald-400', bg: 'bg-emerald-950/30', border: 'border-emerald-500/30', icon: CheckCircle2, label: 'Ready' },

    degraded: { color: 'text-amber-400', bg: 'bg-amber-950/30', border: 'border-amber-500/30', icon: AlertTriangle, label: 'Degraded' },
    initializing: { color: 'text-blue-400', bg: 'bg-blue-950/30', border: 'border-blue-500/30', icon: Activity, label: 'Init...' },
    checking: { color: 'text-blue-400', bg: 'bg-blue-950/30', border: 'border-blue-500/30', icon: Activity, label: 'Checking' },

    disconnected: { color: 'text-red-400', bg: 'bg-red-950/30', border: 'border-red-500/30', icon: XCircle, label: 'Down' },
    error: { color: 'text-red-400', bg: 'bg-red-950/30', border: 'border-red-500/30', icon: XCircle, label: 'Error' },
    inactive: { color: 'text-gray-400', bg: 'bg-gray-800/50', border: 'border-gray-700/50', icon: Slash, label: 'Inactive' },
    unknown: { color: 'text-gray-500', bg: 'bg-gray-900/50', border: 'border-gray-800/50', icon: Slash, label: 'Unknown' },
    fallback: { color: 'text-amber-400', bg: 'bg-amber-950/30', border: 'border-amber-500/30', icon: AlertTriangle, label: 'Fallback' },
    mock: { color: 'text-purple-400', bg: 'bg-purple-950/30', border: 'border-purple-500/30', icon: Database, label: 'Mock' },
}

const getStatusConfig = (status: string) => {
    const norm = status?.toLowerCase() || 'unknown'
    return statusConfig[norm] || statusConfig['unknown']
}

const HealthCapsule = ({ label, status, icon: Icon, details, collapsed }: HealthComponentProps) => {
    const config = getStatusConfig(status)

    if (collapsed) {
        return (
            <TooltipProvider delayDuration={300}>
                <Tooltip>
                    <TooltipTrigger asChild>
                        <div
                            className={cn(
                                "flex items-center justify-center w-8 h-8 rounded-lg transition-all",
                                config.bg,
                                config.color
                            )}
                        >
                            <Icon className="w-4 h-4" />
                            <div className={cn(
                                "absolute top-0 right-0 w-2 h-2 rounded-full border-2 border-gray-900",
                                status === 'active' || status === 'healthy' || status === 'connected' ? 'bg-green-500' :
                                    status === 'checking' ? 'bg-blue-500' : 'bg-red-500'
                            )} />
                        </div>
                    </TooltipTrigger>
                    <TooltipContent side="right" className="bg-gray-900 border-gray-700 text-xs">
                        <div className="font-medium text-gray-200">{label}</div>
                        <div className="text-gray-400 capitalize">{status}</div>
                    </TooltipContent>
                </Tooltip>
            </TooltipProvider>
        )
    }

    return (
        <TooltipProvider delayDuration={300}>
            <Tooltip>
                <TooltipTrigger asChild>
                    <motion.div
                        layout
                        initial={{ opacity: 0, y: 5 }}
                        animate={{ opacity: 1, y: 0 }}
                        className={cn(
                            "flex items-center justify-between px-2 py-1.5 rounded-md border backdrop-blur-sm transition-all duration-300 group cursor-help",
                            "bg-gradient-to-r",
                            config.bg,
                            config.border,
                            "hover:border-opacity-100 border-opacity-60"
                        )}
                    >
                        <div className="flex items-center gap-2">
                            <Icon className={cn("w-3.5 h-3.5", config.color)} />
                            <span className="text-[10px] font-medium text-gray-300 group-hover:text-white transition-colors">
                                {label}
                            </span>
                        </div>

                        <div className="flex items-center gap-1.5 pl-2 border-l border-white/5 mx-1">
                            <span className={cn("text-[9px] font-medium tracking-wide", config.color)}>
                                {config.label}
                            </span>
                            <motion.div
                                animate={status === 'checking' || status === 'initializing' ? { rotate: 360 } : {}}
                                transition={{ duration: 2, repeat: Infinity, ease: "linear" }}
                            >
                                {status !== 'checking' && status !== 'initializing' && (
                                    <div className={cn("w-1.5 h-1.5 rounded-full shadow-[0_0_8px_currentColor]", config.color)} />
                                )}
                            </motion.div>
                        </div>
                    </motion.div>
                </TooltipTrigger>
                <TooltipContent side="right" className="bg-gray-900 border-gray-700 text-xs shadow-xl">
                    <div className="font-medium text-gray-200">{label} status</div>
                    <div className="text-gray-400 mt-1">
                        Current state: <span className={cn(config.color)}>{status}</span>
                    </div>
                    {details && <div className="text-gray-500 mt-1 italic border-t border-gray-800 pt-1">{details}</div>}
                </TooltipContent>
            </Tooltip>
        </TooltipProvider>
    )
}

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
                <HealthCapsule label="Backend" status={backendStatus} icon={Server} collapsed={true} />
                <HealthCapsule label="Doris" status={dorisStatus} icon={Database} collapsed={true} />
                <HealthCapsule label="Oracle" status={oracleStatus} icon={Database} collapsed={true} />
            </div>
        )
    }

    return (
        <div className={cn("space-y-2", className)}>
            <div className="text-[10px] font-medium text-gray-500 mb-2 pl-1 flex items-center gap-2">
                <Activity className="w-3 h-3" />
                System status
            </div>

            <div className="grid grid-cols-1 gap-1.5">
                {/* Backend / Orchestrator */}
                <HealthCapsule
                    label="Backend"
                    status={backendStatus}
                    icon={Server}
                    details="FastAPI, LangGraph Orchestrator"
                />

                {/* Database Layer */}
                <HealthCapsule
                    label="Oracle"
                    status={oracleStatus}
                    icon={Database}
                    details="Oracle SQLcl Pool"
                />

                <HealthCapsule
                    label="Doris"
                    status={dorisStatus}
                    icon={Database}
                    details="Doris MCP Server (Streamable HTTP)"
                />

                <HealthCapsule
                    label="Redis"
                    status={redisStatus}
                    icon={Share2}
                    details="Cache & Pub/Sub Layer"
                />

                <HealthCapsule
                    label="Graph"
                    status={graphStatus}
                    icon={Share2}
                    details="Graphiti / FalkorDB Knowledge Graph"
                />
            </div>
        </div>
    )
}
