import { useMemo } from 'react'
import { motion } from 'framer-motion'
import { cn } from '@/utils/cn'
import {
    CheckCircle2,
    AlertTriangle,
    XCircle,
    Activity,
    Slash,
    Database,
} from 'lucide-react'
import {
    Tooltip,
    TooltipContent,
    TooltipProvider,
    TooltipTrigger,
} from "@/components/ui/tooltip"

export interface StatusCapsuleProps {
    status: string
    label?: string
    icon?: React.ElementType
    showLabel?: boolean
    details?: string
    size?: 'sm' | 'md'
    animate?: boolean
    collapsed?: boolean
    className?: string
}

// Map normalized status to visual configuration
const getStatusConfig = (status: string) => {
    const s = status?.toLowerCase() || 'unknown'

    // Green / Success
    if (['active', 'healthy', 'connected', 'available', 'ready', 'success'].includes(s)) {
        return {
            color: 'text-emerald-400',
            bgColor: 'bg-emerald-500/10',
            borderColor: 'border-emerald-500/30',
            icon: CheckCircle2,
            label: s
        }
    }

    // Amber / Warning
    if (['degraded', 'fallback', 'warning', 'lagging'].includes(s)) {
        return {
            color: 'text-amber-400',
            bgColor: 'bg-amber-500/10',
            borderColor: 'border-amber-500/30',
            icon: AlertTriangle,
            label: s
        }
    }

    // Red / Error
    if (['disconnected', 'error', 'down', 'offline', 'failed', 'missing'].includes(s)) {
        return {
            color: 'text-red-400',
            bgColor: 'bg-red-500/10',
            borderColor: 'border-red-500/30',
            icon: XCircle,
            label: s
        }
    }

    // Blue / Info / Loading
    if (['initializing', 'checking', 'loading', 'syncing'].includes(s)) {
        return {
            color: 'text-blue-400',
            bgColor: 'bg-blue-500/10',
            borderColor: 'border-blue-500/30',
            icon: Activity,
            label: s
        }
    }

    // Purple / Mock
    if (['mock', 'test'].includes(s)) {
        return {
            color: 'text-purple-400',
            bgColor: 'bg-purple-500/10',
            borderColor: 'border-purple-500/30',
            icon: Database,
            label: s
        }
    }

    // Default / Gray / Unknown
    return {
        color: 'text-gray-400',
        bgColor: 'bg-gray-800/50',
        borderColor: 'border-gray-700/50',
        icon: Slash,
        label: s
    }
}

export function StatusCapsule({
    status,
    label,
    icon: OverrideIcon,
    showLabel = true,
    details,
    size = 'md',
    animate = true,
    collapsed = false,
    className
}: StatusCapsuleProps) {
    const config = useMemo(() => getStatusConfig(status), [status])
    const Icon = OverrideIcon || config.icon

    const isPending = ['initializing', 'checking', 'loading', 'syncing'].includes(config.label.toLowerCase())

    if (collapsed) {
        return (
            <TooltipProvider delayDuration={300}>
                <Tooltip>
                    <TooltipTrigger asChild>
                        <div
                            className={cn(
                                "flex items-center justify-center rounded-md transition-all relative cursor-help",
                                size === 'sm' ? "w-6 h-6" : "w-7 h-7",
                                config.bgColor,
                                className
                            )}
                        >
                            <Icon className={cn(size === 'sm' ? "w-3 h-3" : "w-3.5 h-3.5", config.color)} />
                            <div className={cn(
                                "absolute -top-0.5 -right-0.5 w-1.5 h-1.5 rounded-full border border-gray-900",
                                isPending ? "bg-blue-500" :
                                    ['active', 'healthy', 'connected', 'ready'].includes(config.label.toLowerCase()) ? "bg-green-500" :
                                        ['disconnected', 'error', 'down'].includes(config.label.toLowerCase()) ? "bg-red-500" :
                                            "bg-gray-500"
                            )} />
                        </div>
                    </TooltipTrigger>
                    <TooltipContent side="right" className="bg-gray-900 border-gray-700 text-xs">
                        <div className="font-medium text-gray-200">{label || config.label}</div>
                        <div className="text-gray-400 capitalize text-[10px]">{config.label}</div>
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
                        layout={animate}
                        initial={{ opacity: 0, y: 3 }}
                        animate={{ opacity: 1, y: 0 }}
                        className={cn(
                            "flex items-center justify-between px-3 rounded-md border backdrop-blur-sm transition-all duration-300 group cursor-help min-w-[120px]",
                            size === 'sm' ? "py-0.5 h-5 text-[10px]" : "py-1.5 h-7 text-[11px]",
                            config.bgColor,
                            config.borderColor,
                            "hover:border-opacity-100 border-opacity-60",
                            className
                        )}
                    >
                        <div className="flex items-center gap-2">
                            {collapsed && <Icon className={cn(size === 'sm' ? "w-2.5 h-2.5" : "w-3 h-3", config.color)} />}
                            {showLabel && label && (
                                <span className="font-medium text-gray-300 group-hover:text-white transition-colors truncate max-w-[90px]">
                                    {label}
                                </span>
                            )}
                        </div>

                        <div className="flex items-center gap-1.5 pl-2 border-l border-white/5 ml-auto">
                            <span className={cn("font-medium tracking-wide capitalize text-[8px]", config.color)}>
                                {config.label}
                            </span>
                            <motion.div
                                animate={isPending ? { rotate: 360 } : {}}
                                transition={{ duration: 2, repeat: Infinity, ease: "linear" }}
                            >
                                {!isPending && (
                                    <div className={cn("w-1 h-1 rounded-full shadow-[0_0_4px_currentColor]", config.color)} />
                                )}
                                {isPending && (
                                    <div className={cn("w-1 h-1 rounded-full border border-current", config.color)} />
                                )}
                            </motion.div>
                        </div>
                    </motion.div>
                </TooltipTrigger>
                <TooltipContent side="right" className="bg-gray-900 border-gray-700 text-xs shadow-xl min-w-[140px]">
                    <div className="font-medium text-gray-200">{label || 'System Component'}</div>
                    <div className="text-gray-400 mt-1 flex items-center gap-2">
                        Status: <span className={cn(config.color)}>{config.label}</span>
                    </div>
                    {details && (
                        <div className="text-gray-500 mt-1 text-[10px] italic border-t border-gray-800 pt-1">
                            {details}
                        </div>
                    )}
                </TooltipContent>
            </Tooltip>
        </TooltipProvider>
    )
}
