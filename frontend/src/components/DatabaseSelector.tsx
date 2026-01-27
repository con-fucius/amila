import React, { useCallback } from 'react'
import { cn } from '@/utils/cn'
import { useChatActions, useDatabaseType, type DatabaseType } from '@/stores/chatStore'
import { useBackendHealth } from '@/hooks/useBackendHealth'

interface DatabaseSelectorProps {
    variant?: 'sidebar' | 'header'
    disabled?: boolean
    className?: string
}

export function DatabaseSelector({ variant = 'sidebar', disabled = false, className }: DatabaseSelectorProps) {
    const { setDatabaseType } = useChatActions()
    const databaseType = useDatabaseType()
    const { components, recheckHealth } = useBackendHealth(10000)

    // Check if a database component is healthy
    const isHealthy = useCallback((type: DatabaseType): boolean => {
        if (!components) return false
        if (type === 'oracle') {
            const status = components.sqlcl_pool || components.mcp_client
            return ['active', 'connected', 'ready'].includes(status?.toLowerCase())
        }
        if (type === 'doris') {
            const status = components.doris_mcp
            return ['active', 'connected', 'ready'].includes(status?.toLowerCase())
        }
        if (type === 'postgres') {
            // Assuming postgres health is checked too
            return components.postgres !== 'disconnected' && components.postgres !== 'error'
        }
        return false
    }, [components])

    const handleSwitch = (type: DatabaseType) => {
        if (disabled || type === databaseType) return
        if (!isHealthy(type)) {
            recheckHealth()
            return
        }
        setDatabaseType(type)
    }

    const dbs: { id: DatabaseType; label: string }[] = [
        { id: 'oracle', label: 'Oracle' },
        { id: 'doris', label: 'Doris' },
        { id: 'postgres', label: 'Postgres' },
    ]

    if (variant === 'header') {
        return (
            <div className={cn("flex items-center gap-1 bg-white/50 dark:bg-slate-900/50 p-1 rounded-lg border border-gray-200 dark:border-slate-800", className)}>
                {dbs.map((db) => {
                    const active = databaseType === db.id
                    const healthy = isHealthy(db.id)
                    return (
                        <button
                            key={db.id}
                            onClick={() => handleSwitch(db.id)}
                            disabled={disabled || !healthy}
                            className={cn(
                                "px-3 py-1.5 rounded-md text-xs font-medium transition-all flex items-center gap-2",
                                active
                                    ? "bg-emerald-600 text-white shadow-sm"
                                    : "text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-slate-800 hover:text-gray-900 dark:hover:text-gray-200",
                                (disabled || !healthy) && "opacity-50 cursor-not-allowed"
                            )}
                        >
                            {db.label}
                            <span className={cn(
                                "w-1.5 h-1.5 rounded-full",
                                healthy ? "bg-green-500" : "bg-red-500"
                            )} />
                        </button>
                    )
                })}
            </div>
        )
    }

    // Sidebar variant - pill style as requested
    return (
        <div className={cn("flex flex-wrap p-0.5 bg-gray-950/80 rounded-lg border border-gray-800/80 relative", className)}>
            {dbs.map((db) => {
                const active = databaseType === db.id
                const healthy = isHealthy(db.id)
                return (
                    <button
                        key={db.id}
                        onClick={() => handleSwitch(db.id)}
                        disabled={disabled || !healthy}
                        className={cn(
                            "flex-1 px-2 py-1.5 rounded-md text-[11px] font-medium transition-all relative z-10 flex items-center justify-center gap-1.5",
                            active
                                ? "bg-emerald-600 text-white shadow-sm"
                                : "text-gray-400 hover:text-gray-200",
                            (disabled || !healthy) && "opacity-50 cursor-not-allowed"
                        )}
                    >
                        {db.label}
                        <span className={cn(
                            "w-1.5 h-1.5 rounded-full",
                            healthy ? "bg-green-500" : "bg-red-500"
                        )} />
                    </button>
                )
            })}
        </div>
    )
}
