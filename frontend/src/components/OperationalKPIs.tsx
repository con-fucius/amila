

interface OperationalKPIsProps {
    kpis: any
    className?: string
}

export function OperationalKPIs({ kpis, className }: OperationalKPIsProps) {
    if (!kpis) return null

    return (
        <div className={className}>
            <div className="text-[11px] font-semibold text-gray-400 mb-2 pl-0.5 flex items-center justify-between tracking-wide">
                <span>Operation KPIs</span>
                <span className="text-[9px] text-gray-500 font-normal">Last 24h</span>
            </div>
            <div className="grid grid-cols-2 gap-2">
                <div className="bg-gray-900/40 p-2 rounded border border-gray-800/50">
                    <div className="text-[9px] text-gray-500 font-medium mb-0.5">Daily cost</div>
                    <div className="text-xs font-bold text-emerald-400">
                        ${kpis.total_query_cost_24h?.toFixed(2) || '0.00'}
                    </div>
                </div>
                <div className="bg-gray-900/40 p-2 rounded border border-gray-800/50">
                    <div className="text-[9px] text-gray-500 font-medium mb-0.5">Reject rate</div>
                    <div className="text-xs font-bold text-amber-400">
                        {kpis.global_rejection_rate?.toFixed(1) || '0.0'}%
                    </div>
                </div>
            </div>
        </div>
    )
}
