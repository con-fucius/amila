import { useState } from 'react'
import { Webhooks } from '../pages/Webhooks'
import { BudgetForecasting } from '../components/BudgetForecasting'
import { RateLimits } from '../pages/RateLimits'
import { OperationalKPIs } from '../components/OperationalKPIs'
import { useBackendHealth } from '@/hooks/useBackendHealth'
import { cn } from '@/utils/cn'

export function AdminPanel() {
    const [activeTab, setActiveTab] = useState<'overview' | 'webhooks' | 'budget' | 'ratelimits'>('overview')
    const { diagnostics } = useBackendHealth(5000)

    return (
        <div className="space-y-6">
            <div className="flex items-center gap-2 border-b border-gray-800 pb-2">
                <button
                    onClick={() => setActiveTab('overview')}
                    className={cn(
                        "px-3 py-1.5 rounded-md text-sm transition-colors",
                        activeTab === 'overview' ? "bg-gray-800 text-white" : "text-gray-400 hover:text-white"
                    )}
                >
                    Overview
                </button>
                <button
                    onClick={() => setActiveTab('webhooks')}
                    className={cn(
                        "px-3 py-1.5 rounded-md text-sm transition-colors",
                        activeTab === 'webhooks' ? "bg-gray-800 text-white" : "text-gray-400 hover:text-white"
                    )}
                >
                    Webhooks
                </button>
                <button
                    onClick={() => setActiveTab('budget')}
                    className={cn(
                        "px-3 py-1.5 rounded-md text-sm transition-colors",
                        activeTab === 'budget' ? "bg-gray-800 text-white" : "text-gray-400 hover:text-white"
                    )}
                >
                    Budgeting
                </button>
                <button
                    onClick={() => setActiveTab('ratelimits')}
                    className={cn(
                        "px-3 py-1.5 rounded-md text-sm transition-colors",
                        activeTab === 'ratelimits' ? "bg-gray-800 text-white" : "text-gray-400 hover:text-white"
                    )}
                >
                    Rate Limits
                </button>
            </div>

            <div className="mt-4">
                {activeTab === 'overview' && (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        <div className="p-4 bg-gray-900 border border-gray-800 rounded-lg">
                            <h3 className="text-sm font-semibold text-white mb-4">System Operational Status</h3>
                            <OperationalKPIs kpis={diagnostics?.business_kpis} />
                        </div>
                        <div className="p-4 bg-gray-900 border border-gray-800 rounded-lg">
                            <h3 className="text-sm font-semibold text-white mb-2">Admin Quick Actions</h3>
                            <p className="text-xs text-gray-500">System maintenance controls coming soon.</p>
                        </div>
                    </div>
                )}
                {activeTab === 'webhooks' && <Webhooks />}
                {activeTab === 'budget' && <BudgetForecasting />}
                {activeTab === 'ratelimits' && <RateLimits />}
            </div>
        </div>
    )
}
