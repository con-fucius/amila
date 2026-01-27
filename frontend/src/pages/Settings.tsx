import { useState, useContext, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import {
    ChevronLeft,
    Check,
    RefreshCw,
    Mail,
    Shield,
    Building2,
    LogOut,
    Wrench,
    Info,
    ExternalLink,
    Terminal,
    Box,
    ChevronDown,
    ChevronUp
} from 'lucide-react'
import { cn } from '@/utils/cn'
import { ColorModeContext } from '@/App'
import { useBackendHealth } from '@/hooks/useBackendHealth'
import { apiService } from '@/services/apiService'

type SettingsSection = 'account' | 'appearance' | 'preferences' | 'connections' | 'tools' | 'about'

interface SettingToggleProps {
    label: string
    description: string
    enabled: boolean
    onChange: (enabled: boolean) => void
    icon?: React.ReactNode
}

function SettingToggle({ label, description, enabled, onChange }: SettingToggleProps) {
    return (
        <div className="flex items-center justify-between py-1 px-1.5 rounded-md hover:bg-gray-800/20 transition-colors">
            <div className="flex items-center gap-1.5 min-w-0 flex-1">
                <p className="font-medium text-white text-[12px] flex-shrink-0">{label}</p>
                <span className="text-gray-600 text-[10px]">•</span>
                <p className="text-[11px] text-gray-500 truncate">{description}</p>
            </div>
            <button
                onClick={() => onChange(!enabled)}
                className={cn(
                    'relative w-7 h-4 rounded-full transition-colors flex-shrink-0 ml-3',
                    enabled ? 'bg-emerald-500' : 'bg-gray-600'
                )}
            >
                <span
                    className={cn(
                        'absolute top-0.5 left-0.5 w-3 h-3 rounded-full bg-white transition-transform shadow-sm',
                        enabled && 'translate-x-3'
                    )}
                />
            </button>
        </div>
    )
}

interface ConnectionStatusProps {
    name: string
    status: string
    details?: string
}

function ConnectionStatus({ name, status, details }: ConnectionStatusProps) {
    const isHealthy = status === 'active' || status === 'connected' || status === 'ready'
    return (
        <div className="flex items-center justify-between py-1 px-1.5 rounded-md hover:bg-gray-800/20 transition-colors">
            <div className="flex items-center gap-2 min-w-0 flex-1">
                <p className="font-medium text-white text-[12px] flex-shrink-0">{name}</p>
                <span className={cn(
                    'text-[8px] px-1.5 py-0.5 rounded-full capitalize font-bold tracking-tight',
                    isHealthy
                        ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                        : 'bg-red-500/10 text-red-400 border border-red-500/20'
                )}>
                    {status || 'unknown'}
                </span>
                {details && (
                    <>
                        <span className="text-gray-600 text-[10px]">•</span>
                        <p className="text-[11px] text-gray-500 truncate">{details}</p>
                    </>
                )}
            </div>
            <div className="flex-shrink-0 ml-3">
                <span className={cn(
                    'block w-1.5 h-1.5 rounded-full',
                    isHealthy ? 'bg-emerald-500' : 'bg-red-500'
                )} />
            </div>
        </div>
    )
}

export function Settings() {
    const navigate = useNavigate()
    const colorMode = useContext(ColorModeContext)
    const isDark = colorMode.mode === 'dark'
    const { components, recheckHealth, lastCheck } = useBackendHealth(10000)
    const [isRefreshing, setIsRefreshing] = useState(false)
    const [activeSection, setActiveSection] = useState<SettingsSection>('account')
    const [profile, setProfile] = useState<any>(null)
    const [mcpData, setMcpData] = useState<any>(null)
    const [expandedServers, setExpandedServers] = useState<Record<string, boolean>>({ oracle: true, doris: true, postgres: true })

    useEffect(() => {
        try {
            const token = localStorage.getItem('access_token')
            if (!token || token === 'dev-bypass-token') {
                setProfile({
                    username: 'admin',
                    role: 'Admin',
                    email: 'admin@company.com',
                    department: 'IT',
                    displayName: 'System Administrator',
                    jobTitle: 'BI Administrator',
                })
                return
            }
            const parts = token.split('.')
            if (parts.length >= 2) {
                const payload = JSON.parse(atob(parts[1].replace(/-/g, '+').replace(/_/g, '/')))
                setProfile({
                    username: payload.sub || payload.username || 'user',
                    role: payload.role || 'Analyst',
                    email: payload.email || `${payload.sub || 'user'}@company.com`,
                    department: payload.department || 'Analytics',
                    displayName: payload.name || payload.displayName || payload.sub || 'User',
                    jobTitle: payload.jobTitle || payload.title || 'Data Analyst',
                })
            }
        } catch {
            setProfile({ username: 'user', role: 'Analyst' })
        }
    }, [])

    const getStatus = (comp: any) => {
        if (!comp) return 'unknown'
        if (typeof comp === 'string') return comp.toLowerCase()
        return comp.status?.toLowerCase() || 'unknown'
    }

    useEffect(() => {
        if (activeSection === 'tools') {
            const fetchTools = async () => {
                const data = await apiService.getMCPToolsStatus()
                setMcpData(data)
            }
            fetchTools()
        }
    }, [activeSection])

    const toggleServer = (serverId: string) => {
        setExpandedServers(prev => ({ ...prev, [serverId]: !prev[serverId] }))
    }

    const handleLogout = () => {
        localStorage.removeItem('access_token')
        localStorage.removeItem('refresh_token')
        navigate('/login')
    }

    const handleRefresh = async () => {
        setIsRefreshing(true)
        await recheckHealth()
        setTimeout(() => setIsRefreshing(false), 500)
    }

    // Settings state (persisted in localStorage)
    const [settings, setSettings] = useState({
        showReasoningSteps: localStorage.getItem('showReasoningSteps') !== 'false',
        autoApproveSimple: localStorage.getItem('autoApproveSimple') === 'true',
        enableNotifications: localStorage.getItem('enableNotifications') !== 'false',
        enableSoundEffects: localStorage.getItem('enableSoundEffects') === 'true',
        showQueryTimer: localStorage.getItem('showQueryTimer') !== 'false',
        compactMode: localStorage.getItem('compactMode') === 'true',
        developerMode: localStorage.getItem('developerMode') === 'true',
        showSQLPreview: localStorage.getItem('showSQLPreview') !== 'false',
    })

    // Persist settings changes
    const updateSetting = (key: keyof typeof settings, value: boolean) => {
        setSettings(prev => ({ ...prev, [key]: value }))
        localStorage.setItem(key, String(value))
    }

    const handleThemeChange = (theme: 'light' | 'dark' | 'system') => {
        if (theme === 'system') {
            const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches
            if (prefersDark !== isDark) {
                colorMode.toggleColorMode()
            }
            localStorage.setItem('themePreference', 'system')
        } else if (theme === 'dark' && !isDark) {
            colorMode.toggleColorMode()
            localStorage.setItem('themePreference', 'dark')
        } else if (theme === 'light' && isDark) {
            colorMode.toggleColorMode()
            localStorage.setItem('themePreference', 'light')
        }
    }

    const themePreference = localStorage.getItem('themePreference') || (isDark ? 'dark' : 'light')

    const sections: { id: SettingsSection; label: string }[] = [
        { id: 'account', label: 'Account' },
        { id: 'appearance', label: 'Appearance' },
        { id: 'preferences', label: 'Preferences' },
        { id: 'connections', label: 'Connections' },
        { id: 'tools', label: 'Tools' },
        { id: 'about', label: 'About' },
    ]

    return (
        <div className="h-full bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 overflow-auto">
            <div className="p-6">
                {/* Header */}
                <div className="flex items-center gap-4 mb-4">
                    <button
                        onClick={() => navigate(-1)}
                        className="p-1.5 rounded-lg hover:bg-gray-700/50 text-gray-400 hover:text-white transition-colors"
                    >
                        <ChevronLeft className="w-4 h-4" />
                    </button>
                    <h1 className="text-xl font-bold text-white">Settings</h1>
                </div>

                <div className="flex gap-6">
                    {/* Sidebar Navigation */}
                    <div className="w-48 flex-shrink-0">
                        <nav className="space-y-0.5">
                            {sections.map((section) => (
                                <button
                                    key={section.id}
                                    onClick={() => setActiveSection(section.id)}
                                    className={cn(
                                        'w-full flex items-center px-3 py-1.5 rounded-md text-left transition-all',
                                        activeSection === section.id
                                            ? 'bg-emerald-600/20 text-emerald-400 border border-emerald-500/30'
                                            : 'text-gray-400 hover:text-white hover:bg-gray-800/50'
                                    )}
                                >
                                    <span className="text-[13px] font-medium">{section.label}</span>
                                </button>
                            ))}
                        </nav>
                    </div>

                    {/* Content Area */}
                    <div className="flex-1 min-w-0">
                        {/* Account Section */}
                        {activeSection === 'account' && profile && (
                            <div className="space-y-3">
                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <div className="flex items-center gap-4">
                                        <div className="w-12 h-12 rounded-full bg-gradient-to-r from-emerald-500 to-green-600 flex items-center justify-center text-white text-xl font-bold">
                                            {profile.displayName?.charAt(0).toUpperCase() || 'U'}
                                        </div>
                                        <div>
                                            <h2 className="text-base font-semibold text-white">{profile.displayName}</h2>
                                            <p className="text-gray-400 text-[11px]">@{profile.username}</p>
                                        </div>
                                    </div>
                                </div>

                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <h3 className="text-sm font-semibold text-white mb-2">Profile Details</h3>
                                    <div className="space-y-1">
                                        <div className="flex items-center gap-2 py-1 px-1.5 rounded-md hover:bg-gray-800/30 transition-colors">
                                            <Mail className="w-3.5 h-3.5 text-emerald-400 flex-shrink-0" />
                                            <div className="flex items-center gap-1.5 overflow-hidden">
                                                <span className="text-[11px] font-medium text-gray-400">Email:</span>
                                                <span className="text-white text-[12px] truncate">{profile.email}</span>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-2 py-1 px-1.5 rounded-md hover:bg-gray-800/30 transition-colors">
                                            <Shield className="w-3.5 h-3.5 text-emerald-400 flex-shrink-0" />
                                            <div className="flex items-center gap-1.5 overflow-hidden">
                                                <span className="text-[11px] font-medium text-gray-400">Role:</span>
                                                <span className="text-white text-[12px] truncate">{profile.role}</span>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-2 py-1 px-1.5 rounded-md hover:bg-gray-800/30 transition-colors">
                                            <Building2 className="w-3.5 h-3.5 text-emerald-400 flex-shrink-0" />
                                            <div className="flex items-center gap-1.5 overflow-hidden">
                                                <span className="text-[11px] font-medium text-gray-400">Department:</span>
                                                <span className="text-white text-[12px] truncate">{profile.department}</span>
                                            </div>
                                        </div>
                                    </div>
                                </div>

                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <button
                                        onClick={handleLogout}
                                        className="w-full flex items-center justify-center gap-2 py-2 rounded-lg bg-red-900/40 text-red-400 hover:bg-red-900/60 transition-colors text-[12px] font-medium"
                                    >
                                        <LogOut className="w-4 h-4" />
                                        Sign out
                                    </button>
                                </div>
                            </div>
                        )}
                        {/* Appearance Section */}
                        {activeSection === 'appearance' && (
                            <div className="space-y-3">
                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <h3 className="text-sm font-semibold text-white mb-2">Theme</h3>
                                    <div className="grid grid-cols-3 gap-2">
                                        {[
                                            { id: 'light', label: 'Light' },
                                            { id: 'dark', label: 'Dark' },
                                            { id: 'system', label: 'System' },
                                        ].map((theme) => (
                                            <button
                                                key={theme.id}
                                                onClick={() => handleThemeChange(theme.id as 'light' | 'dark' | 'system')}
                                                className={cn(
                                                    'flex items-center justify-center py-1.5 px-3 rounded-lg border transition-all',
                                                    themePreference === theme.id
                                                        ? 'bg-emerald-600/20 border-emerald-500 text-emerald-400 font-medium'
                                                        : 'bg-gray-800/30 border-gray-700 text-gray-400 hover:border-gray-600'
                                                )}
                                            >
                                                <span className="text-[12px]">{theme.label}</span>
                                            </button>
                                        ))}
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* Preferences Section */}
                        {activeSection === 'preferences' && (
                            <div className="space-y-3">
                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <h3 className="text-sm font-semibold text-white mb-3">Query Behavior</h3>
                                    <div className="space-y-1.5">
                                        <SettingToggle
                                            label="Show Reasoning Steps"
                                            description="Display AI thinking process"
                                            enabled={settings.showReasoningSteps}
                                            onChange={(v) => updateSetting('showReasoningSteps', v)}
                                        />
                                        <SettingToggle
                                            label="Show SQL Preview"
                                            description="Preview SQL before execution"
                                            enabled={settings.showSQLPreview}
                                            onChange={(v) => updateSetting('showSQLPreview', v)}
                                        />
                                        <SettingToggle
                                            label="Show Query Timer"
                                            description="Display execution time"
                                            enabled={settings.showQueryTimer}
                                            onChange={(v) => updateSetting('showQueryTimer', v)}
                                        />
                                    </div>
                                </div>

                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <h3 className="text-sm font-semibold text-white mb-3">Approval Settings</h3>
                                    <div className="space-y-1.5">
                                        <SettingToggle
                                            label="Auto-approve Simple Queries"
                                            description="Skip manual approval for low-risk SELECTs (Admin only)"
                                            enabled={settings.autoApproveSimple}
                                            onChange={(v) => updateSetting('autoApproveSimple', v)}
                                        />
                                    </div>
                                </div>

                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <h3 className="text-sm font-semibold text-white mb-3">Notifications</h3>
                                    <div className="space-y-1.5">
                                        <SettingToggle
                                            label="Enable Notifications"
                                            description="Show browser notifications"
                                            enabled={settings.enableNotifications}
                                            onChange={(v) => updateSetting('enableNotifications', v)}
                                        />
                                        <SettingToggle
                                            label="Sound Effects"
                                            description="Play audio feedback"
                                            enabled={settings.enableSoundEffects}
                                            onChange={(v) => updateSetting('enableSoundEffects', v)}
                                        />
                                    </div>
                                </div>

                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <h3 className="text-sm font-semibold text-white mb-3">Developer</h3>
                                    <div className="space-y-1.5">
                                        <SettingToggle
                                            label="Developer Mode"
                                            description="Show debugging info and raw API responses"
                                            enabled={settings.developerMode}
                                            onChange={(v) => updateSetting('developerMode', v)}
                                        />
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* Connections Section */}
                        {activeSection === 'connections' && (
                            <div className="space-y-3">
                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <div className="flex items-center justify-between mb-3">
                                        <h3 className="text-sm font-semibold text-white">System Status</h3>
                                        <button
                                            onClick={handleRefresh}
                                            disabled={isRefreshing}
                                            className={cn(
                                                'flex items-center gap-1.5 px-2 py-1 rounded-md text-[11px] transition-all',
                                                'bg-gray-700/50 text-gray-300 hover:bg-gray-700 hover:text-white',
                                                isRefreshing && 'opacity-50 cursor-not-allowed'
                                            )}
                                        >
                                            <RefreshCw className={cn('w-3 h-3', isRefreshing && 'animate-spin')} />
                                            Refresh
                                        </button>
                                    </div>

                                    {lastCheck && (
                                        <p className="text-[10px] text-gray-500 mb-2">
                                            Last checked: {lastCheck.toLocaleTimeString()}
                                        </p>
                                    )}

                                    <div className="space-y-1.5">
                                        <ConnectionStatus
                                            name="Oracle Database"
                                            status={getStatus(components?.sqlcl_pool) || components?.mcp_client || 'unknown'}
                                            details="SQLcl MCP Connection Pool"
                                        />
                                        <ConnectionStatus
                                            name="Doris Database"
                                            status={getStatus(components?.doris) || 'unknown'}
                                            details="HTTP MCP Connection"
                                        />
                                        <ConnectionStatus
                                            name="Redis"
                                            status={getStatus(components?.redis) || 'unknown'}
                                            details="Cache, Sessions, Celery"
                                        />
                                        <ConnectionStatus
                                            name="Graphiti"
                                            status={getStatus(components?.graphiti) || getStatus(components?.falkordb) || 'unknown'}
                                            details="Knowledge Graph Storage"
                                        />
                                        <ConnectionStatus
                                            name="LLM Provider"
                                            status={getStatus(components?.llm) || 'unknown'}
                                            details="Gemini / Bedrock / OpenRouter"
                                        />
                                    </div>
                                </div>

                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <h3 className="text-sm font-semibold text-white mb-2">Connection Configuration</h3>
                                    <p className="text-gray-400 text-[11px] leading-relaxed">
                                        Managed by administrator.
                                    </p>
                                </div>
                            </div>
                        )}

                        {/* Tools Section */}
                        {activeSection === 'tools' && (
                            <div className="space-y-3">
                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <div className="flex items-center justify-between mb-4">
                                        <div>
                                            <h3 className="text-sm font-semibold text-white">MCP Tools</h3>
                                            <p className="text-[10px] text-gray-500">Connected Model Context Protocol capabilities</p>
                                        </div>
                                        <Terminal className="w-4 h-4 text-emerald-400" />
                                    </div>

                                    {!mcpData ? (
                                        <div className="flex items-center justify-center py-8">
                                            <RefreshCw className="w-5 h-5 text-gray-600 animate-spin" />
                                        </div>
                                    ) : (
                                        <div className="space-y-2">
                                            {Object.entries(mcpData.servers || {}).map(([id, server]: [string, any]) => (
                                                <div key={id} className="border border-gray-700/50 rounded-lg overflow-hidden bg-gray-900/30">
                                                    <button
                                                        onClick={() => toggleServer(id)}
                                                        className="w-full flex items-center justify-between p-2.5 hover:bg-gray-800/40 transition-colors"
                                                    >
                                                        <div className="flex items-center gap-2">
                                                            <Box className="w-3.5 h-3.5 text-emerald-500" />
                                                            <span className="text-[12px] font-medium text-gray-200">{server.server_name}</span>
                                                            <span className={cn(
                                                                "text-[8px] px-1.5 py-0.5 rounded-full uppercase tracking-wider font-bold",
                                                                server.server_status === 'connected' ? "bg-emerald-500/10 text-emerald-400" : "bg-red-500/10 text-red-400"
                                                            )}>
                                                                {server.server_status}
                                                            </span>
                                                        </div>
                                                        {expandedServers[id] ? <ChevronUp className="w-3.5 h-3.5 text-gray-500" /> : <ChevronDown className="w-3.5 h-3.5 text-gray-500" />}
                                                    </button>

                                                    {expandedServers[id] && (
                                                        <div className="p-2 pt-0 border-t border-gray-800/50">
                                                            <div className="grid grid-cols-1 gap-1 mt-2">
                                                                {server.tools?.length > 0 ? (
                                                                    server.tools.map((tool: any) => (
                                                                        <div key={tool.name} className="flex items-start gap-2 p-1.5 rounded bg-black/20 border border-gray-800/30">
                                                                            <Terminal className="w-3 h-3 text-gray-500 mt-0.5 flex-shrink-0" />
                                                                            <div className="min-w-0">
                                                                                <div className="flex items-center gap-2">
                                                                                    <span className="text-[11px] font-mono text-emerald-400 truncate">{tool.name}</span>
                                                                                    {tool.status === 'available' ? (
                                                                                        <span className="w-1 h-1 rounded-full bg-emerald-500 shadow-[0_0_4px_rgba(16,185,129,0.5)]" />
                                                                                    ) : (
                                                                                        <span className="w-1 h-1 rounded-full bg-red-500" />
                                                                                    )}
                                                                                </div>
                                                                                {tool.description && (
                                                                                    <p className="text-[9px] text-gray-500 leading-tight truncate">{tool.description}</p>
                                                                                )}
                                                                            </div>
                                                                        </div>
                                                                    ))
                                                                ) : (
                                                                    <p className="text-center py-2 text-[10px] text-gray-600">No tools discovered</p>
                                                                )}
                                                            </div>
                                                        </div>
                                                    )}
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            </div>
                        )}

                        {/* About Section */}
                        {activeSection === 'about' && (
                            <div className="space-y-3">
                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-8 flex flex-col items-center text-center">
                                    <div className="mb-6 relative">
                                        <div className="absolute -inset-1 bg-gradient-to-r from-emerald-500 to-green-600 rounded-full blur opacity-25"></div>
                                        <img
                                            src="/amila_circular_logo.png"
                                            alt="Amila Logo"
                                            className="w-24 h-24 rounded-full relative border-2 border-gray-700 shadow-2xl"
                                        />
                                    </div>
                                    <h2 className="text-2xl font-bold bg-gradient-to-r from-emerald-400 to-green-500 bg-clip-text text-transparent mb-2">
                                        Amila
                                    </h2>
                                    <p className="text-gray-400 text-sm mb-6 font-medium">Ask. Understand. Act</p>

                                    <div className="w-full max-w-sm space-y-4 text-left border-t border-gray-700/50 pt-6">
                                        <div>
                                            <p className="text-gray-300 text-[12px] leading-relaxed">
                                                Amila is an advanced business intelligence and analytics helper supporting Oracle, Apache Doris and PostgreSQL database services through tool-based interaction.
                                            </p>
                                        </div>
                                    </div>

                                    <div className="mt-8 flex gap-4">
                                        <button className="flex items-center gap-2 px-3 py-1.5 rounded-md bg-gray-800 border border-gray-700 text-gray-300 hover:text-white transition-colors text-[11px]">
                                            <Info className="w-3.5 h-3.5" />
                                            Documentation
                                        </button>
                                        <button className="flex items-center gap-2 px-3 py-1.5 rounded-md bg-gray-800 border border-gray-700 text-gray-300 hover:text-white transition-colors text-[11px]">
                                            <ExternalLink className="w-3.5 h-3.5" />
                                            Support
                                        </button>
                                    </div>

                                    <p className="mt-12 text-[10px] text-gray-600">
                                        Released under Zuri
                                        <br />
                                        Version 0.0.8 • Build 202601
                                    </p>
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    )
}
