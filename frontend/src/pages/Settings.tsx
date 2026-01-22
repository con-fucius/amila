import { useState, useContext, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import {
    ChevronLeft,
    Check,
    RefreshCw,
    Mail,
    Shield,
    Building2,
    LogOut
} from 'lucide-react'
import { cn } from '@/utils/cn'
import { ColorModeContext } from '@/App'
import { useBackendHealth } from '@/hooks/useBackendHealth'

type SettingsSection = 'account' | 'appearance' | 'preferences' | 'connections'

interface SettingToggleProps {
    label: string
    description: string
    enabled: boolean
    onChange: (enabled: boolean) => void
    icon?: React.ReactNode
}

function SettingToggle({ label, description, enabled, onChange }: SettingToggleProps) {
    return (
        <div className="flex items-center justify-between p-1.5 rounded-md bg-gray-800/30 hover:bg-gray-800/50 transition-colors">
            <div className="flex items-center gap-2">
                <div>
                    <p className="font-medium text-white text-[13px] leading-tight">{label}</p>
                    <p className="text-[11px] text-gray-500 leading-tight">{description}</p>
                </div>
            </div>
            <button
                onClick={() => onChange(!enabled)}
                className={cn(
                    'relative w-8 h-4.5 rounded-full transition-colors flex-shrink-0 ml-3',
                    enabled ? 'bg-emerald-500' : 'bg-gray-600'
                )}
            >
                <span
                    className={cn(
                        'absolute top-0.5 left-0.5 w-3.5 h-3.5 rounded-full bg-white transition-transform shadow-sm',
                        enabled && 'translate-x-3.5'
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
        <div className="flex items-center justify-between p-1.5 rounded-md bg-gray-800/30">
            <div className="flex items-center gap-2">
                <div>
                    <div className="flex items-center gap-2">
                        <p className="font-medium text-white text-[13px]">{name}</p>
                        <span className={cn(
                            'text-[9px] px-1 py-0 rounded-full capitalize border',
                            isHealthy
                                ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
                                : 'bg-red-500/10 text-red-400 border-red-500/20'
                        )}>
                            {status || 'unknown'}
                        </span>
                    </div>
                    {details && <p className="text-[10px] text-gray-500 leading-tight">{details}</p>}
                </div>
            </div>
            <div className="flex items-center gap-2">
                <span className={cn(
                    'w-1.5 h-1.5 rounded-full',
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
                                    <h3 className="text-sm font-semibold text-white mb-3">Profile Details</h3>
                                    <div className="space-y-2">
                                        <div className="flex items-center gap-3 p-2 rounded-lg bg-gray-700/30">
                                            <Mail className="w-4 h-4 text-emerald-400" />
                                            <div>
                                                <p className="text-[10px] text-gray-400">Email</p>
                                                <p className="text-white text-[12px]">{profile.email}</p>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-3 p-2 rounded-lg bg-gray-700/30">
                                            <Shield className="w-4 h-4 text-emerald-400" />
                                            <div>
                                                <p className="text-[10px] text-gray-400">Role</p>
                                                <p className="text-white text-[12px]">{profile.role}</p>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-3 p-2 rounded-lg bg-gray-700/30">
                                            <Building2 className="w-4 h-4 text-emerald-400" />
                                            <div>
                                                <p className="text-[10px] text-gray-400">Department</p>
                                                <p className="text-white text-[12px]">{profile.department}</p>
                                            </div>
                                        </div>
                                    </div>
                                </div>

                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <button
                                        onClick={handleLogout}
                                        className="w-full flex items-center justify-center gap-2 py-2 rounded-lg bg-red-900/30 border border-red-700/50 text-red-300 hover:bg-red-900/50 transition-colors text-[12px]"
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
                                    <h3 className="text-sm font-semibold text-white mb-3">Theme</h3>
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
                                                    'flex flex-col items-center gap-1.5 p-2 rounded-lg border transition-all',
                                                    themePreference === theme.id
                                                        ? 'bg-emerald-600/20 border-emerald-500 text-emerald-400'
                                                        : 'bg-gray-800/30 border-gray-700 text-gray-400 hover:border-gray-600'
                                                )}
                                            >
                                                <span className="text-[12px] font-medium">{theme.label}</span>
                                                {themePreference === theme.id && (
                                                    <Check className="w-3 h-3 text-emerald-400" />
                                                )}
                                            </button>
                                        ))}
                                    </div>
                                </div>

                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <h3 className="text-sm font-semibold text-white mb-3">Display</h3>
                                    <div className="space-y-1.5">
                                        <SettingToggle
                                            label="Compact Mode"
                                            description="Reduce spacing and use smaller fonts"
                                            enabled={settings.compactMode}
                                            onChange={(v) => updateSetting('compactMode', v)}
                                        />
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
                                            status={components?.sqlcl_pool || components?.mcp_client || 'unknown'}
                                            details="SQLcl MCP Connection Pool"
                                        />
                                        <ConnectionStatus
                                            name="Doris Database"
                                            status={components?.doris_mcp || 'unknown'}
                                            details="HTTP MCP Connection"
                                        />
                                        <ConnectionStatus
                                            name="Redis"
                                            status={components?.redis || 'unknown'}
                                            details="Cache, Sessions, Celery"
                                        />
                                        <ConnectionStatus
                                            name="Graphiti"
                                            status={components?.graphiti || components?.falkordb || 'unknown'}
                                            details="Knowledge Graph Storage"
                                        />
                                        <ConnectionStatus
                                            name="LLM Provider"
                                            status={components?.llm || 'unknown'}
                                            details="Gemini / Bedrock / OpenRouter"
                                        />
                                    </div>
                                </div>

                                <div className="bg-gray-800/50 backdrop-blur-sm rounded-xl border border-gray-700 p-4">
                                    <h3 className="text-sm font-semibold text-white mb-2">Connection Configuration</h3>
                                    <p className="text-gray-400 text-[11px] mb-2 leading-relaxed">
                                        Managed by administrator via environment.
                                    </p>
                                    <div className="p-2 rounded-md bg-blue-900/20 border border-blue-700/30">
                                        <p className="text-blue-300 text-[11px]">
                                            Configure via <code className="bg-blue-900/40 px-1 rounded">.env</code>.
                                        </p>
                                    </div>
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    )
}
