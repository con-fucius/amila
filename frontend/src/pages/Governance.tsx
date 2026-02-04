
import { useEffect, useState } from 'react'
import {
    Tabs,
    TabsContent,
    TabsList,
    TabsTrigger
} from '@/components/ui/tabs'
import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
    CardDescription
} from '@/components/ui/card'
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow
} from '@/components/ui/table'
import { Alert, AlertTitle, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { apiService } from '@/services/apiService'
import {
    AlertTriangle,
    Shield,
    Database,
    Activity,
    CheckCircle,
    XCircle
} from 'lucide-react'

export function Governance() {

    const [summary, setSummary] = useState<any>(null)
    const [auditLogs, setAuditLogs] = useState<any[]>([])
    const [agents, setAgents] = useState<any[]>([])
    const [systems, setSystems] = useState<any[]>([])
    const [misconfigs, setMisconfigs] = useState<any>(null)
    const [isLoading, setIsLoading] = useState(true)

    const [userProfile, setUserProfile] = useState<any>(null)

    useEffect(() => {
        try {
            const token = localStorage.getItem('access_token')
            if (token) {
                const parts = token.split('.')
                if (parts.length >= 2) {
                    const payload = JSON.parse(atob(parts[1].replace(/-/g, '+').replace(/_/g, '/')))
                    setUserProfile(payload)
                }
            }
        } catch (e) { console.error(e) }
        fetchData()
    }, [])

    const fetchData = async () => {
        setIsLoading(true)
        try {
            const [sum, logs, agt, sys, misc] = await Promise.all([
                apiService.getGovernanceAuditSummary(),
                apiService.getGovernanceAuditActivity(50),
                apiService.getAgentCapabilities(),
                apiService.getSystemCapabilities(),
                apiService.getMisconfigurations()
            ])

            setSummary(sum)
            setAuditLogs(logs.logs || [])
            setAgents(agt)
            setSystems(sys)
            setMisconfigs(misc)
        } catch (e) {
            console.error("Failed to fetch governance data", e)
        } finally {
            setIsLoading(false)
        }
    }

    if (isLoading) {
        return <div className="p-8 text-center text-gray-400">Loading governance data...</div>
    }

    // Admin access check
    const role = userProfile?.role || userProfile?.roles?.[0]
    if (role !== 'Admin' && role !== 'admin') {
        return (
            <div className="h-full flex items-center justify-center bg-black text-white">
                <div className="text-center p-8 border border-gray-800 rounded-lg bg-gray-900/50">
                    <Shield className="w-12 h-12 text-gray-600 mx-auto mb-4" />
                    <h2 className="text-xl font-bold text-gray-300">Access Restricted</h2>
                    <p className="text-gray-500 mt-2">Governance controls are limited to administrators.</p>
                </div>
            </div>
        )
    }

    return (
        <div className="h-full flex flex-col bg-black text-white p-6 overflow-y-auto">
            <div className="flex items-center justify-between mb-8">
                <div>
                    <h1 className="text-3xl font-bold bg-gradient-to-r from-emerald-400 to-cyan-500 bg-clip-text text-transparent">
                        Governance & Compliance
                    </h1>
                    <p className="text-gray-400 mt-2">
                        Centralized monitoring of agent permissions, system access, and audit trails.
                    </p>
                </div>
                <div className="flex gap-2">
                    <Badge variant={misconfigs?.overall_status === 'HEALTHY' ? 'default' : 'destructive'} className="text-sm px-3 py-1">
                        System Status: {misconfigs?.overall_status || 'UNKNOWN'}
                    </Badge>
                </div>
            </div>

            <Tabs defaultValue="audit" className="w-full">
                <TabsList className="grid w-full grid-cols-3 max-w-md mb-8 bg-gray-900 border border-gray-800">
                    <TabsTrigger value="audit" className="data-[state=active]:bg-gray-800">
                        <Activity className="w-4 h-4 mr-2" />
                        Audit Trail
                    </TabsTrigger>
                    <TabsTrigger value="capabilities" className="data-[state=active]:bg-gray-800">
                        <Shield className="w-4 h-4 mr-2" />
                        Capabilities
                    </TabsTrigger>
                    <TabsTrigger value="misconfigurations" className="data-[state=active]:bg-gray-800">
                        <AlertTriangle className="w-4 h-4 mr-2" />
                        Risks & Alerts
                    </TabsTrigger>
                </TabsList>

                <TabsContent value="audit" className="space-y-6">
                    {/* Summary Cards */}
                    <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                        <Card className="bg-gray-900 border-gray-800 text-white">
                            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                                <CardTitle className="text-sm font-medium">Total Actions</CardTitle>
                                <Activity className="h-4 w-4 text-emerald-400" />
                            </CardHeader>
                            <CardContent>
                                <div className="text-2xl font-bold">{summary?.total_actions || 0}</div>
                                <p className="text-xs text-gray-400">Recorded events today</p>
                            </CardContent>
                        </Card>
                        <Card className="bg-gray-900 border-gray-800 text-white">
                            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                                <CardTitle className="text-sm font-medium">Query Executions</CardTitle>
                                <Database className="h-4 w-4 text-emerald-400" />
                            </CardHeader>
                            <CardContent>
                                <div className="text-2xl font-bold">{summary?.query_executions || 0}</div>
                                <p className="text-xs text-gray-400">SQL queries run</p>
                            </CardContent>
                        </Card>
                        <Card className="bg-gray-900 border-gray-800 text-white">
                            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                                <CardTitle className="text-sm font-medium">Schema Changes</CardTitle>
                                <AlertTriangle className="h-4 w-4 text-yellow-500" />
                            </CardHeader>
                            <CardContent>
                                <div className="text-2xl font-bold">{summary?.schema_modifications || 0}</div>
                                <p className="text-xs text-gray-400">DDL operations</p>
                            </CardContent>
                        </Card>
                        <Card className="bg-gray-900 border-gray-800 text-white">
                            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                                <CardTitle className="text-sm font-medium">Errors</CardTitle>
                                <XCircle className="h-4 w-4 text-red-500" />
                            </CardHeader>
                            <CardContent>
                                <div className="text-2xl font-bold">{summary?.errors || 0}</div>
                                <p className="text-xs text-gray-400">Failed operations</p>
                            </CardContent>
                        </Card>
                    </div>

                    {/* Activity Table */}
                    <Card className="bg-gray-900 border-gray-800 text-white">
                        <CardHeader>
                            <CardTitle>Recent Activity</CardTitle>
                            <CardDescription className="text-gray-400">
                                Latest audit logs from {summary?.source === 'native' ? 'Native Database' : 'Redis Cache'}
                            </CardDescription>
                        </CardHeader>
                        <CardContent>
                            <Table>
                                <TableHeader>
                                    <TableRow className="border-gray-800 hover:bg-transparent">
                                        <TableHead className="text-gray-400">Timestamp</TableHead>
                                        <TableHead className="text-gray-400">Action</TableHead>
                                        <TableHead className="text-gray-400">User</TableHead>
                                        <TableHead className="text-gray-400">Resource</TableHead>
                                        <TableHead className="text-gray-400">Status</TableHead>
                                        <TableHead className="text-gray-400">Risk</TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {auditLogs.map((log, i) => (
                                        <TableRow key={i} className="border-gray-800 hover:bg-gray-800/50">
                                            <TableCell className="font-mono text-xs text-gray-300">
                                                {new Date(log.timestamp).toLocaleString()}
                                            </TableCell>
                                            <TableCell>
                                                <Badge variant="outline" className="border-gray-700 text-gray-300">
                                                    {log.action}
                                                </Badge>
                                            </TableCell>
                                            <TableCell className="text-gray-300">{log.user || log.user_id}</TableCell>
                                            <TableCell className="text-gray-300 font-mono text-xs truncate max-w-[200px]">
                                                {log.resource_type ? `${log.resource_type}:${log.resource_id}` : '-'}
                                            </TableCell>
                                            <TableCell>
                                                {log.success ? (
                                                    <span className="flex items-center text-emerald-400 text-xs gap-1">
                                                        <CheckCircle className="w-3 h-3" /> Success
                                                    </span>
                                                ) : (
                                                    <span className="flex items-center text-red-400 text-xs gap-1">
                                                        <XCircle className="w-3 h-3" /> Failed
                                                    </span>
                                                )}
                                            </TableCell>
                                            <TableCell>
                                                {log.risk_metrics?.level === 'critical' ? (
                                                    <Badge variant="destructive" className="bg-red-900/50 text-red-200 border-red-800">CRITICAL</Badge>
                                                ) : log.risk_metrics?.level === 'medium' ? (
                                                    <Badge variant="secondary" className="bg-yellow-900/50 text-yellow-200 border-yellow-800">MEDIUM</Badge>
                                                ) : (
                                                    <span className="text-gray-500 text-xs">Low</span>
                                                )}
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </CardContent>
                    </Card>
                </TabsContent>

                <TabsContent value="capabilities" className="space-y-6">
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        {/* Systems */}
                        <Card className="bg-gray-900 border-gray-800 text-white">
                            <CardHeader>
                                <CardTitle>System Capabilities</CardTitle>
                                <CardDescription>Managed infrastructure and access controls</CardDescription>
                            </CardHeader>
                            <CardContent className="space-y-4">
                                {systems.map((sys, i) => (
                                    <div key={i} className="p-4 rounded-lg bg-gray-950 border border-gray-800">
                                        <div className="flex justify-between items-start mb-2">
                                            <div>
                                                <h4 className="font-semibold text-emerald-400">{sys.system_name}</h4>
                                                <p className="text-xs text-gray-500">{sys.system_type}</p>
                                            </div>
                                            <Badge variant={sys.connection_status === 'active' ? 'default' : 'secondary'}
                                                className={sys.connection_status === 'active' ? 'bg-emerald-900/50 text-emerald-200 hover:bg-emerald-900/50' : 'bg-gray-800 text-gray-400'}>
                                                {sys.connection_status}
                                            </Badge>
                                        </div>
                                        <div className="mt-2 text-xs text-gray-400">
                                            <div className="mb-1"><strong>Accessible by:</strong> {sys.accessible_by.join(", ")}</div>
                                            <div><strong>Allowed Ops:</strong> {sys.operations_allowed.join(", ")}</div>
                                        </div>
                                    </div>
                                ))}
                            </CardContent>
                        </Card>

                        {/* Agents */}
                        <Card className="bg-gray-900 border-gray-800 text-white">
                            <CardHeader>
                                <CardTitle>Agent Permissions</CardTitle>
                                <CardDescription>AI Agent capabilities and risk levels</CardDescription>
                            </CardHeader>
                            <CardContent className="space-y-4">
                                {agents.map((agent, i) => (
                                    <div key={i} className="p-4 rounded-lg bg-gray-950 border border-gray-800">
                                        <div className="flex justify-between items-start mb-2">
                                            <div>
                                                <h4 className="font-semibold text-blue-400">{agent.agent_name}</h4>
                                                <p className="text-xs text-gray-500">{agent.agent_type}</p>
                                            </div>
                                            <Badge className={
                                                agent.risk_level === 'HIGH' ? 'bg-red-900/50 text-red-200' :
                                                    agent.risk_level === 'MEDIUM' ? 'bg-yellow-900/50 text-yellow-200' :
                                                        'bg-blue-900/50 text-blue-200'
                                            }>
                                                {agent.risk_level} RISK
                                            </Badge>
                                        </div>
                                        <div className="mt-2 text-xs text-gray-400 space-y-1">
                                            <div><strong>Permissions:</strong> {agent.permissions.join(", ")}</div>
                                            <div><strong>Databases:</strong> {agent.databases.join(", ") || "None"}</div>
                                        </div>
                                    </div>
                                ))}
                            </CardContent>
                        </Card>
                    </div>
                </TabsContent>

                <TabsContent value="misconfigurations" className="space-y-6">
                    <Card className="bg-gray-900 border-gray-800 text-white">
                        <CardHeader>
                            <CardTitle>Security & Configuration Analysis</CardTitle>
                            <CardDescription>
                                Automated detection of potential risks and misconfigurations
                            </CardDescription>
                        </CardHeader>
                        <CardContent className="space-y-4">
                            {misconfigs?.issues?.length === 0 && misconfigs?.warnings?.length === 0 && (
                                <Alert className="bg-emerald-900/20 border-emerald-900 text-emerald-200">
                                    <CheckCircle className="h-4 w-4" />
                                    <AlertTitle>All Systems Healthy</AlertTitle>
                                    <AlertDescription>No critical misconfigurations detected.</AlertDescription>
                                </Alert>
                            )}

                            {misconfigs?.issues?.map((issue: any, i: number) => (
                                <Alert key={`issue-${i}`} variant="destructive" className="bg-red-900/20 border-red-900 text-red-200">
                                    <AlertTriangle className="h-4 w-4" />
                                    <AlertTitle className="font-semibold">CRITICAL: {issue.issue}</AlertTitle>
                                    <AlertDescription>
                                        <p className="mt-1">{issue.agent ? `Agent: ${issue.agent}` : ''}</p>
                                        <p className="font-semibold mt-1">Recommendation: {issue.recommendation}</p>
                                    </AlertDescription>
                                </Alert>
                            ))}

                            {misconfigs?.warnings?.map((warn: any, i: number) => (
                                <Alert key={`warn-${i}`} className="bg-yellow-900/20 border-yellow-900 text-yellow-200">
                                    <AlertTriangle className="h-4 w-4" />
                                    <AlertTitle>Warning: {warn.issue}</AlertTitle>
                                    <AlertDescription>
                                        <p className="mt-1">{warn.agent ? `Agent: ${warn.agent}` : ''}</p>
                                        <p className="font-semibold mt-1">Recommendation: {warn.recommendation}</p>
                                    </AlertDescription>
                                </Alert>
                            ))}
                        </CardContent>
                    </Card>
                </TabsContent>
            </Tabs>
        </div>
    )
}
