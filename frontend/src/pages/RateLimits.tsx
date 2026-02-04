import { useEffect, useMemo, useState } from 'react'
import { apiService } from '@/services/apiService'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Badge } from '@/components/ui/badge'

interface EndpointStatus {
  limit: number
  remaining: number
  used: number
  window_seconds: number
  tier: string
  error?: string
}

export function RateLimits() {
  const [data, setData] = useState<Record<string, EndpointStatus>>({})
  const [tier, setTier] = useState<string>('viewer')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const endpoints = useMemo(() => [
    '/api/v1/queries/process',
    '/api/v1/queries/submit',
    '/api/v1/queries/connections',
    '/api/v1/diagnostics/status',
  ], [])

  const load = async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await apiService.getRateLimitStatus(endpoints)
      setTier(res.tier)
      setData(res.endpoints || {})
    } catch (e: any) {
      setError(e?.message || 'Failed to load rate limit status')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
  }, [])

  return (
    <div className="h-full flex flex-col bg-black text-white p-6 overflow-y-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Rate Limits</h1>
          <p className="text-gray-400 text-sm mt-1">Your current per-endpoint limits and usage.</p>
        </div>
        <Button variant="outline" onClick={load} disabled={loading}>Refresh</Button>
      </div>

      {error && (
        <Card className="bg-gray-900 border-gray-800 text-white mb-4">
          <CardHeader>
            <CardTitle>Error</CardTitle>
            <CardDescription className="text-red-400">{error}</CardDescription>
          </CardHeader>
        </Card>
      )}

      <Card className="bg-gray-900 border-gray-800 text-white">
        <CardHeader>
          <CardTitle>Status</CardTitle>
          <CardDescription className="text-gray-400">Tier: {tier}</CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow className="border-gray-800 hover:bg-transparent">
                <TableHead className="text-gray-400">Endpoint</TableHead>
                <TableHead className="text-gray-400">Remaining</TableHead>
                <TableHead className="text-gray-400">Used</TableHead>
                <TableHead className="text-gray-400">Limit</TableHead>
                <TableHead className="text-gray-400">Window</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {endpoints.map((ep) => {
                const s = data[ep]
                const remaining = s?.remaining ?? 0
                const limit = s?.limit ?? 0
                const ratio = limit > 0 ? remaining / limit : 0
                const badge = ratio > 0.5 ? 'ok' : (ratio > 0.2 ? 'warn' : 'crit')
                return (
                  <TableRow key={ep} className="border-gray-800 hover:bg-gray-800/50">
                    <TableCell className="text-gray-300 font-mono text-xs">{ep}</TableCell>
                    <TableCell>
                      {s?.error ? (
                        <Badge variant="destructive">ERROR</Badge>
                      ) : badge === 'ok' ? (
                        <Badge className="bg-emerald-900/40 text-emerald-200 border border-emerald-800">{remaining}</Badge>
                      ) : badge === 'warn' ? (
                        <Badge className="bg-yellow-900/40 text-yellow-200 border border-yellow-800">{remaining}</Badge>
                      ) : (
                        <Badge className="bg-red-900/40 text-red-200 border border-red-800">{remaining}</Badge>
                      )}
                    </TableCell>
                    <TableCell className="text-gray-300">{s?.used ?? '-'}</TableCell>
                    <TableCell className="text-gray-300">{s?.limit ?? '-'}</TableCell>
                    <TableCell className="text-gray-300">{s?.window_seconds ?? '-'}</TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  )
}
