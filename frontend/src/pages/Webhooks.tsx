import { useEffect, useMemo, useState } from 'react'
import { apiService } from '@/services/apiService'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Badge } from '@/components/ui/badge'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'

interface WebhookItem {
  webhook_id: string
  user_id?: string
  url: string
  events: string[]
  active: boolean
  secret?: string | null
  created_at: string
  updated_at: string
  last_delivery_at?: string | null | undefined
  last_status_code?: number | null
  last_error?: string | null
  consecutive_failures?: number
}

export function Webhooks() {
  const [items, setItems] = useState<WebhookItem[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [createOpen, setCreateOpen] = useState(false)
  const [url, setUrl] = useState('')
  const [events, setEvents] = useState('query.finished\nquery.error\nquery.rejected')
  const [secret, setSecret] = useState('')

  const parsedEvents = useMemo(() => {
    return events
      .split(/\r?\n/)
      .map(s => s.trim())
      .filter(Boolean)
  }, [events])

  const load = async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await apiService.listWebhooks()
      setItems(res.webhooks || [])
    } catch (e: any) {
      setError(e?.message || 'Failed to load webhooks')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
  }, [])

  const onCreate = async () => {
    setLoading(true)
    setError(null)
    try {
      await apiService.createWebhook({ url, events: parsedEvents, secret: secret || undefined, active: true })
      setCreateOpen(false)
      setUrl('')
      setSecret('')
      await load()
    } catch (e: any) {
      setError(e?.message || 'Failed to create webhook')
    } finally {
      setLoading(false)
    }
  }

  const onDelete = async (webhookId: string) => {
    setLoading(true)
    setError(null)
    try {
      await apiService.deleteWebhook(webhookId)
      await load()
    } catch (e: any) {
      setError(e?.message || 'Failed to delete webhook')
    } finally {
      setLoading(false)
    }
  }

  const onTest = async (webhookId: string) => {
    setLoading(true)
    setError(null)
    try {
      await apiService.testWebhook(webhookId)
      await load()
    } catch (e: any) {
      setError(e?.message || 'Failed to test webhook')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="h-full flex flex-col bg-black text-white p-6 overflow-y-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Webhooks</h1>
          <p className="text-gray-400 text-sm mt-1">Receive query terminal events as signed HTTP POST callbacks.</p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={load} disabled={loading}>Refresh</Button>
          <Button onClick={() => setCreateOpen(true)} disabled={loading}>Add Webhook</Button>
        </div>
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
          <CardTitle>Subscriptions</CardTitle>
          <CardDescription className="text-gray-400">Endpoints are managed per-user.</CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow className="border-gray-800 hover:bg-transparent">
                <TableHead className="text-gray-400">ID</TableHead>
                <TableHead className="text-gray-400">URL</TableHead>
                <TableHead className="text-gray-400">Events</TableHead>
                <TableHead className="text-gray-400">Status</TableHead>
                <TableHead className="text-gray-400">Last Delivery</TableHead>
                <TableHead className="text-gray-400">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.map((w) => (
                <TableRow key={w.webhook_id} className="border-gray-800 hover:bg-gray-800/50">
                  <TableCell className="font-mono text-xs text-gray-300">{w.webhook_id}</TableCell>
                  <TableCell className="text-gray-300 font-mono text-xs truncate max-w-[360px]">{w.url}</TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-1">
                      {(w.events || []).slice(0, 4).map((e) => (
                        <Badge key={e} variant="outline" className="border-gray-700 text-gray-300">{e}</Badge>
                      ))}
                      {(w.events || []).length > 4 && (
                        <Badge variant="secondary" className="bg-gray-800 text-gray-300">+{(w.events || []).length - 4}</Badge>
                      )}
                    </div>
                  </TableCell>
                  <TableCell>
                    {w.active ? (
                      <Badge className="bg-emerald-900/40 text-emerald-200 border border-emerald-800">ACTIVE</Badge>
                    ) : (
                      <Badge variant="secondary" className="bg-gray-800 text-gray-300">DISABLED</Badge>
                    )}
                  </TableCell>
                  <TableCell className="text-gray-300 text-xs">
                    {w.last_delivery_at ? new Date(w.last_delivery_at).toLocaleString() : '-'}
                    {typeof w.last_status_code === 'number' && (
                      <span className="ml-2 text-gray-500">({w.last_status_code})</span>
                    )}
                    {w.last_error && (
                      <div className="text-red-400 text-[11px] truncate max-w-[240px]">{w.last_error}</div>
                    )}
                  </TableCell>
                  <TableCell>
                    <div className="flex gap-2">
                      <Button size="sm" variant="outline" onClick={() => onTest(w.webhook_id)} disabled={loading}>Test</Button>
                      <Button size="sm" variant="destructive" onClick={() => onDelete(w.webhook_id)} disabled={loading}>Delete</Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
              {items.length === 0 && (
                <TableRow className="border-gray-800">
                  <TableCell colSpan={6} className="text-gray-400 text-sm">No webhooks configured.</TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent className="bg-gray-900 border-gray-800 text-white">
          <DialogHeader>
            <DialogTitle>Add Webhook</DialogTitle>
          </DialogHeader>
          <div className="space-y-3">
            <div>
              <div className="text-xs text-gray-400 mb-1">URL</div>
              <Input value={url} onChange={(e) => setUrl(e.target.value)} placeholder="https://example.com/webhook" />
            </div>
            <div>
              <div className="text-xs text-gray-400 mb-1">Events (one per line)</div>
              <Textarea value={events} onChange={(e) => setEvents(e.target.value)} rows={6} />
            </div>
            <div>
              <div className="text-xs text-gray-400 mb-1">Secret (optional)</div>
              <Input value={secret} onChange={(e) => setSecret(e.target.value)} placeholder="Overrides global signing secret" />
            </div>
            <div className="flex justify-end gap-2">
              <Button variant="outline" onClick={() => setCreateOpen(false)} disabled={loading}>Cancel</Button>
              <Button onClick={onCreate} disabled={loading || !url.trim()}>Create</Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}
