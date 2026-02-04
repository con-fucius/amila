import { useState } from 'react'
import { Calendar } from 'lucide-react'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { apiService } from '@/services/apiService'

interface ScheduleDialogProps {
    sql: string
    connection?: string
    databaseType: string
    onSchedule?: (config: any) => void
}

export function ScheduleDialog({ sql, connection, databaseType, onSchedule }: ScheduleDialogProps) {
    const [open, setOpen] = useState(false)
    const [name, setName] = useState('')
    const [frequency, setFrequency] = useState('daily')
    const [time, setTime] = useState('09:00')
    const [format, setFormat] = useState<'html' | 'pdf' | 'docx'>('html')
    const [recipients, setRecipients] = useState('')
    const [loading, setLoading] = useState(false)
    const [success, setSuccess] = useState(false)
    const [error, setError] = useState<string | null>(null)

    const buildCron = () => {
        const [hourStr, minuteStr] = time.split(':')
        const hour = Number(hourStr)
        const minute = Number(minuteStr)
        if (Number.isNaN(hour) || Number.isNaN(minute)) return null

        if (frequency === 'hourly') {
            return `${minute} * * * *`
        }
        if (frequency === 'daily') {
            return `${minute} ${hour} * * *`
        }
        if (frequency === 'weekly') {
            return `${minute} ${hour} * * 1`
        }
        return `${minute} ${hour} 1 * *`
    }

    const handleSchedule = async () => {
        setError(null)
        const trimmedName = name.trim()
        if (!trimmedName) {
            setError('Schedule name is required.')
            return
        }
        if (!sql.trim()) {
            setError('SQL is required to schedule a report.')
            return
        }
        const cron = buildCron()
        if (!cron) {
            setError('Invalid time selection.')
            return
        }
        const recipientList = recipients
            .split(',')
            .map((r) => r.trim())
            .filter(Boolean)
        if (recipientList.length === 0) {
            setError('At least one recipient email is required.')
            return
        }

        setLoading(true)
        try {
            const response = await apiService.createReportSchedule({
                name: trimmedName,
                cron,
                sql_query: sql,
                database_type: databaseType,
                connection_name: connection,
                format,
                recipients: recipientList,
            })

            if (response.status !== 'success') {
                throw new Error(response.message || 'Failed to create schedule.')
            }
            setSuccess(true)
            if (onSchedule) onSchedule({ frequency, time, cron })

            setTimeout(() => {
                setOpen(false)
                setSuccess(false)
            }, 1500)
        } catch (e) {
            console.error('Failed to schedule', e)
            setError((e as Error).message || 'Failed to schedule report.')
        } finally {
            setLoading(false)
        }
    }

    return (
        <Dialog open={open} onOpenChange={setOpen}>
            <DialogTrigger asChild>
                <Button variant="outline" size="sm" className="gap-2">
                    <Calendar className="h-3.5 w-3.5" />
                    Schedule
                </Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-[425px]">
                <DialogHeader>
                    <DialogTitle>Schedule Query Execution</DialogTitle>
                    <DialogDescription>
                        Automate this query to run at specific intervals. Results will be emailed to you.
                    </DialogDescription>
                </DialogHeader>

                {success ? (
                    <div className="py-6 flex flex-col items-center justify-center text-emerald-600">
                        <div className="rounded-full bg-emerald-100 p-3 mb-3">
                            <Calendar className="h-6 w-6" />
                        </div>
                        <p className="font-medium">Query Scheduled Successfully!</p>
                    </div>
                ) : (
                    <div className="grid gap-4 py-4">
                        <div className="grid grid-cols-4 items-center gap-4">
                            <Label htmlFor="schedule-name" className="text-right">Name</Label>
                            <Input
                                id="schedule-name"
                                value={name}
                                onChange={(e) => setName(e.target.value)}
                                placeholder="Weekly revenue summary"
                                className="col-span-3"
                            />
                        </div>

                        <div className="grid grid-cols-4 items-center gap-4">
                            <Label htmlFor="freq" className="text-right">Frequency</Label>
                            <Select value={frequency} onValueChange={setFrequency}>
                                <SelectTrigger className="col-span-3">
                                    <SelectValue placeholder="Select frequency" />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="hourly">Hourly</SelectItem>
                                    <SelectItem value="daily">Daily</SelectItem>
                                    <SelectItem value="weekly">Weekly</SelectItem>
                                    <SelectItem value="monthly">Monthly</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>

                        <div className="grid grid-cols-4 items-center gap-4">
                            <Label htmlFor="time" className="text-right">Time</Label>
                            <Input
                                id="time"
                                type="time"
                                value={time}
                                onChange={(e) => setTime(e.target.value)}
                                className="col-span-3"
                            />
                        </div>

                        <div className="grid grid-cols-4 items-center gap-4">
                            <Label htmlFor="format" className="text-right">Format</Label>
                            <Select value={format} onValueChange={(val) => setFormat(val as 'html' | 'pdf' | 'docx')}>
                                <SelectTrigger className="col-span-3">
                                    <SelectValue placeholder="Select format" />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="html">HTML</SelectItem>
                                    <SelectItem value="pdf">PDF</SelectItem>
                                    <SelectItem value="docx">DOCX</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>

                        <div className="grid grid-cols-4 items-center gap-4">
                            <Label htmlFor="recipients" className="text-right">Recipients</Label>
                            <Input
                                id="recipients"
                                value={recipients}
                                onChange={(e) => setRecipients(e.target.value)}
                                placeholder="ops@company.com, finance@company.com"
                                className="col-span-3"
                            />
                        </div>
                        {frequency === 'weekly' && (
                            <div className="text-xs text-gray-500 col-span-4 text-right">
                                Weekly schedules run on Mondays.
                            </div>
                        )}
                        {frequency === 'monthly' && (
                            <div className="text-xs text-gray-500 col-span-4 text-right">
                                Monthly schedules run on the 1st.
                            </div>
                        )}
                        {error && (
                            <div className="text-xs text-red-600 col-span-4 text-right">{error}</div>
                        )}
                    </div>
                )}

                {!success && (
                    <DialogFooter>
                        <Button variant="ghost" onClick={() => setOpen(false)}>Cancel</Button>
                        <Button onClick={handleSchedule} disabled={loading || !sql.trim()}>
                            {loading ? (
                                <>Saving...</>
                            ) : (
                                <>Save Schedule</>
                            )}
                        </Button>
                    </DialogFooter>
                )}
            </DialogContent>
        </Dialog>
    )
}
