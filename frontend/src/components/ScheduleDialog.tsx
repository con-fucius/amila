import { useState } from 'react'
import { Calendar } from 'lucide-react'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

interface ScheduleDialogProps {
    sql: string
    connection?: string
    databaseType: string
    onSchedule?: (config: any) => void
}

export function ScheduleDialog({ sql, connection, databaseType, onSchedule }: ScheduleDialogProps) {
    const [open, setOpen] = useState(false)
    const [frequency, setFrequency] = useState('daily')
    const [time, setTime] = useState('09:00')
    const [loading, setLoading] = useState(false)
    const [success, setSuccess] = useState(false)

    const handleSchedule = async () => {
        setLoading(true)
        try {
            // In a real implementation, this would call apiService.scheduleQuery(...)
            // For now, we simulate the network request and "save"
            await new Promise(resolve => setTimeout(resolve, 1000))

            console.log('Scheduled query:', { sql, connection, databaseType, frequency, time })

            // Mock success
            setSuccess(true)
            if (onSchedule) onSchedule({ frequency, time })

            setTimeout(() => {
                setOpen(false)
                setSuccess(false)
            }, 1500)
        } catch (e) {
            console.error('Failed to schedule', e)
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
