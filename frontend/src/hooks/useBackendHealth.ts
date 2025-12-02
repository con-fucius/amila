import { useState, useEffect, useCallback } from 'react'
import { apiService } from '@/services/apiService'

export interface BackendHealth {
  isConnected: boolean
  status: string | null
  components: any
  lastCheck: Date | null
  latencyMs?: number
}

export function useBackendHealth(checkIntervalMs: number = 30000) {
  const [health, setHealth] = useState<BackendHealth>({
    isConnected: false,
    status: null,
    components: null,
    lastCheck: null,
  })

  const checkHealth = useCallback(async () => {
    try {
      const healthData = await apiService.checkHealth()
      setHealth({
        isConnected: true,
        status: healthData.status,
        components: healthData.components,
        lastCheck: new Date(),
        latencyMs: (healthData as any).latency_ms,
      })
    } catch (err) {
      console.error('Health check failed:', err)
      setHealth({
        isConnected: false,
        status: 'error',
        components: null,
        lastCheck: new Date(),
        latencyMs: undefined,
      })
    }
  }, [])

  useEffect(() => {
    // Initial check
    checkHealth()

    // Set up periodic health checks
    const interval = setInterval(checkHealth, checkIntervalMs)

    return () => clearInterval(interval)
  }, [checkHealth, checkIntervalMs])

  return {
    ...health,
    recheckHealth: checkHealth,
  }
}
