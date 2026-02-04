import { useState, useEffect, useCallback } from 'react'
import { apiService } from '@/services/apiService'

export interface BackendHealth {
  isConnected: boolean
  status: string | null
  components: any
  diagnostics: any | null
  lastCheck: Date | null
  latencyMs?: number
}

export function useBackendHealth(checkIntervalMs: number = 5000) {
  const [health, setHealth] = useState<BackendHealth>({
    isConnected: false,
    status: null,
    components: null,
    diagnostics: null,
    lastCheck: null,
  })

  const checkHealth = useCallback(async () => {
    try {
      const [healthData, diagnosticsData] = await Promise.allSettled([
        apiService.checkHealth(),
        apiService.getSystemDiagnostics()
      ]);

      const health = healthData.status === 'fulfilled' ? healthData.value : { status: 'error', components: null };
      const diagnostics = diagnosticsData.status === 'fulfilled' ? diagnosticsData.value : null;

      setHealth({
        isConnected: healthData.status === 'fulfilled',
        status: health.status,
        components: health.components,
        diagnostics: diagnostics,
        lastCheck: new Date(),
        latencyMs: (health as any).latency_ms,
      })
    } catch (err) {
      console.error('Health check failed:', err)
      setHealth({
        isConnected: false,
        status: 'error',
        components: null,
        diagnostics: null,
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
