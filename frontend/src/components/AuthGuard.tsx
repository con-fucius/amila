import { ReactNode, useEffect, useState } from 'react'
import { Navigate, useLocation } from 'react-router-dom'

interface AuthGuardProps {
  children: ReactNode
}

export function AuthGuard({ children }: AuthGuardProps) {
  const location = useLocation()
  const [isAuthenticated, setIsAuthenticated] = useState<boolean | null>(null)

  useEffect(() => {
    const checkAuth = () => {
      const token = localStorage.getItem('access_token')
      setIsAuthenticated(!!token)
    }

    checkAuth()

    // Synchronize across tabs
    const handleStorageChange = (e: StorageEvent) => {
      if (e.key === 'access_token' || e.key === 'refresh_token') {
        checkAuth()
      }
    }

    // Handle logout from apiService
    const handleAuthLogout = () => {
      setIsAuthenticated(false)
    }

    window.addEventListener('storage', handleStorageChange)
    window.addEventListener('auth-logout', handleAuthLogout)

    return () => {
      window.removeEventListener('storage', handleStorageChange)
      window.removeEventListener('auth-logout', handleAuthLogout)
    }
  }, [])

  // Show nothing while checking auth status
  if (isAuthenticated === null) {
    return (
      <div className="h-screen flex items-center justify-center bg-gray-900">
        <div className="animate-pulse text-gray-400">Loading...</div>
      </div>
    )
  }

  if (!isAuthenticated) {
    // Redirect to login, preserving the intended destination
    return <Navigate to="/login" state={{ from: location }} replace />
  }

  return <>{children}</>
}
