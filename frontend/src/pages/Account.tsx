import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { User, Mail, Shield, Building2, LogOut, ChevronLeft } from 'lucide-react'
import { cn } from '@/utils/cn'

interface UserProfile {
  username: string
  role: string
  email?: string
  department?: string
  displayName?: string
  jobTitle?: string
}

export function Account() {
  const navigate = useNavigate()
  const [profile, setProfile] = useState<UserProfile | null>(null)

  useEffect(() => {
    // Parse JWT token to extract user info
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
      setProfile({
        username: 'user',
        role: 'Analyst',
        email: 'user@company.com',
        department: 'Analytics',
        displayName: 'User',
        jobTitle: 'Data Analyst',
      })
    }
  }, [])

  const handleLogout = () => {
    localStorage.removeItem('access_token')
    localStorage.removeItem('refresh_token')
    navigate('/login')
  }

  const getRoleBadgeColor = (role: string) => {
    const r = role.toLowerCase()
    if (r === 'admin') return 'bg-purple-500/20 text-purple-300 border-purple-500/30'
    if (r === 'analyst') return 'bg-blue-500/20 text-blue-300 border-blue-500/30'
    return 'bg-gray-500/20 text-gray-300 border-gray-500/30'
  }

  if (!profile) {
    return (
      <div className="h-full flex items-center justify-center bg-gray-900">
        <div className="animate-pulse text-gray-400">Loading...</div>
      </div>
    )
  }

  return (
    <div className="h-full bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 overflow-auto">
      <div className="p-6">
        {/* Header */}
        <div className="flex items-center gap-4 mb-8">
          <button
            onClick={() => navigate(-1)}
            className="p-2 rounded-lg hover:bg-gray-700/50 text-gray-400 hover:text-white transition-colors"
          >
            <ChevronLeft className="w-5 h-5" />
          </button>
          <h1 className="text-2xl font-bold text-white">Account</h1>
        </div>

        {/* Profile Card */}
        <div className="bg-gray-800/50 backdrop-blur-sm rounded-2xl border border-gray-700 p-6 mb-6">
          <div className="flex items-start gap-4">
            <div className="w-16 h-16 rounded-full bg-gradient-to-r from-emerald-500 to-green-600 flex items-center justify-center text-white text-2xl font-bold">
              {profile.displayName?.charAt(0).toUpperCase() || 'U'}
            </div>
            <div className="flex-1">
              <h2 className="text-xl font-semibold text-white">{profile.displayName}</h2>
              <p className="text-gray-400 text-sm">@{profile.username}</p>
              <span className={cn('inline-block mt-2 px-3 py-1 rounded-full text-xs font-medium border', getRoleBadgeColor(profile.role))}>
                {profile.role}
              </span>
            </div>
          </div>
        </div>

        {/* Details */}
        <div className="bg-gray-800/50 backdrop-blur-sm rounded-2xl border border-gray-700 p-6 mb-6">
          <h3 className="text-lg font-semibold text-white mb-4">Profile Details</h3>
          <div className="space-y-4">
            <div className="flex items-center gap-3 p-3 rounded-lg bg-gray-700/30">
              <User className="w-5 h-5 text-emerald-400" />
              <div>
                <p className="text-xs text-gray-400">Username</p>
                <p className="text-white">{profile.username}</p>
              </div>
            </div>
            <div className="flex items-center gap-3 p-3 rounded-lg bg-gray-700/30">
              <Mail className="w-5 h-5 text-emerald-400" />
              <div>
                <p className="text-xs text-gray-400">Email</p>
                <p className="text-white">{profile.email}</p>
              </div>
            </div>
            <div className="flex items-center gap-3 p-3 rounded-lg bg-gray-700/30">
              <Shield className="w-5 h-5 text-emerald-400" />
              <div>
                <p className="text-xs text-gray-400">Role</p>
                <p className="text-white">{profile.role}</p>
              </div>
            </div>
            <div className="flex items-center gap-3 p-3 rounded-lg bg-gray-700/30">
              <Building2 className="w-5 h-5 text-emerald-400" />
              <div>
                <p className="text-xs text-gray-400">Department</p>
                <p className="text-white">{profile.department}</p>
              </div>
            </div>
          </div>
        </div>

        {/* SSO Info */}
        <div className="bg-gray-800/50 backdrop-blur-sm rounded-2xl border border-gray-700 p-6 mb-6">
          <h3 className="text-lg font-semibold text-white mb-2">Organization</h3>
          <p className="text-gray-400 text-sm mb-4">
            This account is managed by your organization. Contact your IT administrator for profile changes.
          </p>
          <div className="p-3 rounded-lg bg-blue-900/20 border border-blue-700/30">
            <p className="text-blue-300 text-sm">
              SSO integration available. Configure AD/Entra ID in production for single sign-on.
            </p>
          </div>
        </div>

        {/* Logout */}
        <button
          onClick={handleLogout}
          className="w-full flex items-center justify-center gap-2 py-3 rounded-xl bg-red-900/30 border border-red-700/50 text-red-300 hover:bg-red-900/50 transition-colors"
        >
          <LogOut className="w-4 h-4" />
          Sign out
        </button>
      </div>
    </div>
  )
}
