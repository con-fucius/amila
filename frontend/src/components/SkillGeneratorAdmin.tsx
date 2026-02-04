/**
 * Skill Generator Admin Component
 * 
 * Admin interface for reviewing and managing auto-generated skills.
 * Allows administrators to view generated skills, approve/reject them,
 * and export them for use in the system.
 * 
 * Features:
 * - List all generated skills with filtering
 * - View skill details including YAML content
 * - Approve/reject skills
 * - Generate new skills from patterns
 * - Export skills to YAML
 * - View skill statistics
 */

import { useEffect, useState, useCallback } from 'react'
import {
  Wand2,
  CheckCircle,
  XCircle,
  Trash2,
  Download,
  RefreshCw,
  Code,
  TrendingUp,

  AlertTriangle,
  Sparkles,
  BarChart3,
  FileCode,
  Search
} from 'lucide-react'
import { cn } from '@/utils/cn'
import { apiService } from '@/services/apiService'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Progress } from '@/components/ui/progress'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from '@/components/ui/dialog'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Input } from '@/components/ui/input'
import { Alert, AlertDescription } from '@/components/ui/alert'

// Types
interface Skill {
  skill_id: string
  skill_type: string
  name: string
  description: string
  confidence: number
  generated_at: string
  effectiveness_score: number
  usage_count: number
  yaml_preview?: string
}

interface SkillDetail extends Skill {
  yaml_content: string
  source_queries: string[]
}

interface SkillStats {
  total_skills: number
  by_type: Record<string, number>
  avg_confidence: number
  avg_effectiveness: number
  total_usage: number
  recently_generated: number
}

type TabType = 'skills' | 'stats' | 'generate'

const skillTypeLabels: Record<string, string> = {
  column_mapping: 'Column Mapping',
  query_pattern: 'Query Pattern',
  table_join: 'Table Join',
  aggregation: 'Aggregation',
  filter: 'Filter'
}

const skillTypeColors: Record<string, string> = {
  column_mapping: 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300',
  query_pattern: 'bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300',
  table_join: 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300',
  aggregation: 'bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-300',
  filter: 'bg-rose-100 text-rose-800 dark:bg-rose-900/30 dark:text-rose-300'
}

export function SkillGeneratorAdmin() {
  const [activeTab, setActiveTab] = useState<TabType>('skills')
  const [skills, setSkills] = useState<Skill[]>([])
  const [stats, setStats] = useState<SkillStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedSkill, setSelectedSkill] = useState<SkillDetail | null>(null)
  const [skillDetailOpen, setSkillDetailOpen] = useState(false)
  const [generating, setGenerating] = useState(false)
  const [deletingSkill, setDeletingSkill] = useState<string | null>(null)
  const [filters, setFilters] = useState({
    skill_type: '',
    min_confidence: 0,
    search: ''
  })

  const fetchSkills = useCallback(async () => {
    setLoading(true)
    setError(null)

    try {
      const response = await apiService.listSkills({
        skill_type: filters.skill_type || undefined,
        min_confidence: filters.min_confidence || undefined,
        limit: 100
      })
      setSkills(response.skills)
    } catch (err: any) {
      console.error('Failed to fetch skills:', err)
      setError(err.message || 'Failed to load skills')
    } finally {
      setLoading(false)
    }
  }, [filters.skill_type, filters.min_confidence])

  const fetchStats = useCallback(async () => {
    try {
      const response = await apiService.getSkillStats()
      setStats(response)
    } catch (err: any) {
      console.error('Failed to fetch stats:', err)
    }
  }, [])

  useEffect(() => {
    fetchSkills()
    fetchStats()
  }, [fetchSkills, fetchStats])

  const handleGenerateSkills = async () => {
    setGenerating(true)
    setError(null)

    try {
      const response = await apiService.generateSkills({
        min_frequency: 3,
        min_confidence: 0.7
      })

      if (response.success) {
        await fetchSkills()
        await fetchStats()
      }
    } catch (err: any) {
      console.error('Failed to generate skills:', err)
      setError(err.message || 'Failed to generate skills')
    } finally {
      setGenerating(false)
    }
  }

  const handleViewSkill = async (skillId: string) => {
    try {
      const detail = await apiService.getSkillDetail(skillId)
      setSelectedSkill(detail)
      setSkillDetailOpen(true)
    } catch (err: any) {
      console.error('Failed to get skill detail:', err)
      setError(err.message || 'Failed to load skill details')
    }
  }

  const handleApproveSkill = async (skillId: string, approved: boolean) => {
    try {
      await apiService.approveSkill(skillId, approved)
      await fetchSkills()

      if (selectedSkill?.skill_id === skillId) {
        setSkillDetailOpen(false)
        setSelectedSkill(null)
      }
    } catch (err: any) {
      console.error('Failed to approve skill:', err)
      setError(err.message || 'Failed to update skill')
    }
  }

  const handleDeleteSkill = async (skillId: string) => {
    if (!confirm('Are you sure you want to delete this skill?')) return

    setDeletingSkill(skillId)
    try {
      await apiService.deleteSkill(skillId)
      await fetchSkills()
      await fetchStats()
    } catch (err: any) {
      console.error('Failed to delete skill:', err)
      setError(err.message || 'Failed to delete skill')
    } finally {
      setDeletingSkill(null)
    }
  }

  const handleExportSkills = async () => {
    try {
      const response = await apiService.exportSkills()

      // Download the file
      const blob = new Blob([response.content], { type: 'text/yaml' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = response.filename
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    } catch (err: any) {
      console.error('Failed to export skills:', err)
      setError(err.message || 'Failed to export skills')
    }
  }

  const filteredSkills = skills.filter(skill => {
    if (filters.search) {
      const searchLower = filters.search.toLowerCase()
      return (
        skill.name.toLowerCase().includes(searchLower) ||
        skill.description.toLowerCase().includes(searchLower)
      )
    }
    return true
  })

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    })
  }

  // Loading state
  if (loading && skills.length === 0) {
    return (
      <Card className="w-full">
        <CardContent className="p-8">
          <div className="flex items-center justify-center space-x-2">
            <RefreshCw className="w-5 h-5 animate-spin text-gray-400" />
            <span className="text-gray-600 dark:text-gray-400">Loading skills...</span>
          </div>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 dark:text-gray-100 flex items-center gap-2">
            <Wand2 className="w-6 h-6 text-purple-500" />
            Skill Generator Admin
          </h2>
          <p className="text-gray-600 dark:text-gray-400">
            Review and manage auto-generated skills from query patterns
          </p>
        </div>
        <div className="flex gap-2">
          <Button onClick={handleExportSkills} variant="outline" size="sm">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button onClick={() => { fetchSkills(); fetchStats(); }} variant="outline" size="sm">
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>
      </div>

      {/* Error Alert */}
      {error && (
        <Alert variant="destructive">
          <AlertTriangle className="w-4 h-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {/* Stats Overview */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                {stats.total_skills}
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                Total Skills
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">
                {(stats.avg_confidence * 100).toFixed(0)}%
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                Avg Confidence
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold text-emerald-600 dark:text-emerald-400">
                {(stats.avg_effectiveness * 100).toFixed(0)}%
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                Avg Effectiveness
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                {stats.total_usage}
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                Total Usage
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="text-2xl font-bold text-amber-600 dark:text-amber-400">
                {stats.recently_generated}
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                New (7 days)
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Tabs */}
      <div className="border-b border-gray-200 dark:border-slate-700">
        <nav className="flex gap-4">
          {[
            { key: 'skills', label: 'Skills', icon: FileCode },
            { key: 'stats', label: 'Statistics', icon: BarChart3 },
            { key: 'generate', label: 'Generate', icon: Sparkles }
          ].map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key as TabType)}
              className={cn(
                "pb-2 px-1 text-sm font-medium border-b-2 transition-colors flex items-center gap-2",
                activeTab === tab.key
                  ? "border-purple-500 text-purple-600 dark:text-purple-400"
                  : "border-transparent text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200"
              )}
            >
              <tab.icon className="w-4 h-4" />
              {tab.label}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab Content */}
      <div className="space-y-4">
        {/* Skills Tab */}
        {activeTab === 'skills' && (
          <>
            {/* Filters */}
            <Card>
              <CardContent className="p-4">
                <div className="flex flex-wrap gap-4">
                  <div className="flex-1 min-w-[200px]">
                    <div className="relative">
                      <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                      <Input
                        placeholder="Search skills..."
                        value={filters.search}
                        onChange={(e) => setFilters({ ...filters, search: e.target.value })}
                        className="pl-10"
                      />
                    </div>
                  </div>
                  <Select
                    value={filters.skill_type}
                    onValueChange={(value) => setFilters({ ...filters, skill_type: value })}
                  >
                    <SelectTrigger className="w-[180px]">
                      <SelectValue placeholder="All Types" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="">All Types</SelectItem>
                      {Object.entries(skillTypeLabels).map(([key, label]) => (
                        <SelectItem key={key} value={key}>{label}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <Select
                    value={String(filters.min_confidence)}
                    onValueChange={(value) => setFilters({ ...filters, min_confidence: Number(value) })}
                  >
                    <SelectTrigger className="w-[180px]">
                      <SelectValue placeholder="Min Confidence" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="0">Any Confidence</SelectItem>
                      <SelectItem value="0.5">50%+</SelectItem>
                      <SelectItem value="0.7">70%+</SelectItem>
                      <SelectItem value="0.9">90%+</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </CardContent>
            </Card>

            {/* Skills List */}
            {filteredSkills.length === 0 ? (
              <Card>
                <CardContent className="p-8 text-center">
                  <Sparkles className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                  <p className="text-gray-600 dark:text-gray-400">No skills found</p>
                  <p className="text-sm text-gray-500 mt-1">
                    Generate skills from query patterns or adjust filters
                  </p>
                </CardContent>
              </Card>
            ) : (
              <div className="grid gap-3">
                {filteredSkills.map((skill) => (
                  <Card key={skill.skill_id} className="hover:shadow-md transition-shadow">
                    <CardContent className="p-4">
                      <div className="flex items-start justify-between">
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 mb-1">
                            <h3 className="font-semibold text-gray-900 dark:text-gray-100 truncate">
                              {skill.name}
                            </h3>
                            <Badge className={skillTypeColors[skill.skill_type] || 'bg-gray-100'}>
                              {skillTypeLabels[skill.skill_type] || skill.skill_type}
                            </Badge>
                          </div>
                          <p className="text-sm text-gray-600 dark:text-gray-400 line-clamp-2">
                            {skill.description}
                          </p>
                          <div className="flex items-center gap-4 mt-2 text-xs text-gray-500">
                            <span>Generated: {formatDate(skill.generated_at)}</span>
                            <span>Usage: {skill.usage_count}</span>
                            <span className="flex items-center gap-1">
                              Effectiveness: {(skill.effectiveness_score * 100).toFixed(0)}%
                            </span>
                          </div>
                        </div>
                        <div className="flex items-center gap-2 ml-4">
                          <div className="text-right mr-4">
                            <div className="text-sm font-medium text-gray-900 dark:text-gray-100">
                              {(skill.confidence * 100).toFixed(0)}%
                            </div>
                            <div className="text-xs text-gray-500">Confidence</div>
                          </div>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleViewSkill(skill.skill_id)}
                          >
                            <Code className="w-4 h-4" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleDeleteSkill(skill.skill_id)}
                            disabled={deletingSkill === skill.skill_id}
                          >
                            <Trash2 className="w-4 h-4 text-red-500" />
                          </Button>
                        </div>
                      </div>
                      {/* Confidence Bar */}
                      <div className="mt-3">
                        <Progress value={skill.confidence * 100} className="h-1" />
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            )}
          </>
        )}

        {/* Stats Tab */}
        {activeTab === 'stats' && stats && (
          <div className="grid md:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <BarChart3 className="w-5 h-5" />
                  Skills by Type
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  {Object.entries(stats.by_type).map(([type, count]) => (
                    <div key={type} className="flex items-center justify-between">
                      <span className="text-gray-600 dark:text-gray-400">
                        {skillTypeLabels[type] || type}
                      </span>
                      <div className="flex items-center gap-3">
                        <div className="w-32 h-2 bg-gray-100 dark:bg-slate-800 rounded-full overflow-hidden">
                          <div
                            className="h-full bg-purple-500 rounded-full"
                            style={{ width: `${(count / stats.total_skills) * 100}%` }}
                          />
                        </div>
                        <span className="text-sm font-medium w-8 text-right">{count}</span>
                      </div>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <TrendingUp className="w-5 h-5" />
                  Performance Metrics
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <div className="flex justify-between text-sm mb-1">
                    <span className="text-gray-600 dark:text-gray-400">Average Confidence</span>
                    <span className="font-medium">{(stats.avg_confidence * 100).toFixed(1)}%</span>
                  </div>
                  <Progress value={stats.avg_confidence * 100} className="h-2" />
                </div>
                <div>
                  <div className="flex justify-between text-sm mb-1">
                    <span className="text-gray-600 dark:text-gray-400">Average Effectiveness</span>
                    <span className="font-medium">{(stats.avg_effectiveness * 100).toFixed(1)}%</span>
                  </div>
                  <Progress value={stats.avg_effectiveness * 100} className="h-2" />
                </div>
                <div className="pt-4 border-t border-gray-100 dark:border-slate-800">
                  <div className="flex justify-between items-center">
                    <span className="text-gray-600 dark:text-gray-400">Total Usage</span>
                    <span className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                      {stats.total_usage}
                    </span>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        )}

        {/* Generate Tab */}
        {activeTab === 'generate' && (
          <Card>
            <CardContent className="p-8">
              <div className="max-w-md mx-auto text-center">
                <Sparkles className="w-16 h-16 text-purple-500 mx-auto mb-4" />
                <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-2">
                  Generate Skills from Patterns
                </h3>
                <p className="text-gray-600 dark:text-gray-400 mb-6">
                  Analyze recorded query patterns and automatically generate new skills.
                  Skills with sufficient frequency and confidence will be created.
                </p>
                <div className="flex flex-col gap-3">
                  <Button
                    onClick={handleGenerateSkills}
                    disabled={generating}
                    className="w-full"
                  >
                    {generating ? (
                      <>
                        <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                        Generating...
                      </>
                    ) : (
                      <>
                        <Wand2 className="w-4 h-4 mr-2" />
                        Generate Skills
                      </>
                    )}
                  </Button>
                  <p className="text-xs text-gray-500">
                    Minimum frequency: 3 occurrences • Minimum confidence: 70%
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>

      {/* Skill Detail Dialog */}
      <Dialog open={skillDetailOpen} onOpenChange={setSkillDetailOpen}>
        <DialogContent className="max-w-3xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {selectedSkill?.name}
              <Badge className={skillTypeColors[selectedSkill?.skill_type || ''] || 'bg-gray-100'}>
                {skillTypeLabels[selectedSkill?.skill_type || ''] || selectedSkill?.skill_type}
              </Badge>
            </DialogTitle>
            <DialogDescription>{selectedSkill?.description}</DialogDescription>
          </DialogHeader>

          {selectedSkill && (
            <div className="space-y-4">
              {/* Stats */}
              <div className="grid grid-cols-3 gap-4">
                <div className="bg-gray-50 dark:bg-slate-800 p-3 rounded-lg text-center">
                  <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">
                    {(selectedSkill.confidence * 100).toFixed(0)}%
                  </div>
                  <div className="text-xs text-gray-500">Confidence</div>
                </div>
                <div className="bg-gray-50 dark:bg-slate-800 p-3 rounded-lg text-center">
                  <div className="text-2xl font-bold text-emerald-600 dark:text-emerald-400">
                    {(selectedSkill.effectiveness_score * 100).toFixed(0)}%
                  </div>
                  <div className="text-xs text-gray-500">Effectiveness</div>
                </div>
                <div className="bg-gray-50 dark:bg-slate-800 p-3 rounded-lg text-center">
                  <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                    {selectedSkill.usage_count}
                  </div>
                  <div className="text-xs text-gray-500">Usage</div>
                </div>
              </div>

              {/* YAML Content */}
              <div>
                <h4 className="text-sm font-medium text-gray-900 dark:text-gray-100 mb-2 flex items-center gap-2">
                  <FileCode className="w-4 h-4" />
                  YAML Content
                </h4>
                <pre className="bg-gray-900 text-gray-100 p-4 rounded-lg overflow-x-auto text-sm">
                  <code>{selectedSkill.yaml_content}</code>
                </pre>
              </div>

              {/* Source Queries */}
              {selectedSkill.source_queries.length > 0 && (
                <div>
                  <h4 className="text-sm font-medium text-gray-900 dark:text-gray-100 mb-2">
                    Source Queries ({selectedSkill.source_queries.length})
                  </h4>
                  <ul className="space-y-2">
                    {selectedSkill.source_queries.slice(0, 5).map((query, idx) => (
                      <li key={idx} className="text-sm text-gray-600 dark:text-gray-400 bg-gray-50 dark:bg-slate-800 p-2 rounded">
                        "{query}"
                      </li>
                    ))}
                    {selectedSkill.source_queries.length > 5 && (
                      <li className="text-sm text-gray-500">
                        ... and {selectedSkill.source_queries.length - 5} more
                      </li>
                    )}
                  </ul>
                </div>
              )}
            </div>
          )}

          <DialogFooter className="gap-2">
            <Button
              variant="outline"
              onClick={() => handleApproveSkill(selectedSkill?.skill_id || '', false)}
            >
              <XCircle className="w-4 h-4 mr-2" />
              Reject
            </Button>
            <Button
              onClick={() => handleApproveSkill(selectedSkill?.skill_id || '', true)}
            >
              <CheckCircle className="w-4 h-4 mr-2" />
              Approve
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
