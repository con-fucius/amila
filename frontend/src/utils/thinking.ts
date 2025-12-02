import type { ThinkingStep } from '@/types/domain'

/**
 * Normalize thinking steps coming from various backend shapes
 * (thinkingSteps vs thinking_steps, nested in llm_metadata, etc.).
 */
export function extractThinkingSteps(source: any): ThinkingStep[] {
  if (!source) return []

  const raw =
    (Array.isArray(source.thinkingSteps) && source.thinkingSteps) ||
    (Array.isArray(source.thinking_steps) && source.thinking_steps) ||
    (Array.isArray(source.llm_metadata?.thinking_steps) && source.llm_metadata.thinking_steps) ||
    (Array.isArray(source.metadata?.thinking_steps) && source.metadata.thinking_steps) ||
    []

  if (!Array.isArray(raw)) return []
  
  // Normalize each step to ensure consistent structure
  return raw.map((step: any, idx: number) => ({
    id: step.id || `step-${idx}`,
    name: step.name || step.stage || undefined,
    stage: step.stage || step.name || undefined,
    content: step.content || step.message || step.description || `Step ${idx + 1}`,
    status: normalizeStatus(step.status),
    error: step.error,
    timestamp: step.timestamp,
    details: step.details,
  })) as ThinkingStep[]
}

/**
 * Normalize status string to valid ThinkingStep status
 */
function normalizeStatus(status: any): ThinkingStep['status'] {
  if (!status) return 'pending'
  const s = String(status).toLowerCase()
  if (s === 'completed' || s === 'done' || s === 'success') return 'completed'
  if (s === 'in-progress' || s === 'running' || s === 'processing') return 'in-progress'
  if (s === 'failed' || s === 'error') return 'failed'
  return 'pending'
}

/**
 * Get a human-readable label for a stage name
 */
export function getStageLabel(stage: string | undefined): string {
  if (!stage) return 'Processing'
  
  const labels: Record<string, string> = {
    'understand': 'Understanding Query',
    'retrieve_context': 'Retrieving Context',
    'context': 'Gathering Context',
    'hypothesis': 'Forming Hypothesis',
    'generate_sql': 'Generating SQL',
    'validation': 'Validating Query',
    'execution': 'Executing Query',
    'results': 'Processing Results',
    'planning': 'Planning Execution',
    'prepared': 'Preparation Complete',
    'pending_approval': 'Awaiting Approval',
    'executing': 'Executing',
    'finished': 'Completed',
    'error': 'Error',
  }
  
  return labels[stage.toLowerCase()] || stage.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
}
