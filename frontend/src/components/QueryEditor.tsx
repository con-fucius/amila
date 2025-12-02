import React, { useState, useCallback } from 'react'
import {
  Box,
  Paper,
  Typography,
  Button,
  Alert,
  Chip,
  IconButton,
  Tooltip,
  Tabs,
  Tab,
  Switch,
  FormControlLabel,
} from '@mui/material'

import {
  PlayArrow as ExecuteIcon,
  Clear as ClearIcon,
  Save as SaveIcon,
  History as HistoryIcon,
  Code as CodeIcon,
  Visibility as PreviewIcon,
  Settings as SettingsIcon,
} from '@mui/icons-material'
import Editor from '@monaco-editor/react'

interface QueryEditorProps {
  query: string
  onQueryChange: (query: string) => void
  onExecute: (query: string) => void
  loading?: boolean
  disabled?: boolean
}

interface TabPanelProps {
  children?: React.ReactNode
  index: number
  value: number
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`query-tabpanel-${index}`}
      aria-labelledby={`query-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ pt: 2 }}>{children}</Box>}
    </div>
  )
}

const QueryEditor: React.FC<QueryEditorProps> = ({
  query,
  onQueryChange,
  onExecute,
  loading = false,
  disabled = false,
}) => {
  const [tabValue, setTabValue] = useState(0)
  const [editorTheme, setEditorTheme] = useState<'light' | 'dark'>('light')
  const [autoComplete] = useState(true)

  const [queryHistory, setQueryHistory] = useState<string[]>([])

  // SQL validation
  const validateQuery = useCallback((sql: string): { isValid: boolean; errors: string[] } => {
    const errors: string[] = []
    const upperSql = sql.trim().toUpperCase()

    if (!sql.trim()) {
      errors.push('Query cannot be empty')
      return { isValid: false, errors }
    }

    if (!upperSql.startsWith('SELECT')) {
      errors.push('Only SELECT statements are allowed')
    }

    const dangerousKeywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'TRUNCATE', 'ALTER', 'CREATE']
    for (const keyword of dangerousKeywords) {
      if (upperSql.includes(keyword)) {
        errors.push(`Prohibited operation detected: ${keyword}`)
      }
    }

    return { isValid: errors.length === 0, errors }
  }, [])

  const validation = validateQuery(query)

  const handleExecute = useCallback(() => {
    if (validation.isValid && query.trim()) {
      // Add to history
      if (!queryHistory.includes(query.trim())) {
        setQueryHistory(prev => [query.trim(), ...prev.slice(0, 9)]) // Keep last 10 queries
      }
      onExecute(query.trim())
    }
  }, [query, validation.isValid, queryHistory, onExecute])

  const handleClear = useCallback(() => {
    onQueryChange('')
  }, [onQueryChange])

  const handleHistorySelect = useCallback((historicalQuery: string) => {
    onQueryChange(historicalQuery)
  }, [onQueryChange])

  const handleTabChange = useCallback((_: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue)
  }, [])

  // Monaco Editor configuration
  const editorOptions = {
    minimap: { enabled: false },
    scrollBeyondLastLine: false,
    fontSize: 14,
    lineNumbers: 'on' as const,
    roundedSelection: false,
    readOnly: disabled,
    cursorStyle: 'line' as const,
    automaticLayout: true,
    wordWrap: 'on' as const,
    quickSuggestions: autoComplete,
  }

  return (
    <Paper sx={{ p: 3, mb: 3 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Typography variant="h5">
          SQL Query Editor
        </Typography>
        <Box display="flex" gap={1} alignItems="center">
          <Tooltip title="Editor Settings">
            <IconButton size="small">
              <SettingsIcon />
            </IconButton>
          </Tooltip>
          <FormControlLabel
            control={
              <Switch
                checked={editorTheme === 'dark'}
                onChange={(e) => setEditorTheme(e.target.checked ? 'dark' : 'light')}
                size="small"
              />
            }
            label="Dark Mode"
            sx={{ ml: 1 }}
          />
        </Box>
      </Box>

      <Tabs value={tabValue} onChange={handleTabChange} sx={{ mb: 2 }}>
        <Tab icon={<CodeIcon />} label="Editor" />
        <Tab icon={<HistoryIcon />} label="History" />
        <Tab icon={<PreviewIcon />} label="Preview" />
      </Tabs>

      <TabPanel value={tabValue} index={0}>
        {/* Main Editor */}
        <Box sx={{ border: 1, borderColor: 'divider', borderRadius: 1, mb: 2 }}>
          <Editor
            height="300px"
            defaultLanguage="sql"
            theme={editorTheme === 'dark' ? 'vs-dark' : 'vs'}
            value={query}
            onChange={(value) => onQueryChange(value || '')}
            options={editorOptions}
          />
        </Box>

        {/* Validation Status */}
        {query && (
          <Box mb={2}>
            {validation.isValid ? (
              <Alert severity="success" sx={{ mb: 1 }}>
                <Typography variant="body2">Query validation passed</Typography>
              </Alert>
            ) : (
              <Alert severity="error" sx={{ mb: 1 }}>
                <Typography variant="body2" gutterBottom>Query validation failed:</Typography>
                <ul style={{ margin: 0, paddingLeft: 20 }}>
                  {validation.errors.map((error, index) => (
                    <li key={index}>{error}</li>
                  ))}
                </ul>
              </Alert>
            )}
          </Box>
        )}

        {/* Action Buttons */}
        <Box display="flex" gap={2} alignItems="center">
          <Button
            variant="contained"
            startIcon={<ExecuteIcon />}
            onClick={handleExecute}
            disabled={loading || disabled || !validation.isValid || !query.trim()}
            size="large"
          >
            {loading ? 'Executing...' : 'Execute Query'}
          </Button>
          
          <Button
            variant="outlined"
            startIcon={<ClearIcon />}
            onClick={handleClear}
            disabled={loading || disabled}
          >
            Clear
          </Button>

          <Button
            variant="outlined"
            startIcon={<SaveIcon />}
            disabled={loading || disabled || !query.trim()}
          >
            Save Query
          </Button>

          <Box flex={1} />

          <Chip 
            label={`${query.length} characters`} 
            size="small" 
            variant="outlined" 
          />
        </Box>
      </TabPanel>

      <TabPanel value={tabValue} index={1}>
        {/* Query History */}
        <Typography variant="h6" gutterBottom>
          Recent Queries
        </Typography>
        {queryHistory.length === 0 ? (
          <Alert severity="info">
            No query history available. Execute some queries to see them here.
          </Alert>
        ) : (
          <Box>
            {queryHistory.map((historicalQuery, index) => (
              <Paper
                key={index}
                variant="outlined"
                sx={{ p: 2, mb: 1, cursor: 'pointer', '&:hover': { bgcolor: 'action.hover' } }}
                onClick={() => handleHistorySelect(historicalQuery)}
              >
                <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                  {historicalQuery.length > 100 
                    ? `${historicalQuery.substring(0, 100)}...` 
                    : historicalQuery
                  }
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  Click to use this query
                </Typography>
              </Paper>
            ))}
          </Box>
        )}
      </TabPanel>

      <TabPanel value={tabValue} index={2}>
        {/* Query Preview */}
        <Typography variant="h6" gutterBottom>
          Query Preview
        </Typography>
        {query ? (
          <Box>
            <Paper variant="outlined" sx={{ p: 2, mb: 2, bgcolor: 'grey.50' }}>
              <Typography variant="body2" sx={{ fontFamily: 'monospace', whiteSpace: 'pre-wrap' }}>
                {query}
              </Typography>
            </Paper>
            <Box display="flex" gap={1} flexWrap="wrap">
              <Chip label={`${query.split('\n').length} lines`} size="small" />
              <Chip label={`${query.split(' ').length} words`} size="small" />
              <Chip 
                label={validation.isValid ? 'Valid' : 'Invalid'} 
                color={validation.isValid ? 'success' : 'error'} 
                size="small" 
              />
            </Box>
          </Box>
        ) : (
          <Alert severity="info">
            Enter a query in the editor to see the preview here.
          </Alert>
        )}
      </TabPanel>

      {/* Quick Actions */}
      <Box mt={2} pt={2} borderTop={1} borderColor="divider">
        <Typography variant="body2" color="text.secondary" gutterBottom>
          Quick Actions:
        </Typography>
        <Box display="flex" gap={1} flexWrap="wrap">
          <Button
            size="small"
            variant="outlined"
            onClick={() => onQueryChange('SELECT * FROM dual')}
            disabled={loading || disabled}
          >
            Test Connection
          </Button>
          <Button
            size="small"
            variant="outlined"
            onClick={() => onQueryChange('SELECT table_name FROM user_tables')}
            disabled={loading || disabled}
          >
            List Tables
          </Button>
          <Button
            size="small"
            variant="outlined"
            onClick={() => onQueryChange('SELECT column_name, data_type FROM user_tab_columns WHERE table_name = \'YOUR_TABLE\'')}
            disabled={loading || disabled}
          >
            Describe Table
          </Button>
        </Box>
      </Box>
    </Paper>
  )
}

export default QueryEditor