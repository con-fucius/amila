import React, { useState, useMemo, useCallback, useEffect, useRef } from 'react'
import {
  Box,
  Paper,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  IconButton,
  Chip,
  Alert,
  Tabs,
  Tab,
  Button,
  Menu,
  MenuItem,
  TextField,
  InputAdornment,
  Collapse,
  Grid,
  CircularProgress,
  TableSortLabel,
  Checkbox,
  ListItemIcon,
  ListItemText,
  Tooltip,
  TableFooter,
  Select,
  FormControl,
  InputLabel,
} from '@mui/material'

import {
  GetApp as ExportIcon,
  ViewColumn as ColumnsIcon,
  BarChart as ChartIcon,
  TableChart as TableIcon,
  Code as JsonIcon,
  Search as SearchIcon,
  ContentCopy as CopyIcon,
  ExpandMore as ExpandIcon,
  ExpandLess as CollapseIcon,
  Info as InfoIcon,
} from '@mui/icons-material'
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RechartsTooltip,
  Legend,
  ResponsiveContainer,
  AreaChart,
  Area,
} from 'recharts'
import { useSnackbar } from '../contexts/SnackbarContext'
import ProgressiveDisclosureDialog from './ProgressiveDisclosureDialog'
import { detectNumericColumnIndexes, paginateRows } from '@/utils/results'

interface QueryResultsProps {
  data: {
    columns: string[]
    rows: any[][]
    metadata?: {
      execution_time?: number
      row_count?: number
      query_id?: string
      timestamp?: string
    }
  } | null
  loading?: boolean
  error?: string | null
  viewMode?: 'table' | 'chart' | 'json'
}

type Order = 'asc' | 'desc'

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
      id={`results-tabpanel-${index}`}
      aria-labelledby={`results-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ pt: 2 }}>{children}</Box>}
    </div>
  )
}

const QueryResults: React.FC<QueryResultsProps> = ({ data, loading = false, error = null, viewMode }) => {
  const { success, warning, error: showError } = useSnackbar()
  const [tabValue, setTabValue] = useState(viewMode === 'chart' ? 1 : viewMode === 'json' ? 2 : 0)
  const [collapsed, setCollapsed] = useState(false)
  const [page, setPage] = useState(0)
  const [rowsPerPage, setRowsPerPage] = useState(25)
  const [exportAnchorEl, setExportAnchorEl] = useState<null | HTMLElement>(null)
  const [columnsAnchorEl, setColumnsAnchorEl] = useState<null | HTMLElement>(null)
  const [copyAnchorEl, setCopyAnchorEl] = useState<null | HTMLElement>(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [showMetadata, setShowMetadata] = useState(false)
  const [selectedColumns, setSelectedColumns] = useState<string[]>([])
  const [orderBy, setOrderBy] = useState<number | null>(null)
  const [order, setOrder] = useState<Order>('asc')
  const [columnWidths, setColumnWidths] = useState<Record<number, number>>({})
  const [resizing, setResizing] = useState<{ colIndex: number; startX: number; startWidth: number } | null>(null)
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [selectedCell, setSelectedCell] = useState<{ row: number | null; col: number | null }>({ row: null, col: null })
  const [disclosureDialog, setDisclosureDialog] = useState<{
    open: boolean
    rowCount: number
    estimatedSize: string
  }>({
    open: false,
    rowCount: 0,
    estimatedSize: ''
  })
  const [chartType, setChartType] = useState<'bar' | 'line' | 'pie' | 'area'>('bar')
  const [chartDimension, setChartDimension] = useState<string | null>(null)
  const [chartMetric, setChartMetric] = useState<string | null>(null)

  // Preferences persistence (per query id)
  const prefsKey = useMemo(() => `qr_prefs_${data?.metadata?.query_id || 'default'}`, [data?.metadata?.query_id])

  useEffect(() => {
    if (!data) return
    try {
      const raw = localStorage.getItem(prefsKey)
      if (!raw) return
      const prefs = JSON.parse(raw)
      if (Array.isArray(prefs.selectedColumns)) setSelectedColumns(prefs.selectedColumns)
      if (typeof prefs.orderBy === 'number' || prefs.orderBy === null) setOrderBy(prefs.orderBy)
      if (prefs.order === 'asc' || prefs.order === 'desc') setOrder(prefs.order)
      if (prefs.columnWidths && typeof prefs.columnWidths === 'object') setColumnWidths(prefs.columnWidths)
      if (typeof prefs.rowsPerPage === 'number') setRowsPerPage(prefs.rowsPerPage)
    } catch { }
  }, [prefsKey, data])

  useEffect(() => {
    // persist on change
    const prefs = { selectedColumns, orderBy, order, columnWidths, rowsPerPage }
    try { localStorage.setItem(prefsKey, JSON.stringify(prefs)) } catch { }
  }, [prefsKey, selectedColumns, orderBy, order, columnWidths, rowsPerPage])

  // Check for large result sets
  useEffect(() => {
    if (data && data.rows && data.rows.length > 1000 && !disclosureDialog.open) {
      const sizeKB = (data.rows.length * 100) / 1024
      setDisclosureDialog({
        open: true,
        rowCount: data.rows.length,
        estimatedSize: sizeKB > 1024 ? `${(sizeKB / 1024).toFixed(1)} MB` : `${sizeKB.toFixed(1)} KB`
      })
    }
  }, [data, disclosureDialog.open])

  // Filter data based on search term
  const filteredData = useMemo(() => {
    if (!data) return null
    if (!searchTerm) return data

    const filteredRows = data.rows.filter(row =>
      row.some(cell => cell?.toString().toLowerCase().includes(searchTerm.toLowerCase()))
    )

    return { ...data, rows: filteredRows }
  }, [data, searchTerm])

  // Sort data
  const sortedData = useMemo(() => {
    if (!filteredData) return null
    if (orderBy === null) return filteredData
    const rowsCopy = [...filteredData.rows]
    rowsCopy.sort((a, b) => {
      const av = a[orderBy]
      const bv = b[orderBy]
      // Try number compare first
      const an = Number(av)
      const bn = Number(bv)
      const aIsNum = Number.isFinite(an) && av !== '' && av !== null && av !== undefined
      const bIsNum = Number.isFinite(bn) && bv !== '' && bv !== null && bv !== undefined
      let cmp = 0
      if (aIsNum && bIsNum) {
        cmp = an - bn
      } else {
        // Try date compare next
        const ad = Date.parse(av)
        const bd = Date.parse(bv)
        const aIsDate = !isNaN(ad)
        const bIsDate = !isNaN(bd)
        if (aIsDate && bIsDate) {
          cmp = ad - bd
        } else {
          cmp = (av ?? '').toString().localeCompare((bv ?? '').toString(), undefined, { numeric: true, sensitivity: 'base' })
        }
      }
      return order === 'asc' ? cmp : -cmp
    })
    return { ...filteredData, rows: rowsCopy }
  }, [filteredData, orderBy, order])

  // Visible columns
  const visibleColumnIndexes = useMemo(() => {
    if (!data) return []
    if (selectedColumns.length === 0) return data.columns.map((_, i) => i)
    const set = new Set(selectedColumns)
    return data.columns.map((c, i) => (set.has(c) ? i : -1)).filter(i => i >= 0)
  }, [data, selectedColumns])

  // Paginated data
  const paginatedData = useMemo(() => {
    if (!sortedData) return null
    const { slice } = paginateRows(sortedData.rows, page, rowsPerPage)
    return { ...sortedData, rows: slice }
  }, [sortedData, page, rowsPerPage])

  // Virtualization (react-window) dynamic import (must be after paginatedData declaration)
  const [RW, setRW] = useState<any>(null)
  const listRef = useRef<any>(null)
  useEffect(() => {
    const count = paginatedData?.rows.length || 0
    if (count > 50 && !RW) {
      import('react-window').then((mod) => setRW(mod)).catch(() => { })
    }
  }, [paginatedData?.rows.length, RW])
  const shouldVirtualize = !!RW && (paginatedData?.rows.length || 0) > 50

  // Identify numeric columns using shared detection utility
  const numericColumns = useMemo(() => {
    if (!data) return []
    return detectNumericColumnIndexes(
      data.columns,
      data.rows,
      (row, _column, index) => (Array.isArray(row) ? row[index] : (row as any)?.[index]),
      50,
    )
  }, [data])

  const visibleNumericIndexes = useMemo(() => {
    const set = new Set(visibleColumnIndexes)
    return numericColumns.filter(idx => set.has(idx))
  }, [numericColumns, visibleColumnIndexes])

  // Aggregate calculations for visible numeric columns (on full sortedData)
  const aggregates = useMemo(() => {
    const result: Record<number, { sum: number; avg: number } | undefined> = {}
    if (!sortedData) return result
    visibleNumericIndexes.forEach((idx) => {
      let sum = 0
      let count = 0
      for (const row of sortedData.rows) {
        const n = Number(row[idx])
        if (Number.isFinite(n)) { sum += n; count += 1 }
      }
      result[idx] = { sum, avg: count ? sum / count : 0 }
    })
    return result
  }, [sortedData, visibleNumericIndexes])

  // Chart data preparation with explicit dimension/metric mapping
  const chartConfig = useMemo(() => {
    if (!data || data.rows.length === 0) return null
    if (numericColumns.length === 0) return null

    const allIndexes = data.columns.map((_, idx) => idx)

    // Prefer a non-numeric visible column as the dimension (X axis / label)
    const dimensionCandidates = allIndexes.filter(idx => !numericColumns.includes(idx))
    let dimensionIndex: number | null = null

    // 1) Visible non-numeric column
    for (const idx of dimensionCandidates) {
      if (visibleColumnIndexes.includes(idx)) {
        dimensionIndex = idx
        break
      }
    }

    // 2) Any non-numeric column
    if (dimensionIndex === null && dimensionCandidates.length > 0) {
      dimensionIndex = dimensionCandidates[0]
    }

    // 3) Fallback to first column
    if (dimensionIndex === null) {
      dimensionIndex = allIndexes[0] ?? 0
    }

    // 3) Fallback to first column
    if (dimensionIndex === null) {
      dimensionIndex = allIndexes[0] ?? 0
    }

    // Override with user selection if valid
    if (chartDimension && data.columns.includes(chartDimension)) {
      const idx = data.columns.indexOf(chartDimension)
      if (idx >= 0) dimensionIndex = idx
    }

    let metricIndex = numericColumns[0]
    // Override with user selection if valid
    if (chartMetric && data.columns.includes(chartMetric)) {
      const idx = data.columns.indexOf(chartMetric)
      if (idx >= 0 && numericColumns.includes(idx)) metricIndex = idx
    }

    const points = data.rows.slice(0, 50).map(row => ({
      dimension: row[dimensionIndex!]?.toString() || 'N/A',
      metric: Number(row[metricIndex]) || 0,
    }))

    return {
      data: points,
      dimensionKey: 'dimension' as const,
      metricKey: 'metric' as const,
      dimensionLabel: data.columns[dimensionIndex!] ?? 'Dimension',
      metricLabel: data.columns[metricIndex] ?? 'Value',
    }
  }, [data, numericColumns, visibleColumnIndexes])

  const handleTabChange = useCallback((_: React.SyntheticEvent, newValue: number) => setTabValue(newValue), [])
  const handleChangePage = useCallback((_: unknown, newPage: number) => setPage(newPage), [])
  const handleChangeRowsPerPage = useCallback((event: React.ChangeEvent<HTMLInputElement>) => { setRowsPerPage(parseInt(event.target.value, 10)); setPage(0) }, [])
  const handleExportClick = useCallback((event: React.MouseEvent<HTMLElement>) => setExportAnchorEl(event.currentTarget), [])
  const handleExportClose = useCallback(() => setExportAnchorEl(null), [])
  const handleColumnsClick = useCallback((event: React.MouseEvent<HTMLElement>) => setColumnsAnchorEl(event.currentTarget), [])
  const handleColumnsClose = useCallback(() => setColumnsAnchorEl(null), [])

  const toggleColumn = useCallback((col: string) => {
    setSelectedColumns(prev => {
      if (prev.includes(col)) return prev.filter(c => c !== col)
      return [...prev, col]
    })
  }, [])

  const handleRequestSort = useCallback((index: number) => {
    if (orderBy === index) {
      setOrder(prev => (prev === 'asc' ? 'desc' : 'asc'))
    } else {
      setOrderBy(index)
      setOrder('asc')
    }
  }, [orderBy])

  // Column resize handlers
  const onResizeMouseDown = useCallback((e: React.MouseEvent, colIndex: number) => {
    e.preventDefault()
    e.stopPropagation()
    const startX = e.clientX
    const startWidth = columnWidths[colIndex] || (e.currentTarget.parentElement as HTMLElement)?.offsetWidth || 160
    setResizing({ colIndex, startX, startWidth })
  }, [columnWidths])

  useEffect(() => {
    if (!resizing) return
    const onMouseMove = (e: MouseEvent) => {
      const delta = e.clientX - resizing.startX
      const newWidth = Math.max(80, Math.min(600, resizing.startWidth + delta))
      setColumnWidths(prev => ({ ...prev, [resizing.colIndex]: newWidth }))
    }
    const onMouseUp = () => setResizing(null)
    window.addEventListener('mousemove', onMouseMove)
    window.addEventListener('mouseup', onMouseUp)
    return () => {
      window.removeEventListener('mousemove', onMouseMove)
      window.removeEventListener('mouseup', onMouseUp)
    }
  }, [resizing])

  // Copy helpers
  const handleCopyClick = useCallback((event: React.MouseEvent<HTMLElement>) => setCopyAnchorEl(event.currentTarget), [])
  const handleCopyClose = useCallback(() => setCopyAnchorEl(null), [])

  const copyCell = useCallback(() => {
    if (!paginatedData) return
    const r = selectedCell.row, c = selectedCell.col
    if (r === null || c === null) return
    const value = paginatedData.rows[r]?.[visibleColumnIndexes[c]]
    navigator.clipboard.writeText(value == null ? 'NULL' : String(value))
    success('Cell copied')
    handleCopyClose()
  }, [paginatedData, selectedCell, visibleColumnIndexes, success, handleCopyClose])

  const copyRow = useCallback((sep: ',' | '\t' = ',') => {
    if (!paginatedData) return
    const r = selectedCell.row
    if (r === null) return
    const row = paginatedData.rows[r]
    const text = visibleColumnIndexes.map(i => row[i] == null ? '' : String(row[i])).join(sep)
    navigator.clipboard.writeText(text)
    success('Row copied')
    handleCopyClose()
  }, [paginatedData, selectedCell, visibleColumnIndexes, success, handleCopyClose])

  const copyColumn = useCallback((sep: '\n' = '\n') => {
    if (!paginatedData) return
    const c = selectedCell.col
    if (c === null) return
    const colIdx = visibleColumnIndexes[c]
    const text = paginatedData.rows.map(row => row[colIdx] == null ? '' : String(row[colIdx])).join(sep)
    navigator.clipboard.writeText(text)
    success('Column copied')
    handleCopyClose()
  }, [paginatedData, selectedCell, visibleColumnIndexes, success, handleCopyClose])

  const copyPageAs = useCallback((sep: ',' | '\t') => {
    if (!paginatedData || !data) return
    const header = visibleColumnIndexes.map(i => data.columns[i]).join(sep)
    const rows = paginatedData.rows.map(row => visibleColumnIndexes.map(i => row[i] ?? '').join(sep))
    const text = [header, ...rows].join('\n')
    navigator.clipboard.writeText(text)
    success(`Copied current page as ${sep === ',' ? 'CSV' : 'TSV'}`)
    handleCopyClose()
  }, [paginatedData, data, visibleColumnIndexes, success, handleCopyClose])

  const exportToCSV = useCallback(() => {
    if (!sortedData || !data) return
    const colIdx = visibleColumnIndexes
    const header = colIdx.map(i => data.columns[i])
    const csvRows = sortedData.rows.map(row => colIdx.map(i => `"${row[i] ?? ''}"`).join(','))
    const csvContent = [header.join(','), ...csvRows].join('\n')
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' })
    const link = document.createElement('a')
    const url = URL.createObjectURL(blob)
    link.setAttribute('href', url)
    link.setAttribute('download', `query_results_${new Date().toISOString().slice(0, 10)}.csv`)
    link.style.visibility = 'hidden'
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    handleExportClose()
  }, [sortedData, data, visibleColumnIndexes, handleExportClose])

  const exportToJSON = useCallback(() => {
    if (!sortedData || !data) return
    const colIdx = visibleColumnIndexes
    const jsonRows = sortedData.rows.map(row => {
      const obj: Record<string, any> = {}
      colIdx.forEach(i => { obj[data.columns[i]] = row[i] })
      return obj
    })
    const jsonData = { metadata: data.metadata, columns: colIdx.map(i => data.columns[i]), rows: jsonRows, exported_at: new Date().toISOString() }
    const blob = new Blob([JSON.stringify(jsonData, null, 2)], { type: 'application/json' })
    const link = document.createElement('a')
    const url = URL.createObjectURL(blob)
    link.setAttribute('href', url)
    link.setAttribute('download', `query_results_${new Date().toISOString().slice(0, 10)}.json`)
    link.style.visibility = 'hidden'
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    handleExportClose()
  }, [sortedData, data, visibleColumnIndexes, handleExportClose])

  const exportToXLSX = useCallback(async () => {
    if (!sortedData || !data) return
    if (data.metadata?.row_count && data.metadata.row_count >= 1000) {
      warning('Export may be truncated due to row limit')
    }
    try {
      const xlsx = await import('xlsx')
      const header = visibleColumnIndexes.map(i => data.columns[i])
      const rows = sortedData.rows.map(row => visibleColumnIndexes.map(i => row[i]))
      const aoa = [header, ...rows]
      const ws = xlsx.utils.aoa_to_sheet(aoa)
      const wb = xlsx.utils.book_new()
      xlsx.utils.book_append_sheet(wb, ws, 'Results')
      const wbout = xlsx.write(wb, { type: 'array', bookType: 'xlsx' })
      const blob = new Blob([wbout], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' })
      const link = document.createElement('a')
      const url = URL.createObjectURL(blob)
      link.setAttribute('href', url)
      link.setAttribute('download', `query_results_${new Date().toISOString().slice(0, 10)}.xlsx`)
      link.style.visibility = 'hidden'
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      handleExportClose()
      success('Excel exported')
    } catch (e) {
      showError('Install xlsx to enable Excel export: pnpm add xlsx')
    }
  }, [sortedData, data, visibleColumnIndexes, handleExportClose, warning, success, showError])

  const handleDisclosureChoice = useCallback((choice: 'preview' | 'download' | 'aggregate' | 'cancel') => {
    setDisclosureDialog({ ...disclosureDialog, open: false })

    if (choice === 'preview' && data) {
      // Already showing preview (first page)
      success('Showing preview of first 100 rows')
    } else if (choice === 'download' && data) {
      // Trigger export
      exportToCSV()
    } else if (choice === 'aggregate' && data) {
      // Show aggregates tab
      setTabValue(2) // Switch to aggregates view
      success('Showing aggregate summary')
    }
  }, [disclosureDialog, data, success, exportToCSV])

  if (loading) {
    return (
      <Paper elevation={0} sx={{ p: 3, border: '1px solid #e5e7eb', borderRadius: 2 }}>
        <Box display="flex" flexDirection="column" alignItems="center" justifyContent="center" minHeight={400}>
          <CircularProgress size={60} thickness={4} sx={{ mb: 3, color: '#10b981' }} />
          <Typography
            variant="h6"
            gutterBottom
            sx={{ fontFamily: '"Figtree", sans-serif', fontWeight: 600 }}
          >
            Executing request...
          </Typography>
          <Typography
            variant="body2"
            color="text.secondary"
            sx={{ fontFamily: '"Figtree", sans-serif' }}
          >
            Please wait while we process your request
          </Typography>
        </Box>
      </Paper>
    )
  }

  if (error) {
    return (
      <Paper elevation={0} sx={{ p: 3, border: '1px solid #e5e7eb', borderRadius: 2 }}>
        <Alert severity="error">
          <Typography
            variant="h6"
            gutterBottom
            sx={{ fontFamily: '"Figtree", sans-serif', fontWeight: 600 }}
          >
            Query Error
          </Typography>
          <Typography
            variant="body2"
            sx={{ fontFamily: '"Figtree", sans-serif' }}
          >
            {error}
          </Typography>
        </Alert>
      </Paper>
    )
  }

  if (!data) {
    return (
      <Paper elevation={0} sx={{ p: 3, border: '1px solid #e5e7eb', borderRadius: 2 }}>
        <Alert severity="info">
          <Typography sx={{ fontFamily: '"Figtree", sans-serif' }}>
            No query results to display. Execute a query to see results here.
          </Typography>
        </Alert>
      </Paper>
    )
  }

  const formatNumber = useMemo(() => new Intl.NumberFormat(undefined, { maximumFractionDigits: 3 }), [])
  const formatDate = useMemo(() => new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' }), [])

  const isBooleanLike = (v: any) => v === true || v === false || (typeof v === 'string' && /^(true|false)$/i.test(v))
  const isDateLike = (v: any) => typeof v === 'string' && (/^\d{4}-\d{2}-\d{2}/.test(v) || v.includes('T')) && !isNaN(Date.parse(v))

  const renderCell = (v: any) => {
    if (v === null || v === undefined) {
      return (
        <Typography variant="body2" color="text.secondary" fontStyle="italic" sx={{ fontFamily: '"Figtree", sans-serif', fontSize: '0.813rem' }}>
          NULL
        </Typography>
      )
    }
    if (typeof v === 'number' || (!isNaN(Number(v)) && v !== '')) {
      return <Typography component="span" sx={{ fontFamily: '"JetBrains Mono", monospace' }}>{formatNumber.format(Number(v))}</Typography>
    }
    if (isBooleanLike(v)) {
      const val = String(v).toLowerCase() === 'true'
      return <Chip label={val ? 'TRUE' : 'FALSE'} size="small" color={val ? 'success' : 'default'} sx={{ height: 20, fontSize: '0.65rem' }} />
    }
    if (isDateLike(v)) {
      return <Typography component="span" sx={{ fontFamily: '"JetBrains Mono", monospace' }}>{formatDate.format(new Date(v))}</Typography>
    }
    const s = v.toString()
    return (
      <Tooltip title={s.length > 120 ? s : ''} arrow disableInteractive>
        <span>{s}</span>
      </Tooltip>
    )
  }

  const onCellKeyDown = (e: React.KeyboardEvent<HTMLTableCellElement>, localRow: number, localCol: number) => {
    const totalRows = paginatedData?.rows.length || 0
    const totalCols = visibleColumnIndexes.length
    let nextRow = localRow
    let nextCol = localCol
    if (e.key === 'ArrowRight') nextCol = Math.min(totalCols - 1, localCol + 1)
    if (e.key === 'ArrowLeft') nextCol = Math.max(0, localCol - 1)
    if (e.key === 'ArrowDown') nextRow = Math.min(totalRows - 1, localRow + 1)
    if (e.key === 'ArrowUp') nextRow = Math.max(0, localRow - 1)
    if (nextRow !== localRow || nextCol !== localCol) {
      e.preventDefault()
      const selector = `td[data-row='${nextRow}'][data-col='${nextCol}']`
      let el = containerRef.current?.querySelector(selector) as HTMLElement | null
      if (!el && shouldVirtualize && listRef.current) {
        // Scroll into view then try again
        try { listRef.current.scrollToItem(nextRow, 'smart') } catch { }
        setTimeout(() => {
          const el2 = containerRef.current?.querySelector(selector) as HTMLElement | null
          el2?.focus()
        }, 30)
      } else {
        el?.focus()
      }
      setSelectedCell({ row: nextRow, col: nextCol })
    }
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'c') {
      // copy cell on Ctrl/Cmd+C
      e.preventDefault();
      setSelectedCell({ row: localRow, col: localCol })
      const value = paginatedData?.rows?.[localRow]?.[visibleColumnIndexes[localCol]]
      if (value !== undefined) {
        navigator.clipboard.writeText(value == null ? 'NULL' : String(value))
        success('Cell copied')
      }
    }
  }

  return (
    <>
      <Paper
        elevation={0}
        sx={{
          p: 2,
          borderRadius: 3,
          bgcolor: 'rgba(255,255,255,0.65)',
          border: '1px solid rgba(255,255,255,0.45)',
          backdropFilter: 'blur(8px) saturate(120%)',
        }}
      >
        {/* Header */}
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={1.5}>
          <Typography
            variant="subtitle1"
            sx={{
              fontFamily: '"Figtree", sans-serif',
              fontWeight: 600,
              fontSize: '1rem',
              color: '#111827',
            }}
          >
            Query Results
          </Typography>
          <Box display="flex" gap={1} alignItems="center">
            <Button size="small" variant="outlined" onClick={() => setCollapsed(!collapsed)} sx={{ textTransform: 'none' }}>
              {collapsed ? 'Show' : 'Hide'}
            </Button>
            <TextField
              size="small"
              placeholder="Search results..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon fontSize="small" />
                  </InputAdornment>
                ),
                sx: {
                  fontFamily: '"Figtree", sans-serif',
                  fontSize: '0.875rem',
                }
              }}
              sx={{ width: 200 }}
            />
            <Button
              variant="outlined"
              startIcon={<ColumnsIcon />}
              onClick={handleColumnsClick}
              size="small"
              sx={{ fontFamily: '"Figtree", sans-serif', textTransform: 'none', fontWeight: 500 }}
            >
              Columns
            </Button>
            <Menu anchorEl={columnsAnchorEl} open={Boolean(columnsAnchorEl)} onClose={handleColumnsClose}>
              {data.columns.map((col) => {
                const checked = selectedColumns.length === 0 || selectedColumns.includes(col)
                return (
                  <MenuItem key={col} onClick={() => toggleColumn(col)} dense>
                    <ListItemIcon>
                      <Checkbox edge="start" checked={checked} tabIndex={-1} disableRipple />
                    </ListItemIcon>
                    <ListItemText primaryTypographyProps={{ fontFamily: '"Figtree", sans-serif', fontSize: '0.875rem' }} primary={col} />
                  </MenuItem>
                )
              })}
            </Menu>
            <Button
              variant="outlined"
              startIcon={<CopyIcon />}
              onClick={handleCopyClick}
              size="small"
              sx={{ fontFamily: '"Figtree", sans-serif', textTransform: 'none', fontWeight: 500 }}
            >
              Copy
            </Button>
            <Menu anchorEl={copyAnchorEl} open={Boolean(copyAnchorEl)} onClose={handleCopyClose}>
              <MenuItem onClick={copyCell} disabled={selectedCell.row === null || selectedCell.col === null}>Copy cell</MenuItem>
              <MenuItem onClick={() => copyRow(',')} disabled={selectedCell.row === null}>Copy row (CSV)</MenuItem>
              <MenuItem onClick={() => copyRow('\t')} disabled={selectedCell.row === null}>Copy row (TSV)</MenuItem>
              <MenuItem onClick={() => copyColumn()} disabled={selectedCell.col === null}>Copy column</MenuItem>
              <MenuItem onClick={() => copyPageAs(',')}>Copy page as CSV</MenuItem>
              <MenuItem onClick={() => copyPageAs('\t')}>Copy page as TSV</MenuItem>
            </Menu>
            <Button
              variant="outlined"
              startIcon={<ExportIcon />}
              onClick={handleExportClick}
              size="small"
              sx={{ fontFamily: '"Figtree", sans-serif', textTransform: 'none', fontWeight: 500 }}
            >
              Export
            </Button>
            <Menu anchorEl={exportAnchorEl} open={Boolean(exportAnchorEl)} onClose={handleExportClose}>
              <MenuItem onClick={exportToCSV} sx={{ fontFamily: '"Figtree", sans-serif' }}>Export as CSV</MenuItem>
              <MenuItem onClick={exportToJSON} sx={{ fontFamily: '"Figtree", sans-serif' }}>Export as JSON</MenuItem>
              <MenuItem onClick={exportToXLSX} sx={{ fontFamily: '"Figtree", sans-serif' }}>Export as Excel (.xlsx)</MenuItem>
            </Menu>
            {shouldVirtualize && (
              <Chip label="Virtualized" size="small" sx={{ height: 22, fontSize: '0.65rem', fontWeight: 700, bgcolor: '#111827', color: 'white' }} />
            )}
          </Box>
        </Box>

        {/* Metadata */}
        {data.metadata && !collapsed && (
          <Box sx={{ mb: 2, p: 1.5, bgcolor: 'rgba(249,250,251,0.6)', borderRadius: 1.5, border: '1px solid #e5e7eb' }}>
            <Box display="flex" justifyContent="space-between" alignItems="center">
              <Box display="flex" gap={1.5} alignItems="center" flexWrap="wrap">
                <Chip
                  label={`${(sortedData?.rows.length ?? data.rows.length) || 0} rows`}
                  size="small"
                  sx={{ bgcolor: '#10b981', color: 'white', fontWeight: 600, fontSize: '0.75rem', fontFamily: '"Figtree", sans-serif', height: 24 }}
                />
                <Chip
                  label={`${visibleColumnIndexes.length} columns`}
                  size="small"
                  sx={{ bgcolor: '#6b7280', color: 'white', fontWeight: 600, fontSize: '0.75rem', fontFamily: '"Figtree", sans-serif', height: 24 }}
                />
                {data.metadata.execution_time && (
                  <Chip label={`${data.metadata.execution_time}ms`} size="small" sx={{ bgcolor: 'white', border: '1px solid #d1d5db', fontWeight: 500, fontSize: '0.75rem', fontFamily: '"Figtree", sans-serif', height: 24 }} />
                )}
                {data.metadata.row_count && data.metadata.row_count >= 1000 && (
                  <Chip
                    label="ROW LIMIT REACHED"
                    size="small"
                    sx={{ bgcolor: '#f59e0b', color: 'white', fontWeight: 700, fontSize: '0.7rem', height: 24 }}
                  />
                )}
              </Box>
              <IconButton size="small" onClick={() => setShowMetadata(!showMetadata)}>
                {showMetadata ? <CollapseIcon /> : <ExpandIcon />}
              </IconButton>
            </Box>
            <Collapse in={showMetadata}>
              <Box mt={2}>
                <Grid container spacing={2}>
                  {data.metadata.query_id && (
                    <Grid item xs={12} sm={6}>
                      <Typography variant="body2" color="text.secondary" sx={{ fontFamily: '"Figtree", sans-serif', fontSize: '0.65rem' }}>
                        Query ID: {data.metadata.query_id}
                      </Typography>
                    </Grid>
                  )}
                  {data.metadata.timestamp && (
                    <Grid item xs={12} sm={6}>
                      <Typography variant="body2" color="text.secondary" sx={{ fontFamily: '"Figtree", sans-serif', fontSize: '0.65rem' }}>
                        Executed: {new Date(data.metadata.timestamp).toLocaleString()}
                      </Typography>
                    </Grid>
                  )}
                </Grid>
              </Box>
            </Collapse>
          </Box>
        )}

        {/* Tabs */}
        {!collapsed && (
          <Tabs
            value={tabValue}
            onChange={handleTabChange}
            sx={{
              mb: 2,
              display: viewMode ? 'none' : 'flex',
              '& .MuiTab-root': {
                fontFamily: '"Figtree", sans-serif',
                textTransform: 'none',
                fontWeight: 500,
                fontSize: '0.875rem',
                minHeight: 36,
              }
            }}
          >
            <Tab icon={<TableIcon fontSize="small" />} label="Table" />
            <Tab icon={<ChartIcon fontSize="small" />} label="Chart" disabled={!chartConfig} />
            <Tab icon={<JsonIcon fontSize="small" />} label="JSON" />
          </Tabs>
        )}

        {!collapsed && (
          <TabPanel value={tabValue} index={1}>
            {/* Chart View */}
            {chartConfig ? (
              <Box>
                <Paper variant="outlined" sx={{ p: 2, mb: 3, bgcolor: 'rgba(255,255,255,0.5)' }}>
                  <Grid container spacing={3} alignItems="center">
                    <Grid item xs={12} md={4}>
                      <FormControl fullWidth size="small">
                        <InputLabel id="chart-type-label" sx={{ fontFamily: '"Figtree", sans-serif' }}>Chart Type</InputLabel>
                        <Select
                          labelId="chart-type-label"
                          value={chartType}
                          label="Chart Type"
                          onChange={(e) => setChartType(e.target.value as any)}
                          sx={{ fontFamily: '"Figtree", sans-serif' }}
                        >
                          <MenuItem value="bar" sx={{ fontFamily: '"Figtree", sans-serif' }}>Bar Chart</MenuItem>
                          <MenuItem value="line" sx={{ fontFamily: '"Figtree", sans-serif' }}>Line Chart</MenuItem>
                          <MenuItem value="pie" sx={{ fontFamily: '"Figtree", sans-serif' }}>Pie Chart</MenuItem>
                          <MenuItem value="area" sx={{ fontFamily: '"Figtree", sans-serif' }}>Area Chart</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <FormControl fullWidth size="small">
                        <InputLabel id="chart-dim-label" sx={{ fontFamily: '"Figtree", sans-serif' }}>X-Axis (Dimension)</InputLabel>
                        <Select
                          labelId="chart-dim-label"
                          value={chartDimension || ''}
                          label="X-Axis (Dimension)"
                          onChange={(e) => setChartDimension(e.target.value)}
                          sx={{ fontFamily: '"Figtree", sans-serif' }}
                        >
                          {data.columns.map((col) => (
                            <MenuItem key={col} value={col} sx={{ fontFamily: '"Figtree", sans-serif' }}>{col}</MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <FormControl fullWidth size="small">
                        <InputLabel id="chart-metric-label" sx={{ fontFamily: '"Figtree", sans-serif' }}>Y-Axis (Metric)</InputLabel>
                        <Select
                          labelId="chart-metric-label"
                          value={chartMetric || ''}
                          label="Y-Axis (Metric)"
                          onChange={(e) => setChartMetric(e.target.value)}
                          sx={{ fontFamily: '"Figtree", sans-serif' }}
                        >
                          {numericColumns.map((idx) => (
                            <MenuItem key={data.columns[idx]} value={data.columns[idx]} sx={{ fontFamily: '"Figtree", sans-serif' }}>{data.columns[idx]}</MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                    </Grid>
                  </Grid>
                </Paper>

                <Box height={400} width="100%">
                  <ResponsiveContainer width="100%" height="100%">
                    {chartType === 'bar' ? (
                      <BarChart data={chartConfig.data}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                        <XAxis
                          dataKey={chartConfig.dimensionKey}
                          tick={{ fontSize: 11 }}
                          label={{ value: chartConfig.dimensionLabel, position: 'insideBottom', offset: -5 }}
                        />
                        <YAxis
                          tick={{ fontSize: 11 }}
                          label={{ value: chartConfig.metricLabel, angle: -90, position: 'insideLeft' }}
                        />
                        <RechartsTooltip formatter={(value: any) => [formatNumber.format(Number(value) || 0), chartConfig.metricLabel]} />
                        <Legend />
                        <Bar dataKey={chartConfig.metricKey} fill="#10b981" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    ) : chartType === 'line' ? (
                      <LineChart data={chartConfig.data}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                        <XAxis
                          dataKey={chartConfig.dimensionKey}
                          tick={{ fontSize: 11 }}
                          label={{ value: chartConfig.dimensionLabel, position: 'insideBottom', offset: -5 }}
                        />
                        <YAxis
                          tick={{ fontSize: 11 }}
                          label={{ value: chartConfig.metricLabel, angle: -90, position: 'insideLeft' }}
                        />
                        <RechartsTooltip formatter={(value: any) => [formatNumber.format(Number(value) || 0), chartConfig.metricLabel]} />
                        <Legend />
                        <Line type="monotone" dataKey={chartConfig.metricKey} stroke="#10b981" strokeWidth={2} dot={{ r: 4 }} activeDot={{ r: 6 }} />
                      </LineChart>
                    ) : chartType === 'area' ? (
                      <AreaChart data={chartConfig.data}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                        <XAxis
                          dataKey={chartConfig.dimensionKey}
                          tick={{ fontSize: 11 }}
                          label={{ value: chartConfig.dimensionLabel, position: 'insideBottom', offset: -5 }}
                        />
                        <YAxis
                          tick={{ fontSize: 11 }}
                          label={{ value: chartConfig.metricLabel, angle: -90, position: 'insideLeft' }}
                        />
                        <RechartsTooltip formatter={(value: any) => [formatNumber.format(Number(value) || 0), chartConfig.metricLabel]} />
                        <Legend />
                        <Area type="monotone" dataKey={chartConfig.metricKey} stroke="#10b981" fill="#10b981" fillOpacity={0.2} />
                      </AreaChart>
                    ) : (
                      <PieChart>
                        <Pie
                          data={chartConfig.data}
                          cx="50%"
                          cy="50%"
                          labelLine={false}
                          label={({ payload, percent }: any) => `${payload[chartConfig.dimensionKey].substring(0, 15)} ${(percent * 100).toFixed(0)}%`}
                          outerRadius={120}
                          fill="#8884d8"
                          dataKey={chartConfig.metricKey}
                          nameKey={chartConfig.dimensionKey}
                        >
                          {chartConfig.data.map((_entry, index) => (
                            <Cell key={`cell-${index}`} fill={`hsla(${(index * 360) / chartConfig.data.length}, 70%, 50%, 0.8)`} />
                          ))}
                        </Pie>
                        <RechartsTooltip formatter={(value: any) => [formatNumber.format(Number(value) || 0), chartConfig.metricLabel]} />
                        <Legend />
                      </PieChart>
                    )}
                  </ResponsiveContainer>
                </Box>
              </Box>
            ) : (
              <Alert severity="info">
                <Typography>No numeric data available for charting. The chart view requires at least one numeric column.</Typography>
              </Alert>
            )}
          </TabPanel>
        )}
        {!collapsed && (
          <TabPanel value={tabValue} index={0}>
            {/* Table View */}
            <TableContainer sx={{ maxHeight: 500, overflowX: 'auto' }} ref={containerRef}>
              <Table stickyHeader size="small" aria-label="query-results-table" sx={{ minWidth: 600, tableLayout: 'fixed' }}>
                <TableHead>
                  <TableRow>
                    {visibleColumnIndexes.map((colIndex, visIdx) => (
                      <TableCell
                        key={colIndex}
                        sortDirection={orderBy === colIndex ? order : false}
                        sx={{
                          fontWeight: 700,
                          fontFamily: '"Figtree", sans-serif',
                          fontSize: '0.75rem',
                          bgcolor: '#f9fafb',
                          color: '#111827',
                          borderBottom: '2px solid #e5e7eb',
                          borderRight: visIdx < visibleColumnIndexes.length - 1 ? '1px solid #e5e7eb' : 'none',
                          py: 0.6,
                          position: 'relative',
                          width: columnWidths[colIndex] ? `${columnWidths[colIndex]}px` : undefined,
                          whiteSpace: 'nowrap',
                          textAlign: visibleNumericIndexes.includes(colIndex) ? 'right' : 'left',
                        }}
                      >
                        <TableSortLabel
                          active={orderBy === colIndex}
                          direction={orderBy === colIndex ? order : 'asc'}
                          onClick={() => handleRequestSort(colIndex)}
                          hideSortIcon={false}
                        >
                          {data.columns[colIndex]}
                        </TableSortLabel>
                        {/* Resizer */}
                        <Box
                          onMouseDown={(e) => onResizeMouseDown(e, colIndex)}
                          sx={{
                            position: 'absolute',
                            top: 0,
                            right: -4,
                            width: 8,
                            height: '100%',
                            cursor: 'col-resize',
                            zIndex: 2,
                          }}
                        />
                      </TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                {shouldVirtualize ? (
                  <TableBody component={Box} sx={{ display: 'block', height: 500, position: 'relative' }}>
                    {RW && (
                      <RW.FixedSizeList
                        ref={listRef}
                        itemCount={paginatedData?.rows.length || 0}
                        itemSize={32}
                        height={Math.min(500, (paginatedData?.rows.length || 0) * 32)}
                        width="100%"
                      >
                        {({ index, style }: any) => {
                          const row = paginatedData!.rows[index]
                          return (
                            <Box style={style} key={index}>
                              <TableRow hover sx={{ '&:hover': { bgcolor: '#f9fafb' } }}>
                                {visibleColumnIndexes.map((colIndex, visIdx) => (
                                  <TableCell
                                    key={`${index}-${colIndex}`}
                                    data-row={index}
                                    data-col={visIdx}
                                    tabIndex={0}
                                    onKeyDown={(e) => onCellKeyDown(e, index, visIdx)}
                                    onClick={() => setSelectedCell({ row: index, col: visIdx })}
                                    sx={{
                                      fontFamily: '"Figtree", sans-serif',
                                      fontSize: '0.7rem',
                                      color: '#374151',
                                      py: 0.25,
                                      px: 0.8,
                                      borderRight: visIdx < visibleColumnIndexes.length - 1 ? '1px solid #e5e7eb' : 'none',
                                      width: columnWidths[colIndex] ? `${columnWidths[colIndex]}px` : undefined,
                                      outline: selectedCell.row === index && selectedCell.col === visIdx ? '2px solid #10b981' : 'none',
                                      outlineOffset: -2,
                                      overflow: 'hidden',
                                      textOverflow: 'ellipsis',
                                      whiteSpace: 'nowrap',
                                      textAlign: visibleNumericIndexes.includes(colIndex) ? 'right' : 'left',
                                    }}
                                  >
                                    {renderCell(row[colIndex])}
                                  </TableCell>
                                ))}
                              </TableRow>
                            </Box>
                          )
                        }}
                      </RW.FixedSizeList>
                    )}
                  </TableBody>
                ) : (
                  <TableBody>
                    {paginatedData?.rows.map((row, rowIndex) => (
                      <TableRow key={rowIndex} hover sx={{ '&:hover': { bgcolor: '#f9fafb' } }}>
                        {visibleColumnIndexes.map((colIndex, visIdx) => (
                          <TableCell
                            key={`${rowIndex}-${colIndex}`}
                            data-row={rowIndex}
                            data-col={visIdx}
                            tabIndex={0}
                            onKeyDown={(e) => onCellKeyDown(e, rowIndex, visIdx)}
                            onClick={() => setSelectedCell({ row: rowIndex, col: visIdx })}
                            sx={{
                              fontFamily: '"Figtree", sans-serif',
                              fontSize: '0.7rem',
                              color: '#374151',
                              py: 0.25,
                              px: 0.8,
                              borderRight: visIdx < visibleColumnIndexes.length - 1 ? '1px solid #e5e7eb' : 'none',
                              width: columnWidths[colIndex] ? `${columnWidths[colIndex]}px` : undefined,
                              outline: selectedCell.row === rowIndex && selectedCell.col === visIdx ? '2px solid #10b981' : 'none',
                              outlineOffset: -2,
                              overflow: 'hidden',
                              textOverflow: 'ellipsis',
                              whiteSpace: 'nowrap',
                              textAlign: visibleNumericIndexes.includes(colIndex) ? 'right' : 'left',
                            }}
                          >
                            {renderCell(row[colIndex])}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                )}
                {/* Aggregate Footer */}
                <TableFooter>
                  <TableRow>
                    {visibleColumnIndexes.map((colIndex, visIdx) => (
                      <TableCell key={`agg-${colIndex}`} sx={{ fontSize: '0.7rem', fontFamily: '"Figtree", sans-serif', bgcolor: '#f9fafb', borderTop: '2px solid #e5e7eb' }}>
                        {visIdx === 0 ? (
                          <Typography variant="caption" sx={{ fontWeight: 700 }}>Count: {sortedData?.rows.length || 0}</Typography>
                        ) : aggregates[colIndex] ? (
                          <Typography variant="caption" sx={{ display: 'flex', gap: 1 }}>
                            <Box component="span"> {formatNumber.format(aggregates[colIndex]!.sum)}</Box>
                            <Box component="span"> {formatNumber.format(aggregates[colIndex]!.avg)}</Box>
                          </Typography>
                        ) : null}
                      </TableCell>
                    ))}
                  </TableRow>
                </TableFooter>
              </Table>
            </TableContainer>

            <TablePagination
              rowsPerPageOptions={[10, 25, 50, 100]}
              component="div"
              count={sortedData?.rows.length || 0}
              rowsPerPage={rowsPerPage}
              page={page}
              onPageChange={handleChangePage}
              onRowsPerPageChange={handleChangeRowsPerPage}
              sx={{
                '& .MuiTablePagination-selectLabel, & .MuiTablePagination-displayedRows': { fontFamily: '"Figtree", sans-serif', fontSize: '0.875rem' },
                '& .MuiSelect-select': { fontFamily: '"Figtree", sans-serif' }
              }}
            />
          </TabPanel>
        )}



        <TabPanel value={tabValue} index={2}>
          {/* Aggregate Summary + JSON View */}
          <Box sx={{ mb: 2 }}>
            {visibleNumericIndexes.length > 0 ? (
              <Paper variant="outlined" sx={{ p: 2, bgcolor: 'grey.50' }}>
                <Box display="flex" alignItems="center" gap={1} mb={1.5}>
                  <InfoIcon fontSize="small" color="primary" />
                  <Typography
                    variant="subtitle2"
                    sx={{ fontFamily: '"Figtree", sans-serif', fontWeight: 600, fontSize: '0.85rem' }}
                  >
                    Aggregate summary for numeric columns
                  </Typography>
                </Box>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Column</TableCell>
                      <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Sum</TableCell>
                      <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>Average</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {visibleNumericIndexes.map((idx) => {
                      const agg = aggregates[idx]
                      if (!agg) return null
                      return (
                        <TableRow key={idx}>
                          <TableCell sx={{ fontSize: '0.75rem', fontFamily: '"Figtree", sans-serif' }}>
                            {data.columns[idx]}
                          </TableCell>
                          <TableCell sx={{ fontSize: '0.75rem', fontFamily: '"JetBrains Mono", monospace' }}>
                            {formatNumber.format(agg.sum)}
                          </TableCell>
                          <TableCell sx={{ fontSize: '0.75rem', fontFamily: '"JetBrains Mono", monospace' }}>
                            {formatNumber.format(agg.avg)}
                          </TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </Paper>
            ) : (
              <Alert severity="info" sx={{ mb: 1 }}>
                <Typography variant="body2" sx={{ fontFamily: '"Figtree", sans-serif', fontSize: '0.8rem' }}>
                  No numeric columns detected for aggregate summary.
                </Typography>
              </Alert>
            )}
          </Box>

          {/* JSON View */}
          <Paper variant="outlined" sx={{ p: 2, bgcolor: 'grey.50', maxHeight: 400, overflow: 'auto' }}>
            <pre style={{ margin: 0, fontSize: '0.875rem' }}>
              {JSON.stringify({
                metadata: data.metadata,
                columns: data.columns,
                rows: data.rows,
              }, null, 2)}
            </pre>
          </Paper>
        </TabPanel>

      </Paper >

      {/* Progressive Disclosure Dialog */}
      <ProgressiveDisclosureDialog
        open={disclosureDialog.open}
        rowCount={disclosureDialog.rowCount}
        estimatedSize={disclosureDialog.estimatedSize}
        previewData={data ? {
          columns: data.columns,
          rows: data.rows.slice(0, 10)
        } : undefined}
        onClose={() => setDisclosureDialog({ ...disclosureDialog, open: false })}
        onChoiceSelected={handleDisclosureChoice}
      />
    </>
  )
}

export default QueryResults