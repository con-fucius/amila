import React, { useState } from 'react'
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  Alert,
  RadioGroup,
  FormControlLabel,
  Radio,
  Divider,
  Chip,
  CircularProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
} from '@mui/material'
import {
  Warning as WarningIcon,
  PreviewOutlined as PreviewIcon,
  GetApp as DownloadIcon,
  Functions as AggregateIcon,
  TableChart as TableIcon,
} from '@mui/icons-material'

interface ProgressiveDisclosureDialogProps {
  open: boolean
  rowCount: number
  estimatedSize?: string
  previewData?: {
    columns: string[]
    rows: any[][]
  }
  onClose: () => void
  onChoiceSelected: (choice: 'preview' | 'download' | 'aggregate' | 'cancel') => void
  loading?: boolean
}

type DisclosureChoice = 'preview' | 'download' | 'aggregate'

const ProgressiveDisclosureDialog: React.FC<ProgressiveDisclosureDialogProps> = ({
  open,
  rowCount,
  estimatedSize,
  previewData,
  onClose,
  onChoiceSelected,
  loading = false,
}) => {
  const [selectedChoice, setSelectedChoice] = useState<DisclosureChoice>('preview')

  const handleConfirm = () => {
    onChoiceSelected(selectedChoice)
  }

  const handleCancel = () => {
    onChoiceSelected('cancel')
    onClose()
  }

  const formatNumber = (num: number) => {
    return num.toLocaleString()
  }

  const getSeverity = () => {
    if (rowCount > 10000) return 'error'
    if (rowCount > 5000) return 'warning'
    return 'info'
  }

  const getIcon = () => {
    switch (selectedChoice) {
      case 'preview':
        return <PreviewIcon />
      case 'download':
        return <DownloadIcon />
      case 'aggregate':
        return <AggregateIcon />
    }
  }

  return (
    <Dialog
      open={open}
      onClose={handleCancel}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: {
          borderRadius: 2,
          minHeight: '500px',
        },
      }}
    >
      <DialogTitle sx={{ pb: 1 }}>
        <Box display="flex" alignItems="center" gap={1}>
          <WarningIcon color={getSeverity()} />
          <Typography variant="h6" component="span">
            Large Result Set Detected
          </Typography>
        </Box>
      </DialogTitle>

      <DialogContent>
        <Alert severity={getSeverity()} sx={{ mb: 3 }}>
          <Typography variant="body2" fontWeight="bold" gutterBottom>
            This query returned {formatNumber(rowCount)} rows
            {estimatedSize && ` (~${estimatedSize})`}
          </Typography>
          <Typography variant="body2">
            Loading all data may impact performance. Please choose how you'd like to proceed:
          </Typography>
        </Alert>

        <RadioGroup
          value={selectedChoice}
          onChange={(e) => setSelectedChoice(e.target.value as DisclosureChoice)}
        >
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {/* Preview Option */}
            <Paper
              variant="outlined"
              sx={{
                p: 2,
                cursor: 'pointer',
                border: selectedChoice === 'preview' ? 2 : 1,
                borderColor: selectedChoice === 'preview' ? 'primary.main' : 'divider',
                '&:hover': {
                  borderColor: 'primary.light',
                  bgcolor: 'action.hover',
                },
              }}
              onClick={() => setSelectedChoice('preview')}
            >
              <FormControlLabel
                value="preview"
                control={<Radio />}
                label={
                  <Box>
                    <Box display="flex" alignItems="center" gap={1} mb={0.5}>
                      <PreviewIcon color="primary" fontSize="small" />
                      <Typography variant="subtitle1" fontWeight="bold">
                        Preview First 100 Rows
                      </Typography>
                      <Chip label="Recommended" size="small" color="success" />
                    </Box>
                    <Typography variant="body2" color="text.secondary">
                      Quickly view a sample of the data without loading everything. Best for
                      exploration and verification.
                    </Typography>
                  </Box>
                }
                sx={{ m: 0, width: '100%' }}
              />
            </Paper>

            {/* Download Option */}
            <Paper
              variant="outlined"
              sx={{
                p: 2,
                cursor: 'pointer',
                border: selectedChoice === 'download' ? 2 : 1,
                borderColor: selectedChoice === 'download' ? 'primary.main' : 'divider',
                '&:hover': {
                  borderColor: 'primary.light',
                  bgcolor: 'action.hover',
                },
              }}
              onClick={() => setSelectedChoice('download')}
            >
              <FormControlLabel
                value="download"
                control={<Radio />}
                label={
                  <Box>
                    <Box display="flex" alignItems="center" gap={1} mb={0.5}>
                      <DownloadIcon color="primary" fontSize="small" />
                      <Typography variant="subtitle1" fontWeight="bold">
                        Download Full Results
                      </Typography>
                      {rowCount > 10000 && <Chip label="Slow" size="small" color="warning" />}
                    </Box>
                    <Typography variant="body2" color="text.secondary">
                      Export all {formatNumber(rowCount)} rows to CSV or Excel. This may take several
                      minutes for large datasets.
                    </Typography>
                  </Box>
                }
                sx={{ m: 0, width: '100%' }}
              />
            </Paper>

            {/* Aggregate Option */}
            <Paper
              variant="outlined"
              sx={{
                p: 2,
                cursor: 'pointer',
                border: selectedChoice === 'aggregate' ? 2 : 1,
                borderColor: selectedChoice === 'aggregate' ? 'primary.main' : 'divider',
                '&:hover': {
                  borderColor: 'primary.light',
                  bgcolor: 'action.hover',
                },
              }}
              onClick={() => setSelectedChoice('aggregate')}
            >
              <FormControlLabel
                value="aggregate"
                control={<Radio />}
                label={
                  <Box>
                    <Box display="flex" alignItems="center" gap={1} mb={0.5}>
                      <AggregateIcon color="primary" fontSize="small" />
                      <Typography variant="subtitle1" fontWeight="bold">
                        View Aggregated Summary
                      </Typography>
                      <Chip label="Fast" size="small" color="info" />
                    </Box>
                    <Typography variant="body2" color="text.secondary">
                      Generate a statistical summary (counts, averages, top values) without loading
                      raw data. Ideal for large datasets.
                    </Typography>
                  </Box>
                }
                sx={{ m: 0, width: '100%' }}
              />
            </Paper>
          </Box>
        </RadioGroup>

        {/* Preview Sample Data if available */}
        {previewData && previewData.rows.length > 0 && (
          <Box mt={3}>
            <Divider sx={{ mb: 2 }} />
            <Box display="flex" alignItems="center" gap={1} mb={1}>
              <TableIcon fontSize="small" color="action" />
              <Typography variant="subtitle2" color="text.secondary">
                Sample Data Preview (First 5 rows)
              </Typography>
            </Box>
            <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 200 }}>
              <Table size="small" stickyHeader>
                <TableHead>
                  <TableRow>
                    {previewData.columns.map((col, idx) => (
                      <TableCell key={idx} sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>
                        {col}
                      </TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {previewData.rows.slice(0, 5).map((row, rowIdx) => (
                    <TableRow key={rowIdx}>
                      {row.map((cell, cellIdx) => (
                        <TableCell key={cellIdx}>{cell?.toString() || '-'}</TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Box>
        )}
      </DialogContent>

      <DialogActions sx={{ px: 3, py: 2 }}>
        <Button onClick={handleCancel} disabled={loading}>
          Cancel
        </Button>
        <Button
          onClick={handleConfirm}
          variant="contained"
          startIcon={loading ? <CircularProgress size={16} /> : getIcon()}
          disabled={loading}
        >
          {loading
            ? 'Loading...'
            : selectedChoice === 'preview'
            ? 'Show Preview'
            : selectedChoice === 'download'
            ? 'Download'
            : 'Generate Summary'}
        </Button>
      </DialogActions>
    </Dialog>
  )
}

export default ProgressiveDisclosureDialog
