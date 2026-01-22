/**
 * Result Diff Utility
 * Compares two result sets and identifies changed cells
 */

export interface CellDiff {
  rowIndex: number
  columnIndex: number
  oldValue: any
  newValue: any
  changeType: 'added' | 'removed' | 'modified'
}

/**
 * Compare two result sets and find differences
 */
export function compareResults(
  oldColumns: string[],
  oldRows: any[][],
  newColumns: string[],
  newRows: any[][]
): CellDiff[] {
  const diffs: CellDiff[] = []
  
  // Check if columns match
  if (JSON.stringify(oldColumns) !== JSON.stringify(newColumns)) {
    // Columns changed - mark all as different
    return []
  }
  
  // Compare row by row
  const maxRows = Math.max(oldRows.length, newRows.length)
  
  for (let rowIdx = 0; rowIdx < maxRows; rowIdx++) {
    const oldRow = oldRows[rowIdx]
    const newRow = newRows[rowIdx]
    
    // Row added
    if (!oldRow && newRow) {
      newRow.forEach((value, colIdx) => {
        diffs.push({
          rowIndex: rowIdx,
          columnIndex: colIdx,
          oldValue: null,
          newValue: value,
          changeType: 'added'
        })
      })
      continue
    }
    
    // Row removed
    if (oldRow && !newRow) {
      oldRow.forEach((value, colIdx) => {
        diffs.push({
          rowIndex: rowIdx,
          columnIndex: colIdx,
          oldValue: value,
          newValue: null,
          changeType: 'removed'
        })
      })
      continue
    }
    
    // Compare cells in row
    if (oldRow && newRow) {
      oldRow.forEach((oldValue, colIdx) => {
        const newValue = newRow[colIdx]
        
        // Check if values are different
        if (JSON.stringify(oldValue) !== JSON.stringify(newValue)) {
          diffs.push({
            rowIndex: rowIdx,
            columnIndex: colIdx,
            oldValue,
            newValue,
            changeType: 'modified'
          })
        }
      })
    }
  }
  
  return diffs
}

/**
 * Check if a specific cell has changed
 */
export function isCellChanged(
  rowIndex: number,
  columnIndex: number,
  diffs: CellDiff[]
): CellDiff | undefined {
  return diffs.find(d => d.rowIndex === rowIndex && d.columnIndex === columnIndex)
}
