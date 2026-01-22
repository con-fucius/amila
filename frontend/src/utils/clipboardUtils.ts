/**
 * Clipboard Smart Copy Utilities
 * Formats data correctly for Excel/CSV with proper clipboard standards
 * Addresses improvement: Clipboard "Smart Copy"
 */

/**
 * Copy data to clipboard in multiple formats for maximum compatibility
 */
export async function smartCopyToClipboard(
  data: any[][],
  columns?: string[],
  format: 'auto' | 'tsv' | 'csv' | 'html' | 'json' = 'auto'
): Promise<boolean> {
  try {
    // Prepare data with headers if provided
    const fullData = columns ? [columns, ...data] : data;

    // Create clipboard items with multiple formats
    const clipboardItems: Record<string, Blob> = {};

    // 1. Plain text (TSV format - best for Excel)
    const tsvText = formatAsTSV(fullData);
    clipboardItems['text/plain'] = new Blob([tsvText], { type: 'text/plain' });

    // 2. HTML format (for rich paste into Word/Excel)
    const htmlText = formatAsHTML(fullData, columns);
    clipboardItems['text/html'] = new Blob([htmlText], { type: 'text/html' });

    // 3. CSV format (alternative)
    if (format === 'csv' || format === 'auto') {
      const csvText = formatAsCSV(fullData);
      clipboardItems['text/csv'] = new Blob([csvText], { type: 'text/csv' });
    }

    // Write to clipboard with multiple formats
    const clipboardItem = new ClipboardItem(clipboardItems);
    await navigator.clipboard.write([clipboardItem]);

    return true;
  } catch (error) {
    console.error('Smart copy failed:', error);
    // Fallback to simple text copy
    return fallbackCopy(data, columns);
  }
}

/**
 * Format data as TSV (Tab-Separated Values)
 * Best format for Excel - preserves formatting and handles special characters
 */
function formatAsTSV(data: any[][]): string {
  return data
    .map((row) =>
      row
        .map((cell) => {
          // Handle null/undefined
          if (cell === null || cell === undefined) return '';

          // Convert to string
          const str = String(cell);

          // Escape tabs and newlines
          return str.replace(/\t/g, ' ').replace(/\n/g, ' ').replace(/\r/g, '');
        })
        .join('\t')
    )
    .join('\n');
}

/**
 * Format data as CSV (Comma-Separated Values)
 * Standard format with proper escaping
 */
function formatAsCSV(data: any[][]): string {
  return data
    .map((row) =>
      row
        .map((cell) => {
          // Handle null/undefined
          if (cell === null || cell === undefined) return '';

          // Convert to string
          const str = String(cell);

          // Check if cell needs quoting (contains comma, quote, or newline)
          if (str.includes(',') || str.includes('"') || str.includes('\n')) {
            // Escape quotes by doubling them
            return `"${str.replace(/"/g, '""')}"`;
          }

          return str;
        })
        .join(',')
    )
    .join('\n');
}

/**
 * Format data as HTML table
 * Provides rich formatting for paste into Word/Excel
 */
function formatAsHTML(data: any[][], columns?: string[]): string {
  const hasHeaders = columns && columns.length > 0;

  let html = `<table border="1" cellpadding="4" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif; font-size: 12px;">`;

  // Add header row if columns provided
  if (hasHeaders) {
    html += '<thead><tr style="background-color: #f3f4f6; font-weight: bold;">';
    columns!.forEach((col) => {
      html += `<th style="padding: 8px; text-align: left; border: 1px solid #d1d5db;">${escapeHTML(
        col
      )}</th>`;
    });
    html += '</tr></thead>';
  }

  // Add data rows
  html += '<tbody>';
  data.forEach((row, rowIndex) => {
    const bgColor = rowIndex % 2 === 0 ? '#ffffff' : '#f9fafb';
    html += `<tr style="background-color: ${bgColor};">`;
    row.forEach((cell) => {
      const cellValue = cell === null || cell === undefined ? '' : String(cell);
      html += `<td style="padding: 6px; border: 1px solid #d1d5db;">${escapeHTML(
        cellValue
      )}</td>`;
    });
    html += '</tr>';
  });
  html += '</tbody></table>';

  return html;
}

/**
 * Escape HTML special characters
 */
function escapeHTML(str: string): string {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

/**
 * Fallback copy method using simple text
 */
async function fallbackCopy(data: any[][], columns?: string[]): Promise<boolean> {
  try {
    const fullData = columns ? [columns, ...data] : data;
    const text = formatAsTSV(fullData);
    await navigator.clipboard.writeText(text);
    return true;
  } catch (error) {
    console.error('Fallback copy failed:', error);
    return false;
  }
}

/**
 * Copy selected rows from a table
 */
export async function copySelectedRows(
  selectedRows: number[],
  allData: any[][],
  columns?: string[]
): Promise<boolean> {
  const selectedData = selectedRows.map((index) => allData[index]);
  return smartCopyToClipboard(selectedData, columns);
}

/**
 * Copy entire table with formatting
 */
export async function copyTable(
  data: any[][],
  columns: string[],
  options?: {
    includeHeaders?: boolean;
    format?: 'auto' | 'tsv' | 'csv' | 'html' | 'json';
  }
): Promise<boolean> {
  const { includeHeaders = true, format = 'auto' } = options || {};

  if (format === 'json') {
    return copyAsJSON(data, columns);
  }

  return smartCopyToClipboard(data, includeHeaders ? columns : undefined, format);
}

/**
 * Copy data as JSON
 */
async function copyAsJSON(data: any[][], columns: string[]): Promise<boolean> {
  try {
    // Convert to array of objects
    const jsonData = data.map((row) => {
      const obj: Record<string, any> = {};
      columns.forEach((col, index) => {
        obj[col] = row[index];
      });
      return obj;
    });

    const jsonText = JSON.stringify(jsonData, null, 2);
    await navigator.clipboard.writeText(jsonText);
    return true;
  } catch (error) {
    console.error('JSON copy failed:', error);
    return false;
  }
}

/**
 * Copy single cell value
 */
export async function copyCellValue(value: any): Promise<boolean> {
  try {
    const text = value === null || value === undefined ? '' : String(value);
    await navigator.clipboard.writeText(text);
    return true;
  } catch (error) {
    console.error('Cell copy failed:', error);
    return false;
  }
}

/**
 * Copy with visual feedback
 */
export async function copyWithFeedback(
  data: any[][],
  columns?: string[],
  onSuccess?: () => void,
  onError?: (error: Error) => void
): Promise<void> {
  try {
    const success = await smartCopyToClipboard(data, columns);
    if (success && onSuccess) {
      onSuccess();
    } else if (!success && onError) {
      onError(new Error('Copy failed'));
    }
  } catch (error) {
    if (onError) {
      onError(error as Error);
    }
  }
}

/**
 * Check if clipboard API is available
 */
export function isClipboardAvailable(): boolean {
  return (
    typeof navigator !== 'undefined' &&
    typeof navigator.clipboard !== 'undefined' &&
    typeof navigator.clipboard.write === 'function'
  );
}

/**
 * Format number for Excel (preserves precision)
 */
export function formatNumberForExcel(value: number): string {
  // Preserve full precision for Excel
  if (Number.isInteger(value)) {
    return value.toString();
  }
  // Use fixed notation to avoid scientific notation
  return value.toFixed(10).replace(/\.?0+$/, '');
}

/**
 * Format date for Excel (ISO 8601)
 */
export function formatDateForExcel(date: Date): string {
  return date.toISOString();
}

/**
 * Detect data type and format accordingly
 */
export function smartFormatCell(value: any): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'number') return formatNumberForExcel(value);
  if (value instanceof Date) return formatDateForExcel(value);
  return String(value);
}
