/**
 * Table Density Toggle Component
 * Allows users to switch between Compact, Comfortable, and Spacious table views
 * Addresses improvement: Table Density & Scrollbars
 */

import React, { useState, useEffect } from 'react';
import { Maximize2, Minimize2, AlignJustify } from 'lucide-react';
import { cn } from '@/utils/cn';

export type TableDensity = 'compact' | 'comfortable' | 'spacious';

interface TableDensityToggleProps {
  value?: TableDensity;
  onChange?: (density: TableDensity) => void;
  className?: string;
  showLabels?: boolean;
}

export const TableDensityToggle: React.FC<TableDensityToggleProps> = ({
  value,
  onChange,
  className,
  showLabels = false,
}) => {
  const [density, setDensity] = useState<TableDensity>(value || 'comfortable');

  useEffect(() => {
    if (value) {
      setDensity(value);
    }
  }, [value]);

  const handleChange = (newDensity: TableDensity) => {
    setDensity(newDensity);
    if (onChange) {
      onChange(newDensity);
    }
    // Persist to localStorage
    localStorage.setItem('table-density', newDensity);
  };

  const densityOptions: Array<{
    value: TableDensity;
    label: string;
    icon: React.ElementType;
    description: string;
  }> = [
    {
      value: 'compact',
      label: 'Compact',
      icon: Minimize2,
      description: 'Maximum data density',
    },
    {
      value: 'comfortable',
      label: 'Comfortable',
      icon: AlignJustify,
      description: 'Balanced view',
    },
    {
      value: 'spacious',
      label: 'Spacious',
      icon: Maximize2,
      description: 'Maximum readability',
    },
  ];

  return (
    <div className={cn('inline-flex items-center gap-1 p-1 bg-gray-100 rounded-lg', className)}>
      {densityOptions.map((option) => {
        const Icon = option.icon;
        const isActive = density === option.value;

        return (
          <button
            key={option.value}
            onClick={() => handleChange(option.value)}
            className={cn(
              'inline-flex items-center gap-2 px-3 py-1.5 rounded-md text-sm font-medium transition-all duration-200',
              isActive
                ? 'bg-white text-gray-900 shadow-sm'
                : 'text-gray-600 hover:text-gray-900 hover:bg-gray-50'
            )}
            title={option.description}
          >
            <Icon className="w-4 h-4" />
            {showLabels && <span>{option.label}</span>}
          </button>
        );
      })}
    </div>
  );
};

/**
 * Hook to manage table density state
 */
export function useTableDensity(defaultDensity: TableDensity = 'comfortable') {
  const [density, setDensity] = useState<TableDensity>(() => {
    // Load from localStorage
    const saved = localStorage.getItem('table-density');
    return (saved as TableDensity) || defaultDensity;
  });

  const updateDensity = (newDensity: TableDensity) => {
    setDensity(newDensity);
    localStorage.setItem('table-density', newDensity);
  };

  return [density, updateDensity] as const;
}

/**
 * Get CSS classes for table density
 */
export function getTableDensityClasses(density: TableDensity): {
  table: string;
  header: string;
  cell: string;
  row: string;
} {
  const densityMap = {
    compact: {
      table: 'text-xs',
      header: 'px-2 py-1 h-8',
      cell: 'px-2 py-1',
      row: 'h-8',
    },
    comfortable: {
      table: 'text-sm',
      header: 'px-4 py-2 h-10',
      cell: 'px-4 py-2',
      row: 'h-10',
    },
    spacious: {
      table: 'text-base',
      header: 'px-6 py-3 h-12',
      cell: 'px-6 py-3',
      row: 'h-12',
    },
  };

  return densityMap[density];
}

/**
 * Table wrapper component with density support
 */
interface DensityAwareTableProps {
  children: React.ReactNode;
  density?: TableDensity;
  className?: string;
  stickyHeader?: boolean;
  maxHeight?: string;
}

export const DensityAwareTable: React.FC<DensityAwareTableProps> = ({
  children,
  density = 'comfortable',
  className,
  stickyHeader = true,
  maxHeight = '600px',
}) => {
  const classes = getTableDensityClasses(density);

  return (
    <div
      className={cn('relative overflow-auto border border-gray-200 rounded-lg', className)}
      style={{ maxHeight }}
    >
      <table className={cn('w-full border-collapse', classes.table)}>
        {children}
      </table>
      <style>{`
        /* Sticky header styles */
        ${stickyHeader
          ? `
          thead th {
            position: sticky;
            top: 0;
            z-index: 10;
            background-color: white;
            box-shadow: 0 1px 0 0 rgb(229, 231, 235);
          }
        `
          : ''}
      `}</style>
    </div>
  );
};

/**
 * Example usage component
 */
export const TableDensityExample: React.FC = () => {
  const [density, setDensity] = useTableDensity();
  const classes = getTableDensityClasses(density);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold">Query Results</h3>
        <TableDensityToggle value={density} onChange={setDensity} showLabels />
      </div>

      <DensityAwareTable density={density} stickyHeader maxHeight="500px">
        <thead>
          <tr className="bg-gray-50">
            <th className={cn('text-left font-semibold text-gray-700', classes.header)}>
              Column 1
            </th>
            <th className={cn('text-left font-semibold text-gray-700', classes.header)}>
              Column 2
            </th>
            <th className={cn('text-left font-semibold text-gray-700', classes.header)}>
              Column 3
            </th>
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: 20 }).map((_, i) => (
            <tr
              key={i}
              className={cn(
                'border-t border-gray-200 hover:bg-gray-50',
                classes.row
              )}
            >
              <td className={cn('text-gray-900', classes.cell)}>Data {i + 1}</td>
              <td className={cn('text-gray-600', classes.cell)}>Value {i + 1}</td>
              <td className={cn('text-gray-600', classes.cell)}>Result {i + 1}</td>
            </tr>
          ))}
        </tbody>
      </DensityAwareTable>
    </div>
  );
};
