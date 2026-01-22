/**
 * Data Freshness Indicator Component
 * Displays "Last Updated" timestamp and data source metadata
 * Addresses improvement: Executive Visibility & Trust (Data Confidence)
 */

import React from 'react';
import { Clock, Database, RefreshCw } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';

interface DataFreshnessProps {
  timestamp?: string;
  dataSource?: string;
  executionTimeMs?: number;
  cacheStatus?: string;
  compact?: boolean;
}

export const DataFreshnessIndicator: React.FC<DataFreshnessProps> = ({
  timestamp,
  dataSource,
  executionTimeMs,
  cacheStatus = 'fresh',
  compact = false,
}) => {
  if (!timestamp && !dataSource) {
    return null;
  }

  const getRelativeTime = (isoTimestamp: string) => {
    try {
      return formatDistanceToNow(new Date(isoTimestamp), { addSuffix: true });
    } catch {
      return 'just now';
    }
  };

  const getCacheStatusColor = (status: string) => {
    switch (status) {
      case 'fresh':
        return 'text-green-600 bg-green-50';
      case 'cached':
        return 'text-blue-600 bg-blue-50';
      case 'stale':
        return 'text-amber-600 bg-amber-50';
      default:
        return 'text-gray-600 bg-gray-50';
    }
  };

  if (compact) {
    return (
      <div className="inline-flex items-center gap-2 text-xs text-gray-600">
        {timestamp && (
          <span className="flex items-center gap-1">
            <Clock className="w-3 h-3" />
            {getRelativeTime(timestamp)}
          </span>
        )}
        {dataSource && (
          <span className="flex items-center gap-1">
            <Database className="w-3 h-3" />
            {dataSource}
          </span>
        )}
      </div>
    );
  }

  return (
    <div className="flex items-center gap-3 px-3 py-2 bg-gray-50 border border-gray-200 rounded-lg text-sm">
      {timestamp && (
        <div className="flex items-center gap-2">
          <Clock className="w-4 h-4 text-gray-500" />
          <div className="flex flex-col">
            <span className="text-xs text-gray-500">Last Updated</span>
            <span className="font-medium text-gray-700">{getRelativeTime(timestamp)}</span>
          </div>
        </div>
      )}

      {dataSource && (
        <div className="flex items-center gap-2 pl-3 border-l border-gray-300">
          <Database className="w-4 h-4 text-gray-500" />
          <div className="flex flex-col">
            <span className="text-xs text-gray-500">Data Source</span>
            <span className="font-medium text-gray-700 uppercase">{dataSource}</span>
          </div>
        </div>
      )}

      {executionTimeMs !== undefined && (
        <div className="flex items-center gap-2 pl-3 border-l border-gray-300">
          <RefreshCw className="w-4 h-4 text-gray-500" />
          <div className="flex flex-col">
            <span className="text-xs text-gray-500">Execution Time</span>
            <span className="font-medium text-gray-700">
              {executionTimeMs < 1000
                ? `${executionTimeMs}ms`
                : `${(executionTimeMs / 1000).toFixed(2)}s`}
            </span>
          </div>
        </div>
      )}

      {cacheStatus && (
        <div className="ml-auto">
          <span
            className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${getCacheStatusColor(
              cacheStatus
            )}`}
          >
            {cacheStatus === 'fresh' && '● Live Data'}
            {cacheStatus === 'cached' && '● Cached'}
            {cacheStatus === 'stale' && '● Stale'}
          </span>
        </div>
      )}
    </div>
  );
};
