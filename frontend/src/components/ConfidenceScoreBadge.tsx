/**
 * Confidence Score Badge Component
 * Displays SQL generation confidence with visual indicators
 * Addresses improvement: Confidence Score Visualization
 */

import React from 'react';
import { CheckCircle, AlertTriangle, AlertCircle, HelpCircle } from 'lucide-react';

interface ConfidenceScoreBadgeProps {
  confidence?: number; // 0-100
  showLabel?: boolean;
  showIcon?: boolean;
  size?: 'sm' | 'md' | 'lg';
  showTooltip?: boolean;
}

export const ConfidenceScoreBadge: React.FC<ConfidenceScoreBadgeProps> = ({
  confidence,
  showLabel = true,
  showIcon = true,
  size = 'md',
  showTooltip = true,
}) => {
  if (confidence === undefined || confidence === null) {
    return null;
  }

  const getConfidenceLevel = (score: number): {
    level: string;
    color: string;
    bgColor: string;
    borderColor: string;
    icon: React.ReactNode;
    description: string;
  } => {
    if (score >= 90) {
      return {
        level: 'High Confidence',
        color: 'text-green-700',
        bgColor: 'bg-green-50',
        borderColor: 'border-green-200',
        icon: <CheckCircle className="w-4 h-4" />,
        description: 'Verified against schema with exact column matches',
      };
    } else if (score >= 70) {
      return {
        level: 'Good Confidence',
        color: 'text-blue-700',
        bgColor: 'bg-blue-50',
        borderColor: 'border-blue-200',
        icon: <CheckCircle className="w-4 h-4" />,
        description: 'Schema validated with minor assumptions',
      };
    } else if (score >= 50) {
      return {
        level: 'Medium Confidence',
        color: 'text-amber-700',
        bgColor: 'bg-amber-50',
        borderColor: 'border-amber-200',
        icon: <AlertTriangle className="w-4 h-4" />,
        description: 'Some column names inferred from context',
      };
    } else if (score >= 30) {
      return {
        level: 'Low Confidence',
        color: 'text-orange-700',
        bgColor: 'bg-orange-50',
        borderColor: 'border-orange-200',
        icon: <AlertCircle className="w-4 h-4" />,
        description: 'Multiple assumptions made - review recommended',
      };
    } else {
      return {
        level: 'Very Low Confidence',
        color: 'text-red-700',
        bgColor: 'bg-red-50',
        borderColor: 'border-red-200',
        icon: <HelpCircle className="w-4 h-4" />,
        description: 'Significant guesswork - manual review required',
      };
    }
  };

  const confidenceData = getConfidenceLevel(confidence);

  const sizeClasses = {
    sm: 'text-xs px-2 py-1',
    md: 'text-sm px-3 py-1.5',
    lg: 'text-base px-4 py-2',
  };

  const iconSizes = {
    sm: 'w-3 h-3',
    md: 'w-4 h-4',
    lg: 'w-5 h-5',
  };

  return (
    <div
      className={`inline-flex items-center gap-2 rounded-lg border ${confidenceData.bgColor} ${confidenceData.borderColor} ${sizeClasses[size]} ${confidenceData.color} font-medium`}
      title={showTooltip ? confidenceData.description : undefined}
    >
      {showIcon && (
        <span className={confidenceData.color}>
          {React.cloneElement(confidenceData.icon as React.ReactElement, {
            className: iconSizes[size],
          })}
        </span>
      )}

      {showLabel && (
        <span className="flex items-center gap-1.5">
          <span>{confidenceData.level}</span>
          <span className="font-bold">({confidence}%)</span>
        </span>
      )}

      {!showLabel && <span className="font-bold">{confidence}%</span>}
    </div>
  );
};

/**
 * Confidence Score with Progress Bar
 */
export const ConfidenceScoreBar: React.FC<{ confidence?: number }> = ({ confidence }) => {
  if (confidence === undefined || confidence === null) {
    return null;
  }

  const getBarColor = (score: number): string => {
    if (score >= 90) return 'bg-green-500';
    if (score >= 70) return 'bg-blue-500';
    if (score >= 50) return 'bg-amber-500';
    if (score >= 30) return 'bg-orange-500';
    return 'bg-red-500';
  };

  return (
    <div className="w-full">
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs font-medium text-gray-700">SQL Confidence</span>
        <span className="text-xs font-bold text-gray-900">{confidence}%</span>
      </div>
      <div className="w-full h-2 bg-gray-200 rounded-full overflow-hidden">
        <div
          className={`h-full ${getBarColor(confidence)} transition-all duration-300`}
          style={{ width: `${confidence}%` }}
        />
      </div>
    </div>
  );
};
