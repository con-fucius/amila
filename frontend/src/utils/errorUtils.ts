/**
 * Utility functions for extracting user-friendly error messages
 * from various error formats (objects, strings, arrays, etc.)
 */

/**
 * Extract a user-friendly error message from any error format
 * 
 * @param error - Error in any format (object, string, array, etc.)
 * @returns User-friendly error message string
 */
export function extractErrorMessage(error: any): string {
  // Handle null/undefined
  if (error == null) {
    return 'An unknown error occurred';
  }

  // Handle string errors
  if (typeof error === 'string') {
    return error || 'An error occurred';
  }

  // Handle Error objects
  if (error instanceof Error) {
    return error.message || 'An error occurred';
  }

  // Handle objects with common error properties
  if (typeof error === 'object') {
    // Try common error message properties in order of preference
    const messageProps = [
      'detail',
      'message',
      'error',
      'errorMessage',
      'error_message',
      'msg',
      'description',
    ];

    for (const prop of messageProps) {
      if (error[prop]) {
        // Recursively extract if the property is also an object
        if (typeof error[prop] === 'object') {
          const extracted = extractErrorMessage(error[prop]);
          if (extracted !== 'An unknown error occurred') {
            return extracted;
          }
        } else if (typeof error[prop] === 'string') {
          return error[prop];
        }
      }
    }

    // Handle arrays of errors
    if (Array.isArray(error)) {
      if (error.length > 0) {
        return error.map(e => extractErrorMessage(e)).join('; ');
      }
      return 'An error occurred';
    }

    // Handle objects with 'errors' array
    if (error.errors && Array.isArray(error.errors)) {
      if (error.errors.length > 0) {
        return error.errors.map((e: any) => extractErrorMessage(e)).join('; ');
      }
    }

    // Try to stringify if it's a plain object and hasn't been handled
    try {
      const stringified = JSON.stringify(error);
      if (stringified && stringified !== '{}' && stringified !== '[]') {
        // Truncate if too long
        return stringified.length > 200 
          ? stringified.substring(0, 200) + '...' 
          : stringified;
      }
    } catch {
      // JSON stringify failed, continue to default
    }
  }

  // Fallback
  return 'An unknown error occurred';
}

/**
 * Extract error details for debugging/logging
 * 
 * @param error - Error in any format
 * @returns Detailed error object for logging
 */
export function extractErrorDetails(error: any): Record<string, any> {
  if (error == null) {
    return { message: 'Unknown error', type: 'null' };
  }

  if (typeof error === 'string') {
    return { message: error, type: 'string' };
  }

  if (error instanceof Error) {
    return {
      message: error.message,
      name: error.name,
      stack: error.stack,
      type: 'Error',
    };
  }

  if (typeof error === 'object') {
    try {
      return {
        message: extractErrorMessage(error),
        raw: error,
        type: 'object',
      };
    } catch {
      return {
        message: 'Failed to extract error details',
        type: 'object',
      };
    }
  }

  return {
    message: String(error),
    type: typeof error,
  };
}

/**
 * Format an error for display in the UI
 * 
 * @param error - Error in any format
 * @param context - Additional context for the error
 * @returns Formatted error message with context
 */
export function formatErrorForDisplay(error: any, context?: string): string {
  const message = extractErrorMessage(error);
  
  if (context) {
    return `${context}: ${message}`;
  }

  return message;
}

/**
 * Check if an error indicates a network/connection issue
 * 
 * @param error - Error to check
 * @returns True if error is network-related
 */
export function isNetworkError(error: any): boolean {
  const message = extractErrorMessage(error).toLowerCase();
  return (
    message.includes('network') ||
    message.includes('connection') ||
    message.includes('fetch') ||
    message.includes('timeout') ||
    message.includes('cors') ||
    message.includes('not connected') ||
    message.includes('refused')
  );
}

/**
 * Check if an error indicates authentication/authorization issue
 * 
 * @param error - Error to check
 * @returns True if error is auth-related
 */
export function isAuthError(error: any): boolean {
  const message = extractErrorMessage(error).toLowerCase();
  return (
    message.includes('unauthorized') ||
    message.includes('unauthenticated') ||
    message.includes('forbidden') ||
    message.includes('permission denied') ||
    message.includes('access denied') ||
    message.includes('401') ||
    message.includes('403')
  );
}
