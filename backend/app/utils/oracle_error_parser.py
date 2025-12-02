"""
Oracle Error Code Parser
Provides detailed context and suggestions for common Oracle errors
"""

import re
from typing import Dict, Optional

# Common Oracle error codes with explanations and suggestions
ORACLE_ERRORS = {
    "ORA-00001": {
        "title": "Unique constraint violated",
        "explanation": "Attempted to insert a duplicate value in a column with a unique constraint",
        "suggestion": "Check for existing records before inserting, or use MERGE statement"
    },
    "ORA-00904": {
        "title": "Invalid identifier",
        "explanation": "Column name doesn't exist in the table or is misspelled",
        "suggestion": "Verify column names match the schema exactly (case-sensitive)"
    },
    "ORA-00942": {
        "title": "Table or view does not exist",
        "explanation": "Referenced table/view doesn't exist or user lacks privileges",
        "suggestion": "Check table name spelling and schema permissions"
    },
    "ORA-01400": {
        "title": "Cannot insert NULL",
        "explanation": "Attempted to insert NULL into a NOT NULL column",
        "suggestion": "Provide a value for all required columns"
    },
    "ORA-01722": {
        "title": "Invalid number",
        "explanation": "Attempted to convert a non-numeric string to a number",
        "suggestion": "Ensure data types match or use TO_NUMBER with proper format"
    },
    "ORA-01747": {
        "title": "Invalid column specification",
        "explanation": "Column name contains invalid characters or syntax",
        "suggestion": "Use double quotes for reserved words or special characters"
    },
    "ORA-01789": {
        "title": "Query block has incorrect number of result columns",
        "explanation": "UNION/INTERSECT queries have mismatched column counts",
        "suggestion": "Ensure all query blocks return the same number of columns"
    },
    "ORA-01843": {
        "title": "Not a valid month",
        "explanation": "Invalid month value in date conversion",
        "suggestion": "Use correct date format (e.g., TO_DATE with 'DD-MON-YYYY')"
    },
    "ORA-12154": {
        "title": "TNS: could not resolve service name",
        "explanation": "Database connection string is invalid or not found",
        "suggestion": "Verify tnsnames.ora configuration or connection string"
    },
    "ORA-12541": {
        "title": "TNS: no listener",
        "explanation": "Database listener is not running",
        "suggestion": "Start the Oracle listener service"
    },
    "ORA-28000": {
        "title": "Account is locked",
        "explanation": "User account has been locked due to failed login attempts",
        "suggestion": "Contact DBA to unlock the account"
    },
    "ORA-28001": {
        "title": "Password has expired",
        "explanation": "User password needs to be changed",
        "suggestion": "Change password using ALTER USER statement"
    },
}


def parse_oracle_error(error_message: str) -> Dict[str, Optional[str]]:
    """
    Parse Oracle error message and provide detailed context
    
    Args:
        error_message: Raw error message from Oracle
        
    Returns:
        Dictionary with error code, title, explanation, and suggestion
    """
    # Extract ORA-##### error code
    match = re.search(r'ORA-(\d{5})', error_message, re.IGNORECASE)
    
    if not match:
        return {
            "error_code": None,
            "title": "Unknown Error",
            "explanation": error_message,
            "suggestion": "Review the full error message for details",
            "raw_message": error_message
        }
    
    error_code = f"ORA-{match.group(1)}"
    error_info = ORACLE_ERRORS.get(error_code, {
        "title": f"Oracle Error {error_code}",
        "explanation": "Uncommon Oracle error",
        "suggestion": "Consult Oracle documentation for this error code"
    })
    
    return {
        "error_code": error_code,
        "title": error_info["title"],
        "explanation": error_info["explanation"],
        "suggestion": error_info["suggestion"],
        "raw_message": error_message
    }


def extract_invalid_identifier(error_message: str) -> Optional[str]:
    """Extract the offending identifier from an ORA-00904 error message."""
    if not error_message:
        return None

    patterns = [
        r'ORA-00904:\s*"([^"]+)"',  # quoted identifier
        r"ORA-00904:\s*([^:\s]+)",  # token immediately after error code
        r'invalid identifier\s*:\s*"([^"]+)"',  # quoted near generic text
        r"invalid identifier\s+([^\s\"']+)",  # trailing token after message
    ]

    for pattern in patterns:
        match = re.search(pattern, error_message, re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            if candidate:
                return candidate.strip('"')

    return None


def format_oracle_error(error_message: str) -> str:
    """
    Format Oracle error with helpful context for display
    
    Args:
        error_message: Raw error message from Oracle
        
    Returns:
        Formatted error message with context
    """
    parsed = parse_oracle_error(error_message)
    
    if not parsed["error_code"]:
        return error_message
    
    return (
        f" {parsed['title']} ({parsed['error_code']})\n\n"
        f" What happened: {parsed['explanation']}\n\n"
        f" Suggestion: {parsed['suggestion']}\n\n"
        f"Raw error: {parsed['raw_message']}"
    )
