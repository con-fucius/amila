"""
SQL Utilities
- Basic SQL normalization for hashing/caching
"""
from __future__ import annotations

import re

_single_line_comment_re = re.compile(r"--.*?$", re.MULTILINE)
_multi_line_comment_re = re.compile(r"/\*.*?\*/", re.DOTALL)
_whitespace_re = re.compile(r"\s+")


def normalize_sql(sql: str, normalize_params: bool = False) -> str:
    """Lightweight SQL normalization.
    - Remove single/multi-line comments
    - Collapse whitespace to single space
    - Strip trailing semicolons
    - Optionally normalize parameter values for better cache hits
    Returns a normalized string suitable for hashing.
    """
    if not sql:
        return ""
    s = sql.strip()
    # Remove comments
    s = _single_line_comment_re.sub("", s)
    s = _multi_line_comment_re.sub("", s)
    
    # Optional: Normalize parameters for cache key consistency
    if normalize_params:
        # Replace date literals with placeholder
        s = re.sub(r"TO_DATE\s*\('[^']*'", "TO_DATE('<DATE>'", s, flags=re.IGNORECASE)
        # Replace numeric literals in WHERE/HAVING (but not FETCH FIRST)
        s = re.sub(
            r"(WHERE|HAVING|AND|OR)\s+([A-Za-z_][A-Za-z0-9_]*\s*[=<>!]+\s*)(\d+)",
            r"\1 \2<NUM>",
            s,
            flags=re.IGNORECASE
        )
    
    # Collapse whitespace
    s = _whitespace_re.sub(" ", s).strip()
    # Remove trailing semicolon
    if s.endswith(";"):
        s = s[:-1].strip()
    return s
