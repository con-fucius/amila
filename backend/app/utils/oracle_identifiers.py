"""Utility helpers for working with Oracle identifiers.

Oracle treats unquoted identifiers as uppercase and requires double quotes
around identifiers that contain lowercase characters, special symbols, or
match reserved words. These helpers ensure we generate SQL that respects
those rules and avoids ORA-00904 (invalid identifier) errors.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, Optional

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Function
from sqlparse.tokens import Keyword, Name

# Minimal reserved word list covering identifiers we frequently encounter.
# Source: Oracle Database SQL Language Reference (checked Oct 2025).
ORACLE_RESERVED_WORDS = {
    "ACCESS",
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "ANY",
    "AS",
    "ASC",
    "BETWEEN",
    "BY",
    "CHECK",
    "CONNECT",
    "CREATE",
    "DATE",
    "DEFAULT",
    "DELETE",
    "DESC",
    "DISTINCT",
    "DROP",
    "ELSE",
    "EXISTS",
    "FOR",
    "FROM",
    "GRANT",
    "GROUP",
    "HAVING",
    "IN",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "LEVEL",
    "LIKE",
    "LOCK",
    "MINUS",
    "NOT",
    "NULL",
    "OF",
    "ON",
    "OR",
    "ORDER",
    "RENAME",
    "RESOURCE",
    "SELECT",
    "SET",
    "SIZE",
    "SYSDATE",
    "TABLE",
    "THEN",
    "TO",
    "TRIGGER",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "USER",
    "VALUES",
    "VIEW",
    "WHEN",
    "WHERE",
    "WITH",
}

_SAFE_IDENTIFIER_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]*$")


def _strip_quotes(identifier: str) -> str:
    if identifier.startswith('"') and identifier.endswith('"') and len(identifier) >= 2:
        return identifier[1:-1]
    return identifier


def needs_quotes(identifier: str) -> bool:
    """Return True if *identifier* must be surrounded by double quotes."""
    stripped = identifier.strip()
    if not stripped:
        return True

    if stripped.startswith('"') and stripped.endswith('"') and len(stripped) >= 2:
        return False

    upper = stripped.upper()
    if upper in ORACLE_RESERVED_WORDS:
        return True

    if stripped != upper:
        return True

    return _SAFE_IDENTIFIER_PATTERN.fullmatch(stripped) is None


def quote_identifier(identifier: str) -> str:
    """Return *identifier* quoted when required for Oracle."""
    stripped = identifier.strip()
    if not stripped:
        return '""'

    if stripped.startswith('"') and stripped.endswith('"') and len(stripped) >= 2:
        return stripped

    if not needs_quotes(stripped):
        return stripped.upper()

    inner = stripped.replace('"', '""')
    return f'"{inner}"'


def format_qualified_identifier(*parts: str) -> str:
    """Quote each identifier part as needed and join them with dots."""
    quoted_parts = [quote_identifier(part) for part in parts if part and part.strip()]
    return ".".join(quoted_parts)


def normalize_oracle_identifiers(sql_query: str, schema_metadata: Optional[Dict[str, any]] = None) -> str:
    """Ensure generated SQL uses Oracle-safe identifier quoting.

    Uses sqlparse to walk identifiers and applies our quoting helpers based on
    schema metadata (when provided) to keep column/table casing exact. This
    aligns with Oracle Database SQL Language Reference (Nov 2025) requirements
    that case-sensitive identifiers or reserved words be wrapped in double
    quotes.
    """

    if not sql_query:
        return sql_query

    statements = sqlparse.parse(sql_query)
    if not statements:
        return sql_query

    table_lookup: Dict[str, str] = {}
    columns_by_table: Dict[str, Dict[str, str]] = {}

    if schema_metadata:
        for collection in ("tables", "views"):
            for table_name, columns in schema_metadata.get(collection, {}).items():
                table_lookup[table_name.upper()] = table_name
                columns_by_table[table_name.upper()] = {
                    col.get("name", "").upper(): col.get("name", "") for col in columns
                }

    alias_map: Dict[str, str] = {}

    for statement in statements:
        _collect_table_aliases(statement.tokens, table_lookup, alias_map)

    for statement in statements:
        _normalize_token_list(statement.tokens, table_lookup, alias_map, columns_by_table)

    return "".join(str(statement) for statement in statements)


def _collect_table_aliases(tokens, table_lookup: Dict[str, str], alias_map: Dict[str, str]) -> None:
    for token in tokens:
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                _register_alias(identifier, table_lookup, alias_map)
        elif isinstance(token, Identifier):
            _register_alias(token, table_lookup, alias_map)
        elif getattr(token, "is_group", False):
            _collect_table_aliases(token.tokens, table_lookup, alias_map)


def _register_alias(identifier: Identifier, table_lookup: Dict[str, str], alias_map: Dict[str, str]) -> None:
    if not hasattr(identifier, "get_real_name"):
        return
    real = identifier.get_real_name()
    alias = identifier.get_alias()

    if not real:
        return

    real_key = real.strip('"').upper()
    if real_key in table_lookup:
        actual = table_lookup[real_key]
        alias_map[real_key] = actual
        if alias:
            alias_map[alias.strip('"').upper()] = actual


def _normalize_token_list(tokens, table_lookup: Dict[str, str], alias_map: Dict[str, str], columns_by_table: Dict[str, Dict[str, str]]) -> None:
    for token in tokens:
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                if isinstance(identifier, (Identifier, Function)):
                    _normalize_identifier(identifier, table_lookup, alias_map, columns_by_table)
                elif getattr(identifier, "ttype", None) in {Name, Keyword}:
                    _normalize_standalone_name(identifier, columns_by_table)
        elif isinstance(token, (Identifier, Function)):
            _normalize_identifier(token, table_lookup, alias_map, columns_by_table)
        elif getattr(token, "is_group", False):
            _normalize_token_list(token.tokens, table_lookup, alias_map, columns_by_table)


def _normalize_identifier(identifier: Identifier, table_lookup: Dict[str, str], alias_map: Dict[str, str], columns_by_table: Dict[str, Dict[str, str]]) -> None:
    if not isinstance(identifier, (Identifier, Function)):
        return

    # Check if this is a Function (not just an Identifier)
    if isinstance(identifier, Function):
        # Dive into function arguments
        _normalize_token_list(identifier.tokens, table_lookup, alias_map, columns_by_table)
        return

    alias = identifier.get_alias()
    alias_upper = alias.strip('"').upper() if alias else None

    parent = identifier.get_parent_name()
    parent_upper = parent.strip('"').upper() if parent else None
    parent_actual = None

    parent_is_alias = False
    if parent_upper and parent_upper in alias_map:
        parent_actual = alias_map[parent_upper]
        parent_is_alias = parent_upper not in table_lookup
    elif parent_upper and parent_upper in table_lookup:
        parent_actual = table_lookup[parent_upper]

    real = identifier.get_real_name()
    real_upper = real.strip('"').upper() if real else None

    normalized_real = None
    if real_upper:
        if parent_actual:
            col_map = columns_by_table.get(parent_actual.upper())
            if col_map and real_upper in col_map:
                normalized_real = quote_identifier(col_map[real_upper])
        if not normalized_real:
            # Attempt global match when unique across schema
            occurrences = set()
            for col_map in columns_by_table.values():
                if real_upper in col_map:
                    occurrences.add(col_map[real_upper])
            if len(occurrences) == 1:
                normalized_real = quote_identifier(occurrences.pop())

    if not normalized_real and real:
        normalized_real = quote_identifier(real)

    for tok in list(identifier.tokens):
        if isinstance(tok, IdentifierList):
            for sub in tok.get_identifiers():
                _normalize_identifier(sub, table_lookup, alias_map, columns_by_table)
        elif isinstance(tok, Identifier):
            _normalize_identifier(tok, table_lookup, alias_map, columns_by_table)
        elif tok.ttype is Name:
            token_clean = tok.value.strip('"')
            token_upper = token_clean.upper()

            if alias_upper and token_upper == alias_upper:
                continue

            if parent_upper and token_upper == parent_upper:
                if parent_is_alias:
                    continue
                if parent_actual and parent_actual != token_clean:
                    tok.value = quote_identifier(parent_actual)
                else:
                    tok.value = quote_identifier(token_clean)
                continue

            if real_upper and token_upper == real_upper and normalized_real:
                tok.value = normalized_real
                continue


def _normalize_standalone_name(token, columns_by_table: Dict[str, Dict[str, str]]) -> None:
    token_clean = token.value.strip('"')
    token_upper = token_clean.upper()

    occurrences = set()
    for col_map in columns_by_table.values():
        if token_upper in col_map:
            occurrences.add(col_map[token_upper])

    if len(occurrences) == 1:
        token.value = quote_identifier(occurrences.pop())
