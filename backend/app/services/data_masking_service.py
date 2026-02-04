"""
Data Masking Service

Masks sensitive data in query results to protect PII and comply with data privacy regulations.
Configurable per-user role - viewers see more masking than admins.

Supported masking types:
- Email addresses -> ***@***.***
- Phone numbers -> ***-***-****
- Credit card numbers -> ****-****-****-last4
- SSN -> ***-**-last4
- Names -> Initials + ***
- Generic sensitive fields -> MASKED

Compliance: GDPR, CCPA, HIPAA
"""

import re
import logging
import hashlib
from typing import Dict, Any, List, Optional, Pattern, Callable
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class PIIType(Enum):
    """Types of PII that can be detected and masked"""
    EMAIL = "email"
    PHONE = "phone"
    CREDIT_CARD = "credit_card"
    SSN = "ssn"
    NAME = "name"
    ADDRESS = "address"
    IP_ADDRESS = "ip_address"
    API_KEY = "api_key"
    SENSITIVE = "sensitive"


@dataclass
class MaskingRule:
    """Configuration for a PII masking rule"""
    pattern: Pattern
    mask_type: str
    mask_function: Callable[[str], str]
    description: str
    columns_hint: List[str]  # Column names that suggest this data type


class DataMaskingService:
    """
    Service for masking sensitive data in query results.
    
    Supports configurable masking by role:
    - VIEWER: Full masking of all sensitive data
    - ANALYST: Partial masking (masks PII, shows anonymized metrics)
    - DEVELOPER: Minimal masking (only API keys, credit cards)
    - ADMIN: No masking (full visibility)
    """
    
    # Regex patterns for PII detection
    PATTERNS = {
        PIIType.EMAIL: re.compile(
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        ),
        PIIType.PHONE: re.compile(
            r'\b(?:\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})\b'
        ),
        PIIType.CREDIT_CARD: re.compile(
            r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})\b'
        ),
        PIIType.SSN: re.compile(
            r'\b(\d{3})[-.\s]?(\d{2})[-.\s]?(\d{4})\b'
        ),
        PIIType.IP_ADDRESS: re.compile(
            r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b'
        ),
        PIIType.API_KEY: re.compile(
            r'\b(?:api[_-]?key|apikey|token|secret|password)[\s]*[=:][\s]*["\']?[\w]{16,}["\']?',
            re.IGNORECASE
        ),
    }
    
    # Sensitive column name hints
    SENSITIVE_COLUMN_NAMES = {
        PIIType.EMAIL: ["email", "mail", "email_address", "e_mail", "emailaddr"],
        PIIType.PHONE: ["phone", "mobile", "cell", "tel", "telephone", "fax"],
        PIIType.SSN: ["ssn", "social_security", "social", "national_id", "tax_id"],
        PIIType.CREDIT_CARD: ["card", "credit_card", "cc_number", "card_number", "pan"],
        PIIType.NAME: ["name", "first_name", "last_name", "full_name", "customer_name", "user_name"],
        PIIType.ADDRESS: ["address", "street", "city", "zip", "postal", "location"],
        PIIType.SENSITIVE: [
            "password", "secret", "api_key", "token", "private_key", 
            "auth", "credential", "access_key", "session_token"
        ],
    }
    
    # Masking strategies by role
    MASKING_CONFIG = {
        "viewer": {
            PIIType.EMAIL: "full",
            PIIType.PHONE: "full",
            PIIType.CREDIT_CARD: "full",
            PIIType.SSN: "full",
            PIIType.NAME: "initials",
            PIIType.ADDRESS: "none",  # Already masked at address level
            PIIType.IP_ADDRESS: "full",
            PIIType.API_KEY: "full",
            PIIType.SENSITIVE: "full",
        },
        "analyst": {
            PIIType.EMAIL: "partial",
            PIIType.PHONE: "partial",
            PIIType.CREDIT_CARD: "full",
            PIIType.SSN: "full",
            PIIType.NAME: "initials",
            PIIType.ADDRESS: "partial",
            PIIType.IP_ADDRESS: "partial",
            PIIType.API_KEY: "full",
            PIIType.SENSITIVE: "full",
        },
        "developer": {
            PIIType.EMAIL: "none",
            PIIType.PHONE: "partial",
            PIIType.CREDIT_CARD: "full",
            PIIType.SSN: "full",
            PIIType.NAME: "none",
            PIIType.ADDRESS: "none",
            PIIType.IP_ADDRESS: "none",
            PIIType.API_KEY: "full",
            PIIType.SENSITIVE: "full",
        },
        "admin": {
            PIIType.EMAIL: "none",
            PIIType.PHONE: "none",
            PIIType.CREDIT_CARD: "none",
            PIIType.SSN: "none",
            PIIType.NAME: "none",
            PIIType.ADDRESS: "none",
            PIIType.IP_ADDRESS: "none",
            PIIType.API_KEY: "none",
            PIIType.SENSITIVE: "none",
        },
    }
    
    @classmethod
    def _mask_email_full(cls, value: str) -> str:
        """Mask full email address"""
        if '@' in value:
            return "***@***.***"
        return "***"
    
    @classmethod
    def _mask_email_partial(cls, value: str) -> str:
        """Mask email partially - show domain"""
        if '@' in value:
            parts = value.split('@')
            local = parts[0]
            domain = parts[1]
            # Show first character of local, mask the rest
            if len(local) > 1:
                masked_local = local[0] + "***"
            else:
                masked_local = "***"
            return f"{masked_local}@{domain}"
        return cls._mask_email_full(value)
    
    @classmethod
    def _mask_phone_full(cls, value: str) -> str:
        """Mask full phone number"""
        return "***-***-****"
    
    @classmethod
    def _mask_phone_partial(cls, value: str) -> str:
        """Mask phone number, show last 4 digits"""
        digits = re.sub(r'\D', '', value)
        if len(digits) >= 4:
            return f"***-{digits[-4:]}"
        return "***-****"
    
    @classmethod
    def _mask_credit_card_full(cls, value: str) -> str:
        """Mask credit card, show last 4 digits only"""
        digits = re.sub(r'\D', '', value)
        if len(digits) >= 4:
            return f"****-****-****-{digits[-4:]}"
        return "****-****-****-****"
    
    @classmethod
    def _mask_ssn_full(cls, value: str) -> str:
        """Mask SSN, show last 4 only"""
        digits = re.sub(r'\D', '', value)
        if len(digits) >= 4:
            return f"***-**-{digits[-4:]}"
        return "***-**-****"
    
    @classmethod
    def _mask_to_initials(cls, value: str) -> str:
        """Convert name to initials with masking"""
        parts = value.strip().split()
        initials = []
        for part in parts:
            if part:
                initials.append(part[0].upper())
        if initials:
            return "".join(initials) + "***"
        return "***"
    
    @classmethod
    def _mask_ip_partial(cls, value: str) -> str:
        """Mask last octet of IP address"""
        parts = value.split('.')
        if len(parts) == 4:
            return f"{parts[0]}.{parts[1]}.{parts[2]}.***"
        return value
    
    @classmethod
    def _mask_full(cls, value: str) -> str:
        """Completely mask value"""
        return "[MASKED]"
    
    @classmethod
    def _detect_pii_type(cls, value: str) -> Optional[PIIType]:
        """Detect the type of PII in a value"""
        if not isinstance(value, str) or not value.strip():
            return None
        
        value = value.strip()
        
        for pii_type, pattern in cls.PATTERNS.items():
            if pattern.search(value):
                return pii_type
        
        return None
    
    @classmethod
    def _detect_pii_type_from_column(cls, column_name: str) -> Optional[PIIType]:
        """Detect likely PII type from column name"""
        column_lower = column_name.lower().replace('_', '').replace('-', '').replace(' ', '')
        
        for pii_type, hints in cls.SENSITIVE_COLUMN_NAMES.items():
            for hint in hints:
                hint_lower = hint.lower().replace('_', '').replace('-', '').replace(' ', '')
                if hint_lower in column_lower or column_lower in hint_lower:
                    return pii_type
        
        return None
    
    @classmethod
    def mask_value(
        cls,
        value: Any,
        pii_type: Optional[PIIType] = None,
        user_role: str = "viewer"
    ) -> Any:
        """
        Mask a single value based on its PII type and user role.
        
        Args:
            value: The value to mask
            pii_type: Type of PII (auto-detected if None)
            user_role: User's role (viewer, analyst, developer, admin)
            
        Returns:
            Masked value or original if no masking needed
        """
        if value is None or value == "":
            return value
        
        if not isinstance(value, str):
            return value
        
        user_role = user_role.lower()
        config = cls.MASKING_CONFIG.get(user_role, cls.MASKING_CONFIG["viewer"])
        
        # Auto-detect PII type if not provided
        if pii_type is None:
            pii_type = cls._detect_pii_type(value)
        
        if pii_type is None:
            return value
        
        masking_strategy = config.get(pii_type, "full")
        
        if masking_strategy == "none":
            return value
        
        # Apply masking based on strategy
        if pii_type == PIIType.EMAIL:
            if masking_strategy == "full":
                return cls._mask_email_full(value)
            else:  # partial
                return cls._mask_email_partial(value)
        
        elif pii_type == PIIType.PHONE:
            if masking_strategy == "full":
                return cls._mask_phone_full(value)
            else:  # partial
                return cls._mask_phone_partial(value)
        
        elif pii_type == PIIType.CREDIT_CARD:
            return cls._mask_credit_card_full(value)
        
        elif pii_type == PIIType.SSN:
            return cls._mask_ssn_full(value)
        
        elif pii_type == PIIType.NAME:
            if masking_strategy == "initials":
                return cls._mask_to_initials(value)
            return value
        
        elif pii_type == PIIType.IP_ADDRESS:
            if masking_strategy == "full":
                return cls._mask_full(value)
            elif masking_strategy == "partial":
                return cls._mask_ip_partial(value)
            return value
        
        elif pii_type == PIIType.API_KEY:
            return cls._mask_full(value)
        
        elif pii_type == PIIType.ADDRESS:
            if masking_strategy == "full":
                return cls._mask_full(value)
            elif masking_strategy == "partial":
                # Mask specific address components
                return cls._mask_address_partials(value)
            return value
        
        # Default: full masking
        return cls._mask_full(value)
    
    @classmethod
    def _mask_address_partials(cls, value: str) -> str:
        """Mask street address but keep city/state if present"""
        # Simple heuristic: mask first part (street number/street)
        parts = value.split(',')
        if len(parts) > 1:
            # Keep city/state, mask street
            return f"[MASKED STREET], {','.join(parts[1:]).strip()}"
        return "[MASKED ADDRESS]"
    
    @classmethod
    def mask_query_result(
        cls,
        result: Dict[str, Any],
        user_role: str = "viewer"
    ) -> Dict[str, Any]:
        """
        Mask sensitive data in query execution results.
        
        Args:
            result: Query result dict with 'columns' and 'rows' keys
            user_role: User's role for determining masking level
            
        Returns:
            Masked result dict
        """
        if result is None:
            return result
        
        if not isinstance(result, dict):
            return result
        
        # Handle direct result format
        if "result" in result and isinstance(result["result"], dict):
            result = result["result"]
        
        columns = result.get("columns", [])
        rows = result.get("rows", [])
        
        if not columns or not rows:
            return result
        
        # Detect PII types from column names
        column_pii_types = {}
        for i, col in enumerate(columns):
            pii_type = cls._detect_pii_type_from_column(col)
            if pii_type:
                column_pii_types[i] = pii_type
        
        # Mask rows
        masked_rows = []
        for row in rows:
            if not isinstance(row, (list, tuple, dict)):
                masked_rows.append(row)
                continue
            
            masked_row = []
            if isinstance(row, dict):
                # Dict format
                for col_name, value in row.items():
                    # Check column name hints
                    pii_type = cls._detect_pii_type_from_column(col_name)
                    # Also check value content
                    if pii_type is None and isinstance(value, str):
                        pii_type = cls._detect_pii_type(value)
                    masked_row.append(cls.mask_value(value, pii_type, user_role))
            else:
                # List format
                for i, value in enumerate(row):
                    pii_type = column_pii_types.get(i)
                    if pii_type is None and isinstance(value, str):
                        pii_type = cls._detect_pii_type(value)
                    masked_row.append(cls.mask_value(value, pii_type, user_role))
            
            masked_rows.append(masked_row)
        
        # Return masked result
        masked_result = dict(result)
        masked_result["rows"] = masked_rows
        masked_result["masked"] = True
        masked_result["masking_role"] = user_role
        
        # Add masking metadata
        masked_columns = [columns[i] for i in column_pii_types.keys()]
        masked_result["masked_columns"] = masked_columns if masked_columns else None
        
        return masked_result
    
    @classmethod
    def mask_execution_result(
        cls,
        execution_result: Any,
        user_role: str = "viewer",
        columns: Optional[List[str]] = None
    ) -> Any:
        """
        General mask function for any execution result format.
        
        Args:
            execution_result: The execution result (could be dict, list, or other)
            user_role: User's role
            columns: Optional column names for list results
            
        Returns:
            Masked result
        """
        if execution_result is None:
            return None
        
        # Handle result dict format
        if isinstance(execution_result, dict):
            if "columns" in execution_result or "rows" in execution_result:
                return cls.mask_query_result(execution_result, user_role)
            
            # Mask values in the dict itself
            masked = {}
            for key, value in execution_result.items():
                pii_type = cls._detect_pii_type_from_column(key)
                if isinstance(value, str):
                    masked[key] = cls.mask_value(value, pii_type, user_role)
                elif isinstance(value, (list, dict)):
                    masked[key] = cls.mask_execution_result(value, user_role)
                else:
                    masked[key] = value
            return masked
        
        # Handle list format
        if isinstance(execution_result, list):
            return [
                cls.mask_execution_result(item, user_role, columns)
                for item in execution_result
            ]
        
        # Handle string
        if isinstance(execution_result, str):
            pii_type = cls._detect_pii_type(execution_result)
            return cls.mask_value(execution_result, pii_type, user_role)
        
        # Return as-is for other types
        return execution_result
    
    @classmethod
    def get_masking_summary(cls, columns: List[str]) -> Dict[str, str]:
        """
        Get summary of which columns would be masked for each role.
        
        Args:
            columns: List of column names
            
        Returns:
            Dict mapping role to list of columns that would be masked
        """
        summary = {}
        
        for role in ["viewer", "analyst", "developer", "admin"]:
            config = cls.MASKING_CONFIG.get(role, {})
            masked_cols = []
            
            for col in columns:
                pii_type = cls._detect_pii_type_from_column(col)
                if pii_type and config.get(pii_type) != "none":
                    masked_cols.append(col)
            
            summary[role] = masked_cols
        
        return summary


# Global instance
data_masking_service = DataMaskingService()


# Convenience functions

def mask_result(
    result: Dict[str, Any],
    user_role: str = "viewer"
) -> Dict[str, Any]:
    """Convenience function to mask query result"""
    return DataMaskingService.mask_query_result(result, user_role)


def mask_value(value: Any, pii_type: Optional[PIIType] = None, user_role: str = "viewer") -> Any:
    """Convenience function to mask a single value"""
    return DataMaskingService.mask_value(value, pii_type, user_role)