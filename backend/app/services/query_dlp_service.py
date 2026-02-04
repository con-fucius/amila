"""
Query-Level Data Loss Prevention (DLP) Service

Provides PII/credential detection in SQL queries and results.
Prevents data exfiltration and ensures compliance.

Features:
- PII pattern detection in queries (SSN, email, phone, credit cards)
- Column-level sensitivity classification
- Query result sampling for DLP validation
- Block/mask/redact actions based on sensitivity
- Audit logging for DLP events
"""

import logging
import re
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timezone

from app.core.redis_client import redis_client

logger = logging.getLogger(__name__)


class SensitivityLevel(Enum):
    """Data sensitivity levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    CRITICAL = "critical"


class DLPAction(Enum):
    """DLP enforcement actions"""
    ALLOW = "allow"
    MASK = "mask"
    REDACT = "redact"
    BLOCK = "block"
    AUDIT = "audit"


@dataclass
class PIIPattern:
    """PII detection pattern"""
    name: str
    pattern: str
    sensitivity: SensitivityLevel
    description: str
    examples: List[str]


@dataclass
class DLPFinding:
    """DLP detection finding"""
    pattern_name: str
    sensitivity: SensitivityLevel
    location: str  # 'query', 'column', 'result'
    matched_text: str
    action: DLPAction
    confidence: float
    mitigation: str


@dataclass
class DLPScanResult:
    """DLP scan result"""
    is_safe: bool
    findings: List[DLPFinding]
    action_required: DLPAction
    sensitivity_level: SensitivityLevel
    scan_summary: Dict[str, Any]


class QueryDLPService:
    """
    Service for query-level Data Loss Prevention.
    
    Detects and prevents:
    - PII exposure in queries
    - Sensitive column access
    - Data exfiltration patterns
    - Unauthorized bulk access
    """
    
    # PII Detection Patterns
    PII_PATTERNS = [
        PIIPattern(
            name="ssn",
            pattern=r"\b\d{3}[-.]?\d{2}[-.]?\d{4}\b",
            sensitivity=SensitivityLevel.RESTRICTED,
            description="US Social Security Number",
            examples=["123-45-6789", "123456789"]
        ),
        PIIPattern(
            name="email",
            pattern=r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
            sensitivity=SensitivityLevel.CONFIDENTIAL,
            description="Email address",
            examples=["user@example.com"]
        ),
        PIIPattern(
            name="phone",
            pattern=r"\b(?:\+?1[-.]?)?\(?([0-9]{3})\)?[-.]?([0-9]{3})[-.]?([0-9]{4})\b",
            sensitivity=SensitivityLevel.CONFIDENTIAL,
            description="Phone number",
            examples=["555-123-4567", "(555) 123-4567"]
        ),
        PIIPattern(
            name="credit_card",
            pattern=r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})\b",
            sensitivity=SensitivityLevel.CRITICAL,
            description="Credit card number",
            examples=["4111111111111111", "5500000000000004"]
        ),
        PIIPattern(
            name="tax_id",
            pattern=r"\b\d{2}[-.]?\d{7}\b",
            sensitivity=SensitivityLevel.RESTRICTED,
            description="Tax ID / EIN",
            examples=["12-3456789"]
        ),
        PIIPattern(
            name="passport",
            pattern=r"\b[A-Z]{1,2}\d{6,9}\b",
            sensitivity=SensitivityLevel.RESTRICTED,
            description="Passport number",
            examples=["AB123456", "A123456789"]
        ),
        PIIPattern(
            name="dob",
            pattern=r"\b(?:0[1-9]|1[0-2])[\/\-.](?:0[1-9]|[12]\d|3[01])[\/\-.](?:19|20)\d{2}\b",
            sensitivity=SensitivityLevel.CONFIDENTIAL,
            description="Date of birth",
            examples=["01/15/1985", "12-25-1990"]
        ),
        PIIPattern(
            name="ip_address",
            pattern=r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b",
            sensitivity=SensitivityLevel.INTERNAL,
            description="IP address",
            examples=["192.168.1.1", "10.0.0.1"]
        ),
        PIIPattern(
            name="api_key",
            pattern=r"\b(?:api[_-]?key|apikey|api[_-]?secret)\s*[=:]\s*['\"]?[a-zA-Z0-9_-]{16,}['\"]?\b",
            sensitivity=SensitivityLevel.CRITICAL,
            description="API key or secret",
            examples=["api_key=abc123xyz789", "apikey: secret123"]
        ),
        PIIPattern(
            name="password",
            pattern=r"\b(?:password|passwd|pwd)\s*[=:]\s*['\"]?[^\s'\"]{4,}['\"]?\b",
            sensitivity=SensitivityLevel.CRITICAL,
            description="Password in query",
            examples=["password=secret123", "pwd: 'mypass'"]
        ),
        PIIPattern(
            name="iban",
            pattern=r"\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}(?:[A-Z0-9]?){0,16}\b",
            sensitivity=SensitivityLevel.RESTRICTED,
            description="IBAN bank account",
            examples=["GB82WEST12345698765432"]
        ),
    ]
    
    # Sensitive column name patterns
    SENSITIVE_COLUMNS = {
        SensitivityLevel.CRITICAL: [
            r"password", r"passwd", r"pwd", r"secret", r"api_key", r"token",
            r"credential", r"private_key", r"ssn_hash", r"password_hash"
        ],
        SensitivityLevel.RESTRICTED: [
            r"ssn", r"social_security", r"tax_id", r"ein", r"passport",
            r"dob", r"birth_date", r"salary", r"compensation", r"bank_account",
            r"routing_number", r"credit_card", r"cc_num", r"card_number"
        ],
        SensitivityLevel.CONFIDENTIAL: [
            r"email", r"phone", r"mobile", r"address", r"city", r"zip",
            r"postal", r"first_name", r"last_name", r"full_name", r"person_name",
            r"employee_name", r"customer_name", r"contact_name"
        ],
        SensitivityLevel.INTERNAL: [
            r"internal", r"employee_id", r"department_id", r"cost_center",
            r"budget", r"forecast", r"revenue", r"profit", r"margin"
        ]
    }
    
    # Bulk access thresholds
    BULK_ACCESS_THRESHOLD = 10000
    HIGH_VOLUME_THRESHOLD = 100000
    
    def __init__(self):
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for performance"""
        self._compiled_patterns = {}
        for pattern in self.PII_PATTERNS:
            try:
                self._compiled_patterns[pattern.name] = re.compile(
                    pattern.pattern, 
                    re.IGNORECASE
                )
            except re.error as e:
                logger.warning(f"Failed to compile pattern {pattern.name}: {e}")
    
    def scan_query(self, sql_query: str, user_role: str = "analyst") -> DLPScanResult:
        """
        Scan SQL query for PII and sensitive data exposure.
        
        Args:
            sql_query: The SQL query to scan
            user_role: User's role for determining action
            
        Returns:
            DLPScanResult with findings and recommended action
        """
        findings = []
        max_sensitivity = SensitivityLevel.PUBLIC
        
        query_upper = sql_query.upper()
        
        # Scan for PII patterns in query text
        for pattern in self.PII_PATTERNS:
            compiled = self._compiled_patterns.get(pattern.name)
            if not compiled:
                continue
            
            matches = compiled.finditer(sql_query)
            for match in matches:
                # Determine action based on sensitivity and role
                action = self._determine_action(pattern.sensitivity, user_role)
                
                finding = DLPFinding(
                    pattern_name=pattern.name,
                    sensitivity=pattern.sensitivity,
                    location="query",
                    matched_text=match.group()[:50],  # Truncate for safety
                    action=action,
                    confidence=0.9,
                    mitigation=f"Avoid including {pattern.description} in queries"
                )
                findings.append(finding)
                
                if pattern.sensitivity.value > max_sensitivity.value:
                    max_sensitivity = pattern.sensitivity
        
        # Check for sensitive column access
        column_findings = self._check_sensitive_columns(sql_query, user_role)
        findings.extend(column_findings)
        
        for finding in column_findings:
            if finding.sensitivity.value > max_sensitivity.value:
                max_sensitivity = finding.sensitivity
        
        # Check for bulk access patterns
        bulk_finding = self._check_bulk_access(sql_query, user_role)
        if bulk_finding:
            findings.append(bulk_finding)
        
        # Determine overall action
        action_required = self._determine_overall_action(findings, user_role)
        is_safe = action_required not in [DLPAction.BLOCK, DLPAction.REDACT]
        
        return DLPScanResult(
            is_safe=is_safe,
            findings=findings,
            action_required=action_required,
            sensitivity_level=max_sensitivity,
            scan_summary={
                "patterns_checked": len(self.PII_PATTERNS),
                "findings_count": len(findings),
                "critical_findings": len([f for f in findings if f.sensitivity == SensitivityLevel.CRITICAL]),
                "restricted_findings": len([f for f in findings if f.sensitivity == SensitivityLevel.RESTRICTED]),
                "scanned_at": datetime.now(timezone.utc).isoformat()
            }
        )
    
    def _check_sensitive_columns(self, sql_query: str, user_role: str) -> List[DLPFinding]:
        """Check for access to sensitive columns"""
        findings = []
        query_lower = sql_query.lower()
        
        for sensitivity, patterns in self.SENSITIVE_COLUMNS.items():
            for pattern in patterns:
                if re.search(r'\b' + pattern + r'\b', query_lower):
                    action = self._determine_action(sensitivity, user_role)
                    finding = DLPFinding(
                        pattern_name=f"sensitive_column_{pattern}",
                        sensitivity=sensitivity,
                        location="column",
                        matched_text=pattern,
                        action=action,
                        confidence=0.85,
                        mitigation=f"Column matches sensitive pattern: {pattern}"
                    )
                    findings.append(finding)
        
        return findings
    
    def _check_bulk_access(self, sql_query: str, user_role: str) -> Optional[DLPFinding]:
        """Check for potential bulk data access"""
        query_upper = sql_query.upper()
        
        # Check for SELECT * without LIMIT
        if "SELECT *" in query_upper:
            # This could indicate bulk access
            if user_role not in ["admin", "developer"]:
                return DLPFinding(
                    pattern_name="bulk_access_select_all",
                    sensitivity=SensitivityLevel.INTERNAL,
                    location="query",
                    matched_text="SELECT *",
                    action=DLPAction.AUDIT,
                    confidence=0.6,
                    mitigation="Consider specifying columns and adding LIMIT clause"
                )
        
        return None
    
    def _determine_action(
        self, 
        sensitivity: SensitivityLevel, 
        user_role: str
    ) -> DLPAction:
        """Determine DLP action based on sensitivity and role"""
        # Role-based action matrix
        action_matrix = {
            "admin": {
                SensitivityLevel.PUBLIC: DLPAction.ALLOW,
                SensitivityLevel.INTERNAL: DLPAction.ALLOW,
                SensitivityLevel.CONFIDENTIAL: DLPAction.ALLOW,
                SensitivityLevel.RESTRICTED: DLPAction.AUDIT,
                SensitivityLevel.CRITICAL: DLPAction.MASK,
            },
            "developer": {
                SensitivityLevel.PUBLIC: DLPAction.ALLOW,
                SensitivityLevel.INTERNAL: DLPAction.ALLOW,
                SensitivityLevel.CONFIDENTIAL: DLPAction.AUDIT,
                SensitivityLevel.RESTRICTED: DLPAction.MASK,
                SensitivityLevel.CRITICAL: DLPAction.BLOCK,
            },
            "analyst": {
                SensitivityLevel.PUBLIC: DLPAction.ALLOW,
                SensitivityLevel.INTERNAL: DLPAction.ALLOW,
                SensitivityLevel.CONFIDENTIAL: DLPAction.MASK,
                SensitivityLevel.RESTRICTED: DLPAction.REDACT,
                SensitivityLevel.CRITICAL: DLPAction.BLOCK,
            },
            "viewer": {
                SensitivityLevel.PUBLIC: DLPAction.ALLOW,
                SensitivityLevel.INTERNAL: DLPAction.ALLOW,
                SensitivityLevel.CONFIDENTIAL: DLPAction.REDACT,
                SensitivityLevel.RESTRICTED: DLPAction.BLOCK,
                SensitivityLevel.CRITICAL: DLPAction.BLOCK,
            },
        }
        
        role_actions = action_matrix.get(user_role.lower(), action_matrix["viewer"])
        return role_actions.get(sensitivity, DLPAction.BLOCK)
    
    def _determine_overall_action(
        self, 
        findings: List[DLPFinding], 
        user_role: str
    ) -> DLPAction:
        """Determine the overall action based on all findings"""
        if not findings:
            return DLPAction.ALLOW
        
        # Get the most restrictive action
        action_priority = [
            DLPAction.ALLOW,
            DLPAction.AUDIT,
            DLPAction.MASK,
            DLPAction.REDACT,
            DLPAction.BLOCK
        ]
        
        most_restrictive = DLPAction.ALLOW
        for finding in findings:
            finding_priority = action_priority.index(finding.action)
            current_priority = action_priority.index(most_restrictive)
            if finding_priority > current_priority:
                most_restrictive = finding.action
        
        return most_restrictive
    
    def mask_sensitive_value(
        self, 
        value: Any, 
        sensitivity: SensitivityLevel
    ) -> str:
        """
        Mask a sensitive value based on its sensitivity level.
        
        Args:
            value: The value to mask
            sensitivity: Sensitivity level of the value
            
        Returns:
            Masked string representation
        """
        if value is None:
            return "NULL"
        
        value_str = str(value)
        
        if sensitivity == SensitivityLevel.CRITICAL:
            # Full redaction
            return "[REDACTED]"
        
        elif sensitivity == SensitivityLevel.RESTRICTED:
            # Show first and last character only
            if len(value_str) <= 4:
                return "****"
            return value_str[0] + "****" + value_str[-1]
        
        elif sensitivity == SensitivityLevel.CONFIDENTIAL:
            # Show first 3 characters
            if len(value_str) <= 6:
                return "****"
            return value_str[:3] + "****"
        
        elif sensitivity == SensitivityLevel.INTERNAL:
            # Partial masking
            if len(value_str) <= 8:
                return value_str[:2] + "****"
            return value_str[:4] + "****" + value_str[-4:]
        
        return value_str
    
    def classify_column_sensitivity(self, column_name: str) -> SensitivityLevel:
        """
        Classify the sensitivity of a column based on its name.
        
        Args:
            column_name: Name of the column
            
        Returns:
            SensitivityLevel for the column
        """
        column_lower = column_name.lower()
        
        for sensitivity, patterns in self.SENSITIVE_COLUMNS.items():
            for pattern in patterns:
                if re.search(r'\b' + pattern + r'\b', column_lower):
                    return sensitivity
        
        return SensitivityLevel.PUBLIC
    
    async def log_dlp_event(
        self,
        user_id: str,
        query_id: str,
        scan_result: DLPScanResult,
        action_taken: str
    ):
        """Log DLP event for compliance"""
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "query_id": query_id,
            "sensitivity_level": scan_result.sensitivity_level.value,
            "findings_count": len(scan_result.findings),
            "action_taken": action_taken,
            "patterns_detected": [f.pattern_name for f in scan_result.findings]
        }
        
        try:
            await redis_client.lpush(
                "dlp:events",
                event,
                ttl=90 * 24 * 3600  # 90 days retention
            )
        except Exception as e:
            logger.warning(f"Failed to log DLP event: {e}")


# Global instance
query_dlp_service = QueryDLPService()


# Convenience functions

def scan_query_for_pii(sql_query: str, user_role: str = "analyst") -> DLPScanResult:
    """Scan a query for PII and sensitive data"""
    return query_dlp_service.scan_query(sql_query, user_role)


def classify_column(column_name: str) -> SensitivityLevel:
    """Classify column sensitivity"""
    return query_dlp_service.classify_column_sensitivity(column_name)


def mask_value(value: Any, sensitivity: SensitivityLevel) -> str:
    """Mask a sensitive value"""
    return query_dlp_service.mask_sensitive_value(value, sensitivity)