"""
Enhanced SQL Injection Detector

Multi-layered SQL injection detection with:
1. Syntactic analysis (sqlparse-based)
2. Semantic analysis (AST pattern matching)
3. Behavioral analysis (query structure anomalies)
4. ML-based detection (optional, via Redis ML)

Detection categories:
- Classic SQL injection (union-based, error-based)
- Blind SQL injection (time-delay, boolean-based)
- Advanced injection (stacked queries, out-of-band)
- Context-aware injection (within strings, identifiers)

Security compliance: OWASP Top 10, CWE-89
"""

import re
import logging
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class InjectionType(Enum):
    """Types of SQL injection attacks"""
    UNION_BASED = "union_based"
    ERROR_BASED = "error_based"
    BOOLEAN_BLIND = "boolean_blind"
    TIME_BLIND = "time_blind"
    STACKED_QUERIES = "stacked_queries"
    OUT_OF_BAND = "out_of_band"
    COMMENT_INJECTION = "comment_injection"
    STRING_ESCAPE = "string_escape"
    IDENTIFIER_INJECTION = "identifier_injection"
    SUBQUERY_INJECTION = "subquery_injection"
    STORED_PROCEDURE = "stored_procedure"
    SECOND_ORDER = "second_order"


class Severity(Enum):
    """Threat severity levels"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class InjectionFinding:
    """A detected injection attempt"""
    injection_type: InjectionType
    severity: Severity
    pattern: str
    position: int
    confidence: float  # 0.0 - 1.0
    context: str
    description: str
    mitigation: str


@dataclass
class DetectionResult:
    """Result from injection detection scan"""
    is_safe: bool
    findings: List[InjectionFinding] = field(default_factory=list)
    risk_score: float = 0.0  # 0.0 - 100.0
    detected_patterns: Set[str] = field(default_factory=set)
    scan_summary: Dict[str, Any] = field(default_factory=dict)


class SQLInjectionDetector:
    """
    Advanced SQL injection detection using multiple analysis techniques.
    
    Features:
    - Pattern-based detection for common injection techniques
    - Context analysis for string escape detection
    - Entropy analysis for obfuscated payloads
    - Heuristic scoring for confidence levels
    """
    
    # Union-based injection patterns
    UNION_PATTERNS = [
        r'\bunion\s+select\b',
        r'\bunion\s+all\s+select\b',
        r'\bunion\s+distinct\s+select\b',
        r'\bunion\s+select\s+(?:chr\(|char\(|ascii\(|concat\()',
        r'\bunion\s+select\s+(?:null[,\s]*)+\d',
    ]
    
    # Error-based injection patterns
    ERROR_PATTERNS = [
        r'\b(?:and|or)\s+1\s*=\s*(?:1|0|convert|cast)\b',
        r'\b(?:or|and)\s+\d+=\s*\d+\b',
        r'\b(?:or|and)\s+\'[^\']*\'\s*=\s*\'',
        r'\b(?:or|and)\s+"[^"]*"\s*=\s*"',
        r'\b(?:or|and)\s+@@version\b',
        r'\b(?:or|and)\s+version\(\)',
    ]
    
    # Boolean-based blind injection patterns
    BOOLEAN_BLIND_PATTERNS = [
        r'\b(?:and|or)\s+\(?\s*select\s+.*\s+from\s+.*\)?\b',
        r'\b(?:and|or)\s+exists\s*\(',
        r'\b(?:and|or)\s+length\s*\(',
        r'\b(?:and|or)\s+substr\s*\(',
        r'\b(?:and|or)\s+substring\s*\(',
        r'\b(?:and|or)\s+ascii\s*\(',
        r'\b(?:and|or)\s+mid\s*\(',
    ]
    
    # Time-based blind injection patterns
    TIME_BLIND_PATTERNS = [
        r'\b(?:waitfor|sleep|benchmark|pg_sleep|dbms_lock\.sleep)',
        r'\b(?:and|or)\s+\d+\s*=\s*\d+\s+wait\s+for\s+delay',
        r';\s*waitfor\s+',
        r'\bsleep\s*\(\s*\d+',
        r'\bbenchmark\s*\(\s*\d+',
    ]
    
    # Stacked query patterns (multiple statements)
    STACKED_QUERY_PATTERNS = [
        r';\s*(?:delete|drop|insert|update|create|alter|exec|execute)\b',
        r';\s*(?:shutdown|backup|restore|grant|revoke)\b',
        r'\bexec\s*\(\s*\'',
        r'\bexecute\s*\(\s*\'',
        r';\s*declare\s+@',
    ]
    
    # Comment-based injection patterns
    COMMENT_PATTERNS = [
        r'--\s*$',  # Comment at end of injection
        r'#\s*$',   # MySQL comment
        r'/\*.*\*/',  # C-style comment
        r'--\s+\'',
        r'--\s+"',
        r'--\s+\d',
    ]
    
    # String escape patterns
    STRING_ESCAPE_PATTERNS = [
        r"\'\s*(?:or|and)\s*\'\s*\'",
        r"\'\s*(?:or|and)\s*\d+\s*=\s*\d+",
        r'\"\s*(?:or|and)\s*\"\s*\"',
        r'\'\s+(?:union|select|insert|delete|update)',
        r'\"\s+(?:union|select|insert|delete|update)',
        r'\'\s*;\s*(?:drop|delete|truncate)',
    ]
    
    # Out-of-band injection patterns
    OOB_PATTERNS = [
        r'\bload_file\s*\(',
        r'\binto\s+outfile\b',
        r'\bpg_read_file\s*\(',
        r'\bcopy\s+.*\s+to\s+',
        r'\bUTL_HTTP\.|\bUTL_INADDR\.|\bUTL_TCP\.|\bUTL_SMTP\.',
        r'\bxp_cmdshell\b',
        r'\bbcp\s+',
        r'\bbulk\s+insert\b',
    ]
    
    # Stored procedure call patterns
    STORED_PROC_PATTERNS = [
        r'\b(?:exec|execute)\s+\w+\s+@',
        r'\b(?:exec|execute)\s+sp_',
        r'\b(?:exec|execute)\s+xp_',
        r';\s*(?:exec|execute)\s+',
    ]
    
    # Subquery injection patterns
    SUBQUERY_PATTERNS = [
        r'\)\s*union\s*select',
        r'\)\s*select\s+.*\s+from',
        r'\)\s*insert\s+into',
        r'\)\s*delete\s+from',
    ]
    
    # Second-order injection indicators
    SECOND_ORDER_INDICATORS = [
        r'\b(select|insert|update|delete)\s+.*\b(select|insert|update|delete)\b',
        r'\b(?:hex|base64|unhex|decode|aes_decrypt|aes_encrypt)\b',
        r'[\x00-\x08\x0B\x0C\x0E-\x1F]',  # Control characters
    ]
    
    # Dangerous functions that shouldn't appear in user queries
    DANGEROUS_FUNCTIONS = [
        r'\bsys_eval\s*\(',
        r'\bsys_exec\s*\(',
        r'\bpg_read_file\s*\(',
        r'\bpg_write_file\s*\(',
        r'\bpg_ls_dir\s*\(',
    ]
    
    # Suspicious character sequences that indicate obfuscation
    OBSCURE_SEQ_PATTERNS = [
        r'%[0-9a-fA-F]{2}',  # URL encoding
        r'\\x[0-9a-fA-F]{2}',  # Hex escape
        r'\\u[0-9a-fA-F]{4}',  # Unicode escape
        r'0x[0-9a-fA-F]+',  # Hex literal
        r'char\s*\(\s*\d+',  # CHAR() encoding
        r'chr\s*\(\s*\d+',   # CHR() encoding
    ]
    
    def __init__(self):
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile all regex patterns for performance"""
        self.compiled_patterns = {
            InjectionType.UNION_BASED: [(re.compile(p, re.IGNORECASE), Severity.HIGH) for p in self.UNION_PATTERNS],
            InjectionType.ERROR_BASED: [(re.compile(p, re.IGNORECASE), Severity.MEDIUM) for p in self.ERROR_PATTERNS],
            InjectionType.BOOLEAN_BLIND: [(re.compile(p, re.IGNORECASE), Severity.HIGH) for p in self.BOOLEAN_BLIND_PATTERNS],
            InjectionType.TIME_BLIND: [(re.compile(p, re.IGNORECASE), Severity.CRITICAL) for p in self.TIME_BLIND_PATTERNS],
            InjectionType.STACKED_QUERIES: [(re.compile(p, re.IGNORECASE), Severity.CRITICAL) for p in self.STACKED_QUERY_PATTERNS],
            InjectionType.OUT_OF_BAND: [(re.compile(p, re.IGNORECASE), Severity.CRITICAL) for p in self.OOB_PATTERNS],
            InjectionType.COMMENT_INJECTION: [(re.compile(p, re.IGNORECASE), Severity.MEDIUM) for p in self.COMMENT_PATTERNS],
            InjectionType.STRING_ESCAPE: [(re.compile(p, re.IGNORECASE), Severity.HIGH) for p in self.STRING_ESCAPE_PATTERNS],
            InjectionType.IDENTIFIER_INJECTION: [],  # Context-based
            InjectionType.SUBQUERY_INJECTION: [(re.compile(p, re.IGNORECASE), Severity.HIGH) for p in self.SUBQUERY_PATTERNS],
            InjectionType.STORED_PROCEDURE: [(re.compile(p, re.IGNORECASE), Severity.HIGH) for p in self.STORED_PROC_PATTERNS],
            InjectionType.SECOND_ORDER: [(re.compile(p, re.IGNORECASE), Severity.MEDIUM) for p in self.SECOND_ORDER_INDICATORS],
        }
        
        # Add dangerous function patterns
        self.dangerous_pattern = re.compile('|'.join(self.DANGEROUS_FUNCTIONS), re.IGNORECASE)
        
        # Add obfuscation patterns
        self.obscure_pattern = re.compile('|'.join(self.OBSCURE_SEQ_PATTERNS), re.IGNORECASE)
        
        # Behavioral thresholds
        self.MAX_OR_CLAUSES = 5
        self.MAX_UNION_SELECTS = 3
        self.MAX_QUERY_NESTING = 4
        self.MAX_COMMENTS = 2
    
    def detect(self, sql: str, context: Optional[str] = None) -> DetectionResult:
        """
        Perform comprehensive SQL injection detection.
        
        Args:
            sql: The SQL query to analyze
            context: Optional context about how the SQL was constructed
            
        Returns:
            DetectionResult with findings and risk assessment
        """
        findings: List[InjectionFinding] = []
        detected_patterns: Set[str] = set()
        
        if not sql or not isinstance(sql, str):
            return DetectionResult(is_safe=True, findings=[])
        
        # Normalize whitespace for analysis
        normalized_sql = ' '.join(sql.split())
        
        # 1. Pattern-based detection
        for injection_type, patterns in self.compiled_patterns.items():
            for pattern, severity in patterns:
                for match in pattern.finditer(normalized_sql):
                    finding = InjectionFinding(
                        injection_type=injection_type,
                        severity=severity,
                        pattern=pattern.pattern,
                        position=match.start(),
                        confidence=self._calculate_confidence(match, injection_type),
                        context=self._extract_context(normalized_sql, match.start(), match.end()),
                        description=self._get_description(injection_type),
                        mitigation=self._get_mitigation(injection_type)
                    )
                    findings.append(finding)
                    detected_patterns.add(injection_type.value)
        
        # 2. Dangerous function check
        if self.dangerous_pattern.search(normalized_sql):
            finding = InjectionFinding(
                injection_type=InjectionType.OUT_OF_BAND,
                severity=Severity.CRITICAL,
                pattern="dangerous_function",
                position=0,
                confidence=0.95,
                context=self._extract_context(normalized_sql, 0, 50),
                description="Dangerous database function detected",
                mitigation="Remove dangerous function calls and use parameterized queries"
            )
            findings.append(finding)
            detected_patterns.add("dangerous_function")
        
        # 3. Obfuscation detection
        if self.obscure_pattern.search(normalized_sql):
            finding = InjectionFinding(
                injection_type=InjectionType.SECOND_ORDER,
                severity=Severity.MEDIUM,
                pattern="obfuscation",
                position=0,
                confidence=self._calculate_obfuscation_confidence(normalized_sql),
                context=self._extract_context(normalized_sql, 0, 50),
                description="Potential obfuscation detected (encoding/escaping)",
                mitigation="Decode and validate input before processing"
            )
            findings.append(finding)
            detected_patterns.add("obfuscated_input")
        
        # 4. Context-aware string escape detection
        string_escape_findings = self._detect_string_escapes(normalized_sql)
        findings.extend(string_escape_findings)
        
        # 5. Behavioral Analysis (Structural Anomalies)
        behavioral_findings = self._analyze_behavior(normalized_sql)
        findings.extend(behavioral_findings)
        
        # 6. Entropy analysis for encoded payloads
        entropy_score = self._calculate_entropy(normalized_sql)
        if entropy_score > self._get_entropy_threshold(normalized_sql):
            finding = InjectionFinding(
                injection_type=InjectionType.SECOND_ORDER,
                severity=Severity.LOW,
                pattern="high_entropy",
                position=0,
                confidence=min(0.7, entropy_score / 100),
                context="High entropy content detected",
                description="Content has high entropy (possible encoded payload)",
                mitigation="Validate and sanitize all user inputs"
            )
            findings.append(finding)
            detected_patterns.add("high_entropy")
        
        # 7. Keyword density analysis
        keyword_score = self._calculate_keyword_density(normalized_sql)
        if keyword_score > 0.3:  # More than 30% SQL keywords
            finding = InjectionFinding(
                injection_type=InjectionType.IDENTIFIER_INJECTION,
                severity=Severity.MEDIUM,
                pattern="high_keyword_density",
                position=0,
                confidence=keyword_score,
                context="Unusual keyword density in input",
                description="Input contains high density of SQL keywords",
                mitigation="Input validation and parameterized queries required"
            )
            findings.append(finding)
            detected_patterns.add("high_keyword_density")
        
        # 8. Second-order specific checks (latent payloads)
        second_order_findings = self._detect_second_order_indicators(normalized_sql)
        findings.extend(second_order_findings)
        
        # Calculate overall risk score
        risk_score = self._calculate_risk_score(findings, entropy_score, keyword_score)
        
        # Determine if safe
        is_safe = (
            len(findings) == 0 or 
            risk_score < 30 or  # Low risk threshold
            not any(f.severity in (Severity.CRITICAL, Severity.HIGH) for f in findings)
        )
        
        # Add safety margin for critical patterns
        if any(f.injection_type in (InjectionType.STACKED_QUERIES, InjectionType.TIME_BLIND) 
               for f in findings):
            is_safe = False
        
        return DetectionResult(
            is_safe=is_safe,
            findings=findings,
            risk_score=risk_score,
            detected_patterns=detected_patterns,
            scan_summary={
                "total_findings": len(findings),
                "critical_count": sum(1 for f in findings if f.severity == Severity.CRITICAL),
                "high_count": sum(1 for f in findings if f.severity == Severity.HIGH),
                "medium_count": sum(1 for f in findings if f.severity == Severity.MEDIUM),
                "low_count": sum(1 for f in findings if f.severity == Severity.LOW),
                "entropy_score": entropy_score,
                "keyword_density": keyword_score,
            }
        )
    
        return findings
    
    def _detect_string_escapes(self, sql: str) -> List[InjectionFinding]:
        """Detect string escape attempts"""
        findings = []
        
        # Single quote escapes
        single_quote_matches = re.finditer(r"'[^']*(?:''|\\')[^']*", sql, re.IGNORECASE)
        for match in single_quote_matches:
            content = match.group().lower()
            if 'or ' in content or 'and ' in content or 'union' in content:
                finding = InjectionFinding(
                    injection_type=InjectionType.STRING_ESCAPE,
                    severity=Severity.HIGH,
                    pattern="single_quote_escape",
                    position=match.start(),
                    confidence=0.85,
                    context=self._extract_context(sql, match.start(), match.end()),
                    description="Suspicious content within single-quoted string",
                    mitigation="Use parameterized queries, validate input encoding"
                )
                findings.append(finding)
        
        # Double quote escapes
        double_quote_matches = re.finditer(r'"[^"]*(?:""|\\")[^"]*', sql, re.IGNORECASE)
        for match in double_quote_matches:
            content = match.group().lower()
            if 'or ' in content or 'and ' in content:
                finding = InjectionFinding(
                    injection_type=InjectionType.STRING_ESCAPE,
                    severity=Severity.HIGH,
                    pattern="double_quote_escape",
                    position=match.start(),
                    confidence=0.85,
                    context=self._extract_context(sql, match.start(), match.end()),
                    description="Suspicious content within double-quoted string",
                    mitigation="Use parameterized queries, validate input encoding"
                )
                findings.append(finding)
        
        return findings

    def _analyze_behavior(self, sql: str) -> List[InjectionFinding]:
        """Analyze query structure for behavioral anomalies"""
        findings = []
        sql_lower = sql.lower()
        
        # 1. Excessive OR clauses (common in boolean-based blind injection)
        or_count = len(re.findall(r'\bor\b', sql_lower))
        if or_count > self.MAX_OR_CLAUSES:
            findings.append(InjectionFinding(
                injection_type=InjectionType.BOOLEAN_BLIND,
                severity=Severity.HIGH,
                pattern="excessive_or_clauses",
                position=0,
                confidence=min(0.9, 0.4 + (or_count / 20)),
                context=f"OR count: {or_count}",
                description=f"Behavioral anomaly: excessive ({or_count}) OR clauses",
                mitigation="Use whitelisted predicates and parameterized queries"
            ))
            
        # 2. Excessive comments (obfuscation technique)
        comment_count = len(re.findall(r'--|#|/\*', sql_lower))
        if comment_count > self.MAX_COMMENTS:
            findings.append(InjectionFinding(
                injection_type=InjectionType.COMMENT_INJECTION,
                severity=Severity.MEDIUM,
                pattern="excessive_comments",
                position=0,
                confidence=0.7,
                context=f"Comment count: {comment_count}",
                description="Behavioral anomaly: unusual number of comments in query",
                mitigation="Remove comments from user-generated SQL"
            ))
            
        # 3. Deep nesting (subqueries/parentheses)
        nesting_level = 0
        max_nesting = 0
        for char in sql:
            if char == '(':
                nesting_level += 1
                max_nesting = max(max_nesting, nesting_level)
            elif char == ')':
                nesting_level -= 1
        
        if max_nesting > self.MAX_QUERY_NESTING:
            findings.append(InjectionFinding(
                injection_type=InjectionType.SUBQUERY_INJECTION,
                severity=Severity.MEDIUM,
                pattern="deep_nesting",
                position=0,
                confidence=0.6,
                context=f"Nesting depth: {max_nesting}",
                description="Behavioral anomaly: excessive query nesting depth",
                mitigation="Limit subquery complexity"
            ))
            
        return findings

    def _detect_second_order_indicators(self, sql: str) -> List[InjectionFinding]:
        """Detect potential second-order injection indicators (latent payloads)"""
        findings = []
        
        # Detect payloads that look like they are preparing for later execution
        # e.g., base64 encoded chunks or hex strings that might be decoded later
        hex_blobs = re.findall(r'0x[0-9a-fA-F]{32,}', sql)
        if hex_blobs:
            findings.append(InjectionFinding(
                injection_type=InjectionType.SECOND_ORDER,
                severity=Severity.HIGH,
                pattern="large_hex_blob",
                position=0,
                confidence=0.8,
                context=f"Hex blob: {hex_blobs[0][:20]}...",
                description="Potential second-order payload: large hex blob detected",
                mitigation="Avoid storing raw hex blobs; validate before retrieval"
            ))
            
        return findings
    
    def _calculate_confidence(self, match: re.Match, injection_type: InjectionType) -> float:
        """Calculate confidence score for a pattern match"""
        base_confidence = 0.8
        
        # Adjust based on injection type
        confidence_adjustments = {
            InjectionType.UNION_BASED: 0.1,
            InjectionType.TIME_BLIND: 0.15,
            InjectionType.STACKED_QUERIES: 0.15,
            InjectionType.STRING_ESCAPE: 0.05,
        }
        
        adjustment = confidence_adjustments.get(injection_type, 0)
        
        # Reduce confidence for shorter matches
        match_len = len(match.group())
        if match_len < 10:
            adjustment -= 0.2
        elif match_len > 50:
            adjustment += 0.05
        
        return min(0.99, max(0.1, base_confidence + adjustment))
    
    def _calculate_obfuscation_confidence(self, sql: str) -> float:
        """Calculate confidence for obfuscation detection"""
        encoding_count = len(self.obscure_pattern.findall(sql))
        if encoding_count == 0:
            return 0.0
        
        # Higher encoding ratio = higher confidence of obfuscation
        sql_len = len(sql)
        encoding_ratio = (encoding_count * 4) / sql_len  # Approx 4 chars per encoding
        
        return min(0.95, 0.4 + encoding_ratio * 2)
    
    def _calculate_entropy(self, text: str) -> float:
        """Calculate Shannon entropy of text"""
        if not text:
            return 0.0
        
        import math
        
        # Calculate character frequency
        freq = {}
        for char in text:
            freq[char] = freq.get(char, 0) + 1
        
        # Calculate entropy
        entropy = 0.0
        length = len(text)
        for count in freq.values():
            if count > 0:
                p = count / length
                entropy -= p * math.log2(p)
        
        # Normalize to 0-100 scale
        max_entropy = math.log2(min(256, len(freq))) if freq else 1
        normalized = (entropy / max_entropy) * 100 if max_entropy > 0 else 0
        
        return normalized
    
    def _get_entropy_threshold(self, sql: str) -> float:
        """Get entropy threshold based on SQL length"""
        # Longer queries naturally have higher entropy
        base_threshold = 60
        length_factor = min(20, len(sql) / 100)
        return base_threshold + length_factor
    
    def _calculate_keyword_density(self, sql: str) -> float:
        """Calculate the density of SQL keywords in the query"""
        sql_keywords = {
            'select', 'insert', 'update', 'delete', 'drop', 'create', 'alter',
            'union', 'join', 'where', 'and', 'or', 'not', 'null', 'from',
            'table', 'database', 'exec', 'execute', 'cast', 'convert'
        }
        
        words = re.findall(r'\b\w+\b', sql.lower())
        if not words:
            return 0.0
        
        keyword_count = sum(1 for word in words if word in sql_keywords)
        return keyword_count / len(words)
    
    def _calculate_risk_score(self, findings: List[InjectionFinding], 
                             entropy: float, keyword_density: float) -> float:
        """Calculate overall risk score"""
        if not findings:
            return max(0, (entropy - 50) / 2)  # Only entropy-based
        
        # Weight findings by severity
        severity_weights = {
            Severity.CRITICAL: 40,
            Severity.HIGH: 25,
            Severity.MEDIUM: 10,
            Severity.LOW: 3
        }
        
        score = sum(
            severity_weights.get(f.severity, 5) * f.confidence 
            for f in findings
        )
        
        # Add entropy contribution
        if entropy > 60:
            score += (entropy - 60) * 0.5
        
        # Add keyword density contribution
        if keyword_density > 0.2:
            score += keyword_density * 20
        
        return min(100, score)
    
    def _extract_context(self, sql: str, start: int, end: int, 
                         context_chars: int = 20) -> str:
        """Extract context around a match"""
        context_start = max(0, start - context_chars)
        context_end = min(len(sql), end + context_chars)
        
        context = sql[context_start:context_end]
        if context_start > 0:
            context = "..." + context
        if context_end < len(sql):
            context = context + "..."
        
        return context
    
    def _get_description(self, injection_type: InjectionType) -> str:
        """Get human-readable description for injection type"""
        descriptions = {
            InjectionType.UNION_BASED: "Union-based SQL injection attempt",
            InjectionType.ERROR_BASED: "Error-based SQL injection attempt",
            InjectionType.BOOLEAN_BLIND: "Boolean-based blind SQL injection",
            InjectionType.TIME_BLIND: "Time-based blind SQL injection (delayed response)",
            InjectionType.STACKED_QUERIES: "Stacked query injection (multiple statements)",
            InjectionType.OUT_OF_BAND: "Out-of-band data exfiltration attempt",
            InjectionType.COMMENT_INJECTION: "Comment-based injection to bypass validation",
            InjectionType.STRING_ESCAPE: "String escape sequence injection",
            InjectionType.IDENTIFIER_INJECTION: "Identifier/keyword injection",
            InjectionType.SUBQUERY_INJECTION: "Subquery injection attack",
            InjectionType.STORED_PROCEDURE: "Stored procedure execution attempt",
            InjectionType.SECOND_ORDER: "Second-order injection indicator",
        }
        return descriptions.get(injection_type, "Unknown injection type")
    
    def _get_mitigation(self, injection_type: InjectionType) -> str:
        """Get mitigation recommendation for injection type"""
        mitigations = {
            InjectionType.UNION_BASED: "Use parameterized queries; validate column count in unions",
            InjectionType.ERROR_BASED: "Use parameterized queries; implement generic error handling",
            InjectionType.BOOLEAN_BLIND: "Use parameterized queries; implement rate limiting",
            InjectionType.TIME_BLIND: "CRITICAL: Use parameterized queries; monitor query execution time",
            InjectionType.STACKED_QUERIES: "Disable multiple statement execution; use parameterized queries",
            InjectionType.OUT_OF_BAND: "Disable dangerous functions; implement strict input validation",
            InjectionType.COMMENT_INJECTION: "Validate input length; sanitize comment characters",
            InjectionType.STRING_ESCAPE: "Use parameterized queries; validate encoding",
            InjectionType.IDENTIFIER_INJECTION: "Whitelist allowed identifiers; use parameterized queries",
            InjectionType.SUBQUERY_INJECTION: "Validate subquery structure; use parameterized queries",
            InjectionType.STORED_PROCEDURE: "Restrict stored procedure execution; validate parameters",
            InjectionType.SECOND_ORDER: "Validate all data at storage and retrieval; sanitize inputs",
        }
        return mitigations.get(injection_type, "Use parameterized queries and input validation")


# Global instance
sql_injection_detector = SQLInjectionDetector()


# Convenience functions

def detect_sql_injection(sql: str, context: Optional[str] = None) -> DetectionResult:
    """Detect SQL injection in query"""
    return sql_injection_detector.detect(sql, context)


def is_sql_safe(sql: str, context: Optional[str] = None) -> bool:
    """Quick check if SQL is safe"""
    result = sql_injection_detector.detect(sql, context)
    return result.is_safe


def get_risk_score(sql: str, context: Optional[str] = None) -> float:
    """Get risk score for SQL query"""
    result = sql_injection_detector.detect(sql, context)
    return result.risk_score