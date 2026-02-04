"""
Audit Trail Immutability Verification Service

Ensures audit trail entries are immutable and tamper-evident.
Provides verification mechanisms for compliance (GDPR, HIPAA, SOX).

Features:
- Cryptographic verification of audit entries
- Tamper detection for audit logs
- Immutable storage verification
- Compliance reporting
- Audit trail integrity checks
"""

import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

from app.core.config import settings

logger = logging.getLogger(__name__)


class VerificationStatus(Enum):
    """Audit entry verification status"""
    VERIFIED = "verified"
    TAMPERED = "tampered"
    MISSING = "missing"
    CORRUPTED = "corrupted"
    PENDING = "pending"


@dataclass
class AuditEntry:
    """Represents a single audit trail entry"""
    entry_id: str
    timestamp: str
    event_type: str
    user_id: str
    session_id: str
    correlation_id: str
    action: str
    resource: str
    status: str
    details: Dict[str, Any]
    hash: Optional[str] = None
    previous_hash: Optional[str] = None
    signature: Optional[str] = None


@dataclass
class VerificationResult:
    """Result of audit entry verification"""
    entry_id: str
    status: VerificationStatus
    timestamp: str
    computed_hash: Optional[str] = None
    stored_hash: Optional[str] = None
    error_message: Optional[str] = None
    chain_valid: bool = False


class AuditImmutabilityService:
    """
    Service for verifying audit trail immutability.
    
    Implements blockchain-inspired verification where each entry
    contains a hash of its content and the previous entry's hash,
    creating a tamper-evident chain.
    """
    
    def __init__(self):
        self._secret_key = getattr(settings, 'AUDIT_SECRET_KEY', 'default-secret-key-change-in-production')
        self._algorithm = 'sha256'
    
    def _compute_entry_hash(self, entry: AuditEntry) -> str:
        """
        Compute cryptographic hash of audit entry.
        
        Creates a deterministic hash of the entry's content,
        excluding the hash and signature fields themselves.
        """
        # Create a copy of entry data excluding hash/signature
        entry_data = {
            "entry_id": entry.entry_id,
            "timestamp": entry.timestamp,
            "event_type": entry.event_type,
            "user_id": entry.user_id,
            "session_id": entry.session_id,
            "correlation_id": entry.correlation_id,
            "action": entry.action,
            "resource": entry.resource,
            "status": entry.status,
            "details": entry.details,
            "previous_hash": entry.previous_hash
        }
        
        # Canonical JSON representation (sorted keys for consistency)
        json_data = json.dumps(entry_data, sort_keys=True, separators=(',', ':'))
        
        # Compute HMAC using secret key (prevents hash recomputation attacks)
        hash_value = hmac.new(
            self._secret_key.encode('utf-8'),
            json_data.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return hash_value
    
    def sign_entry(self, entry: AuditEntry) -> AuditEntry:
        """
        Sign an audit entry with its computed hash.
        
        Should be called when creating a new audit entry.
        """
        entry.hash = self._compute_entry_hash(entry)
        
        # Create a signature that includes the hash
        signature_data = f"{entry.entry_id}:{entry.hash}:{entry.timestamp}"
        entry.signature = hmac.new(
            self._secret_key.encode('utf-8'),
            signature_data.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return entry
    
    def verify_entry(self, entry: AuditEntry) -> VerificationResult:
        """
        Verify the integrity of a single audit entry.
        
        Checks:
        1. Hash integrity (content hasn't changed)
        2. Signature validity (entry is authentic)
        """
        try:
            # Check required fields
            if not entry.hash:
                return VerificationResult(
                    entry_id=entry.entry_id,
                    status=VerificationStatus.CORRUPTED,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    error_message="Entry missing hash field"
                )
            
            # Recompute hash and compare
            computed_hash = self._compute_entry_hash(entry)
            if computed_hash != entry.hash:
                return VerificationResult(
                    entry_id=entry.entry_id,
                    status=VerificationStatus.TAMPERED,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    computed_hash=computed_hash,
                    stored_hash=entry.hash,
                    error_message="Hash mismatch - entry may have been tampered with"
                )
            
            # Verify signature if present
            if entry.signature:
                signature_data = f"{entry.entry_id}:{entry.hash}:{entry.timestamp}"
                expected_signature = hmac.new(
                    self._secret_key.encode('utf-8'),
                    signature_data.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                
                if expected_signature != entry.signature:
                    return VerificationResult(
                        entry_id=entry.entry_id,
                        status=VerificationStatus.TAMPERED,
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        error_message="Signature mismatch - entry may be forged"
                    )
            
            return VerificationResult(
                entry_id=entry.entry_id,
                status=VerificationStatus.VERIFIED,
                timestamp=datetime.now(timezone.utc).isoformat(),
                computed_hash=computed_hash,
                stored_hash=entry.hash,
                chain_valid=True
            )
            
        except Exception as e:
            logger.error(f"Error verifying entry {entry.entry_id}: {e}")
            return VerificationResult(
                entry_id=entry.entry_id,
                status=VerificationStatus.CORRUPTED,
                timestamp=datetime.now(timezone.utc).isoformat(),
                error_message=f"Verification error: {str(e)}"
            )
    
    def verify_entry_chain(
        self,
        entries: List[AuditEntry]
    ) -> Tuple[bool, List[VerificationResult]]:
        """
        Verify a chain of audit entries.
        
        Each entry's previous_hash should match the previous entry's hash,
        creating a tamper-evident chain.
        
        Returns:
            Tuple of (all_valid, list_of_results)
        """
        results = []
        all_valid = True
        previous_hash = None
        
        for i, entry in enumerate(entries):
            # Verify individual entry
            result = self.verify_entry(entry)
            
            # Check chain integrity (except for first entry)
            if i > 0 and previous_hash:
                if entry.previous_hash != previous_hash:
                    result.chain_valid = False
                    result.status = VerificationStatus.TAMPERED
                    result.error_message = (
                        f"Chain broken: previous_hash ({entry.previous_hash}) "
                        f"doesn't match previous entry's hash ({previous_hash})"
                    )
                    all_valid = False
                else:
                    result.chain_valid = True
            elif i == 0:
                # First entry should have no previous_hash or genesis marker
                result.chain_valid = True
            
            results.append(result)
            previous_hash = entry.hash
            
            if result.status != VerificationStatus.VERIFIED:
                all_valid = False
        
        return all_valid, results
    
    def create_genesis_entry(
        self,
        system_id: str,
        description: str = "Audit trail genesis"
    ) -> AuditEntry:
        """
        Create the first entry in an audit chain.
        
        Genesis entries have no previous_hash and mark the start
        of an immutable audit trail.
        """
        entry = AuditEntry(
            entry_id=f"genesis-{system_id}-{datetime.now(timezone.utc).timestamp()}",
            timestamp=datetime.now(timezone.utc).isoformat(),
            event_type="GENESIS",
            user_id="system",
            session_id="genesis",
            correlation_id="genesis",
            action="AUDIT_TRAIL_INIT",
            resource="audit_system",
            status="success",
            details={
                "description": description,
                "system_id": system_id,
                "version": "1.0"
            },
            previous_hash=None  # Genesis has no previous
        )
        
        return self.sign_entry(entry)
    
    def create_entry(
        self,
        event_type: str,
        user_id: str,
        session_id: str,
        correlation_id: str,
        action: str,
        resource: str,
        status: str,
        details: Dict[str, Any],
        previous_entry: Optional[AuditEntry] = None
    ) -> AuditEntry:
        """
        Create a new signed audit entry.
        
        Automatically links to previous entry for chain integrity.
        """
        entry = AuditEntry(
            entry_id=f"{event_type}-{correlation_id}-{datetime.now(timezone.utc).timestamp()}",
            timestamp=datetime.now(timezone.utc).isoformat(),
            event_type=event_type,
            user_id=user_id,
            session_id=session_id,
            correlation_id=correlation_id,
            action=action,
            resource=resource,
            status=status,
            details=details,
            previous_hash=previous_entry.hash if previous_entry else None
        )
        
        return self.sign_entry(entry)
    
    async def verify_audit_trail_integrity(
        self,
        entries: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Comprehensive audit trail integrity verification.
        
        Args:
            entries: List of audit entry dictionaries from storage
            
        Returns:
            Verification report with summary and details
        """
        # Convert dict entries to AuditEntry objects
        audit_entries = []
        for entry_data in entries:
            try:
                entry = AuditEntry(
                    entry_id=entry_data.get("entry_id", ""),
                    timestamp=entry_data.get("timestamp", ""),
                    event_type=entry_data.get("event_type", ""),
                    user_id=entry_data.get("user_id", ""),
                    session_id=entry_data.get("session_id", ""),
                    correlation_id=entry_data.get("correlation_id", ""),
                    action=entry_data.get("action", ""),
                    resource=entry_data.get("resource", ""),
                    status=entry_data.get("status", ""),
                    details=entry_data.get("details", {}),
                    hash=entry_data.get("hash"),
                    previous_hash=entry_data.get("previous_hash"),
                    signature=entry_data.get("signature")
                )
                audit_entries.append(entry)
            except Exception as e:
                logger.error(f"Failed to parse audit entry: {e}")
        
        # Verify the chain
        all_valid, results = self.verify_entry_chain(audit_entries)
        
        # Generate report
        verified_count = sum(1 for r in results if r.status == VerificationStatus.VERIFIED)
        tampered_count = sum(1 for r in results if r.status == VerificationStatus.TAMPERED)
        corrupted_count = sum(1 for r in results if r.status == VerificationStatus.CORRUPTED)
        
        report = {
            "verification_timestamp": datetime.now(timezone.utc).isoformat(),
            "total_entries": len(entries),
            "verified_count": verified_count,
            "tampered_count": tampered_count,
            "corrupted_count": corrupted_count,
            "integrity_valid": all_valid,
            "status": "healthy" if all_valid else "compromised",
            "details": [
                {
                    "entry_id": r.entry_id,
                    "status": r.status.value,
                    "error": r.error_message,
                    "chain_valid": r.chain_valid
                }
                for r in results
            ]
        }
        
        if not all_valid:
            logger.error(f"Audit trail integrity compromised: {tampered_count} tampered, {corrupted_count} corrupted")
        
        return report


# Global instance
audit_immutability_service = AuditImmutabilityService()


# Convenience functions

async def verify_audit_entry(entry_data: Dict[str, Any]) -> VerificationResult:
    """Verify a single audit entry"""
    entry = AuditEntry(
        entry_id=entry_data.get("entry_id", ""),
        timestamp=entry_data.get("timestamp", ""),
        event_type=entry_data.get("event_type", ""),
        user_id=entry_data.get("user_id", ""),
        session_id=entry_data.get("session_id", ""),
        correlation_id=entry_data.get("correlation_id", ""),
        action=entry_data.get("action", ""),
        resource=entry_data.get("resource", ""),
        status=entry_data.get("status", ""),
        details=entry_data.get("details", {}),
        hash=entry_data.get("hash"),
        previous_hash=entry_data.get("previous_hash"),
        signature=entry_data.get("signature")
    )
    return audit_immutability_service.verify_entry(entry)


async def verify_audit_chain(entries: List[Dict[str, Any]]) -> Tuple[bool, List[VerificationResult]]:
    """Verify a chain of audit entries"""
    audit_entries = []
    for entry_data in entries:
        entry = AuditEntry(
            entry_id=entry_data.get("entry_id", ""),
            timestamp=entry_data.get("timestamp", ""),
            event_type=entry_data.get("event_type", ""),
            user_id=entry_data.get("user_id", ""),
            session_id=entry_data.get("session_id", ""),
            correlation_id=entry_data.get("correlation_id", ""),
            action=entry_data.get("action", ""),
            resource=entry_data.get("resource", ""),
            status=entry_data.get("status", ""),
            details=entry_data.get("details", {}),
            hash=entry_data.get("hash"),
            previous_hash=entry_data.get("previous_hash"),
            signature=entry_data.get("signature")
        )
        audit_entries.append(entry)
    
    return audit_immutability_service.verify_entry_chain(audit_entries)
