"""
Secure Session ID Generator
Generates cryptographically secure session IDs on the backend
Addresses Issue 5: session_id generated client-side can be spoofed
"""

import secrets
import hashlib
import time
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class SessionIDGenerator:
    """
    Secure session ID generator using cryptographic randomness
    
    Session IDs are generated server-side to prevent spoofing and ensure
    proper entropy for security.
    """
    
    # Session ID format: sess_{timestamp}_{random_token}_{checksum}
    PREFIX = "sess"
    TOKEN_LENGTH = 32  # 32 bytes = 256 bits of entropy
    
    @staticmethod
    def generate(user_id: Optional[str] = None, additional_context: Optional[str] = None) -> str:
        """
        Generate a cryptographically secure session ID
        
        Args:
            user_id: Optional user ID to include in checksum
            additional_context: Optional additional context (e.g., IP address)
        
        Returns:
            Secure session ID string
        """
        # Timestamp component (milliseconds)
        timestamp = int(time.time() * 1000)
        
        # Cryptographically secure random token
        random_token = secrets.token_urlsafe(SessionIDGenerator.TOKEN_LENGTH)
        
        # Generate checksum for integrity verification
        checksum_input = f"{timestamp}:{random_token}"
        if user_id:
            checksum_input += f":{user_id}"
        if additional_context:
            checksum_input += f":{additional_context}"
        
        checksum = hashlib.sha256(checksum_input.encode()).hexdigest()[:8]
        
        # Format: sess_{timestamp}_{random_token}_{checksum}
        session_id = f"{SessionIDGenerator.PREFIX}_{timestamp}_{random_token}_{checksum}"
        
        logger.debug(f"Generated session ID: {session_id[:20]}... (length: {len(session_id)})")
        
        return session_id
    
    @staticmethod
    def validate_format(session_id: str) -> bool:
        """
        Validate session ID format
        
        Args:
            session_id: Session ID to validate
        
        Returns:
            True if format is valid, False otherwise
        """
        if not session_id:
            return False
        
        parts = session_id.split('_')
        
        # Must have 4 parts: prefix, timestamp, token, checksum
        if len(parts) != 4:
            return False
        
        prefix, timestamp_str, token, checksum = parts
        
        # Validate prefix
        if prefix != SessionIDGenerator.PREFIX:
            return False
        
        # Validate timestamp is numeric
        try:
            timestamp = int(timestamp_str)
            # Timestamp should be reasonable (after 2020, before 2100)
            if timestamp < 1577836800000 or timestamp > 4102444800000:
                return False
        except ValueError:
            return False
        
        # Validate token length (URL-safe base64 encoding adds some characters)
        if len(token) < 40:  # Minimum expected length
            return False
        
        # Validate checksum length
        if len(checksum) != 8:
            return False
        
        return True
    
    @staticmethod
    def is_client_generated(session_id: str) -> bool:
        """
        Detect if session ID was generated client-side (old format)
        
        Args:
            session_id: Session ID to check
        
        Returns:
            True if client-generated, False if server-generated
        """
        if not session_id:
            return False
        
        # Old client format: session_{timestamp}_{random}
        # New server format: sess_{timestamp}_{token}_{checksum}
        
        parts = session_id.split('_')
        
        # Client format has 3 parts and starts with "session"
        if len(parts) == 3 and parts[0] == "session":
            return True
        
        # Server format has 4 parts and starts with "sess"
        if len(parts) == 4 and parts[0] == "sess":
            return False
        
        # Unknown format - treat as client-generated for safety
        return True
    
    @staticmethod
    def extract_timestamp(session_id: str) -> Optional[int]:
        """
        Extract timestamp from session ID
        
        Args:
            session_id: Session ID
        
        Returns:
            Timestamp in milliseconds, or None if invalid
        """
        if not SessionIDGenerator.validate_format(session_id):
            return None
        
        parts = session_id.split('_')
        try:
            return int(parts[1])
        except (ValueError, IndexError):
            return None
    
    @staticmethod
    def get_age_seconds(session_id: str) -> Optional[float]:
        """
        Get age of session in seconds
        
        Args:
            session_id: Session ID
        
        Returns:
            Age in seconds, or None if invalid
        """
        timestamp = SessionIDGenerator.extract_timestamp(session_id)
        if timestamp is None:
            return None
        
        current_time_ms = int(time.time() * 1000)
        age_ms = current_time_ms - timestamp
        
        return age_ms / 1000.0


# Global instance
session_id_generator = SessionIDGenerator()
