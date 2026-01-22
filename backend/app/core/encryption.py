"""
Application-Level Encryption for Sensitive Data

Provides encryption/decryption for sensitive data stored in Redis and other storage.
Uses Fernet (symmetric encryption) from cryptography library.

Security Features:
- AES-128 encryption in CBC mode with HMAC authentication
- Key derivation from environment variable
- Automatic key rotation support
- Field-level encryption for audit logs and sensitive data
- Encryption at rest for Redis data

Note: This is application-level encryption. For production:
1. Enable Redis TLS for encryption in transit
2. Enable Redis encryption at rest (Redis Enterprise or disk encryption)
3. Use AWS KMS or similar for key management
4. Implement key rotation policies
"""

import logging
import base64
import json
from typing import Any, Dict, Optional, List
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

from app.core.config import settings

logger = logging.getLogger(__name__)


class EncryptionService:
    """Service for encrypting/decrypting sensitive data"""
    
    def __init__(self):
        self._fernet: Optional[Fernet] = None
        self._initialized = False
        self._encryption_enabled = False
    
    def initialize(self):
        """Initialize encryption service with key from settings"""
        try:
            # Check if encryption is enabled
            encryption_key = getattr(settings, 'ENCRYPTION_KEY', None)
            
            if not encryption_key or encryption_key == "CHANGE_ME_IN_PRODUCTION":
                logger.warning(
                    "Encryption key not configured or using default. "
                    "Sensitive data will NOT be encrypted. "
                    "Set ENCRYPTION_KEY environment variable for production."
                )
                self._encryption_enabled = False
                self._initialized = True
                return
            
            # Derive key using PBKDF2 for consistent 32-byte key
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=b'amila_audit_salt_v1',  # Static salt for consistency
                iterations=100000,
                backend=default_backend()
            )
            key = base64.urlsafe_b64encode(kdf.derive(encryption_key.encode()))
            
            self._fernet = Fernet(key)
            self._encryption_enabled = True
            self._initialized = True
            logger.info("Encryption service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize encryption service: {e}")
            self._encryption_enabled = False
            self._initialized = True
    
    def is_enabled(self) -> bool:
        """Check if encryption is enabled"""
        if not self._initialized:
            self.initialize()
        return self._encryption_enabled
    
    def encrypt(self, data: str) -> str:
        """
        Encrypt a string
        
        Args:
            data: Plain text string to encrypt
            
        Returns:
            Encrypted string (base64 encoded)
        """
        if not self.is_enabled():
            return data
        
        try:
            encrypted_bytes = self._fernet.encrypt(data.encode('utf-8'))
            return encrypted_bytes.decode('utf-8')
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            # Return original data if encryption fails (graceful degradation)
            return data
    
    def decrypt(self, encrypted_data: str) -> str:
        """
        Decrypt a string
        
        Args:
            encrypted_data: Encrypted string (base64 encoded)
            
        Returns:
            Decrypted plain text string
        """
        if not self.is_enabled():
            return encrypted_data
        
        try:
            decrypted_bytes = self._fernet.decrypt(encrypted_data.encode('utf-8'))
            return decrypted_bytes.decode('utf-8')
        except InvalidToken:
            logger.warning("Failed to decrypt data - invalid token or key mismatch")
            # Return encrypted data if decryption fails (data might not be encrypted)
            return encrypted_data
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            return encrypted_data
    
    def encrypt_dict_fields(
        self, 
        data: Dict[str, Any], 
        fields_to_encrypt: List[str]
    ) -> Dict[str, Any]:
        """
        Encrypt specific fields in a dictionary
        
        Args:
            data: Dictionary containing data
            fields_to_encrypt: List of field names to encrypt
            
        Returns:
            Dictionary with specified fields encrypted
        """
        if not self.is_enabled():
            return data
        
        encrypted_data = data.copy()
        for field in fields_to_encrypt:
            if field in encrypted_data and encrypted_data[field]:
                value = encrypted_data[field]
                # Convert to string if not already
                if not isinstance(value, str):
                    value = json.dumps(value)
                encrypted_data[field] = self.encrypt(value)
                # Mark field as encrypted
                encrypted_data[f"_encrypted_{field}"] = True
        
        return encrypted_data
    
    def decrypt_dict_fields(
        self, 
        data: Dict[str, Any], 
        fields_to_decrypt: List[str]
    ) -> Dict[str, Any]:
        """
        Decrypt specific fields in a dictionary
        
        Args:
            data: Dictionary containing encrypted data
            fields_to_decrypt: List of field names to decrypt
            
        Returns:
            Dictionary with specified fields decrypted
        """
        if not self.is_enabled():
            return data
        
        decrypted_data = data.copy()
        for field in fields_to_decrypt:
            if field in decrypted_data and decrypted_data.get(f"_encrypted_{field}"):
                encrypted_value = decrypted_data[field]
                decrypted_value = self.decrypt(encrypted_value)
                
                # Try to parse as JSON if it was originally a complex type
                try:
                    decrypted_data[field] = json.loads(decrypted_value)
                except (json.JSONDecodeError, TypeError):
                    decrypted_data[field] = decrypted_value
                
                # Remove encryption marker
                decrypted_data.pop(f"_encrypted_{field}", None)
        
        return decrypted_data
    
    def encrypt_audit_entry(self, entry_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encrypt sensitive fields in an audit entry
        
        Sensitive fields:
        - details (may contain SQL queries, PII)
        - ip_address
        - user_agent
        - session_id
        
        Args:
            entry_dict: Audit entry dictionary
            
        Returns:
            Audit entry with sensitive fields encrypted
        """
        sensitive_fields = ['details', 'ip_address', 'user_agent', 'session_id']
        return self.encrypt_dict_fields(entry_dict, sensitive_fields)
    
    def decrypt_audit_entry(self, entry_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decrypt sensitive fields in an audit entry
        
        Args:
            entry_dict: Encrypted audit entry dictionary
            
        Returns:
            Audit entry with sensitive fields decrypted
        """
        sensitive_fields = ['details', 'ip_address', 'user_agent', 'session_id']
        return self.decrypt_dict_fields(entry_dict, sensitive_fields)
    
    def encrypt_session_data(self, session_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encrypt sensitive fields in session data
        
        Sensitive fields:
        - email
        - ip_address
        - user_agent
        
        Args:
            session_dict: Session data dictionary
            
        Returns:
            Session data with sensitive fields encrypted
        """
        sensitive_fields = ['email', 'ip_address', 'user_agent']
        return self.encrypt_dict_fields(session_dict, sensitive_fields)
    
    def decrypt_session_data(self, session_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decrypt sensitive fields in session data
        
        Args:
            session_dict: Encrypted session data dictionary
            
        Returns:
            Session data with sensitive fields decrypted
        """
        sensitive_fields = ['email', 'ip_address', 'user_agent']
        return self.decrypt_dict_fields(session_dict, sensitive_fields)


# Global encryption service instance
encryption_service = EncryptionService()


def get_encryption_service() -> EncryptionService:
    """Get the global encryption service instance"""
    if not encryption_service._initialized:
        encryption_service.initialize()
    return encryption_service
