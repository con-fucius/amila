"""
Langfuse Connection Verification Script

Tests Langfuse configuration and connectivity.
Run this script to verify Langfuse is properly configured.

Usage:
    python scripts/verify_langfuse.py
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.config import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def verify_langfuse():
    """Verify Langfuse configuration and connectivity"""
    
    print("=" * 60)
    print("Langfuse Configuration Verification")
    print("=" * 60)
    
    # Check if enabled
    print(f"\n1. Langfuse Enabled: {settings.LANGFUSE_ENABLED}")
    if not settings.LANGFUSE_ENABLED:
        print("   ❌ Langfuse is disabled. Set LANGFUSE_ENABLED=true in .env")
        return False
    
    # Check credentials
    print(f"\n2. Credentials:")
    print(f"   Public Key: {settings.LANGFUSE_PUBLIC_KEY[:20]}..." if settings.LANGFUSE_PUBLIC_KEY else "   ❌ Not configured")
    print(f"   Secret Key: {settings.LANGFUSE_SECRET_KEY[:20]}..." if settings.LANGFUSE_SECRET_KEY else "   ❌ Not configured")
    print(f"   Host: {settings.LANGFUSE_HOST}")
    
    if not settings.LANGFUSE_PUBLIC_KEY or not settings.LANGFUSE_SECRET_KEY:
        print("   ❌ Credentials not configured. Set LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY in .env")
        return False
    
    # Check package installation
    print(f"\n3. Package Installation:")
    try:
        import langfuse
        print(f"   ✅ langfuse package installed (version: {langfuse.__version__})")
    except ImportError:
        print("   ❌ langfuse package not installed. Run: pip install langfuse")
        return False
    
    # Test connection
    print(f"\n4. Connection Test:")
    try:
        from app.core.langfuse_client import get_langfuse_client
        
        client = get_langfuse_client()
        if not client:
            print("   ❌ Failed to initialize Langfuse client")
            return False
        
        print("   ✅ Langfuse client initialized successfully")
        
        # Test trace creation
        print(f"\n5. Trace Creation Test:")
        test_trace_id = "verify_test_trace"
        
        client.trace(
            id=test_trace_id,
            name="verification_test",
            user_id="system",
            input={"test": "verification"},
            metadata={"purpose": "connection_test"}
        )
        
        print(f"   ✅ Test trace created: {test_trace_id}")
        
        # Test span creation
        print(f"\n6. Span Creation Test:")
        span = client.span(
            name="test_span",
            trace_id=test_trace_id,
            input={"operation": "test"},
            metadata={"test": True}
        )
        
        span.end()
        print(f"   ✅ Test span created and completed")
        
        # Test generation logging
        print(f"\n7. Generation Logging Test:")
        client.generation(
            name="test_generation",
            trace_id=test_trace_id,
            model="test-model",
            input={"prompt": "test"},
            output={"response": "test"},
            usage={"input_tokens": 10, "output_tokens": 20}
        )
        
        print(f"   ✅ Test generation logged")
        
        # Flush events
        print(f"\n8. Flushing Events:")
        client.flush()
        print(f"   ✅ Events flushed to Langfuse")
        
        print(f"\n" + "=" * 60)
        print("✅ All Langfuse verification checks passed!")
        print("=" * 60)
        print(f"\nView your test trace at:")
        print(f"{settings.LANGFUSE_HOST}/project/default/traces/{test_trace_id}")
        print()
        
        return True
        
    except Exception as e:
        print(f"   ❌ Connection test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = verify_langfuse()
    sys.exit(0 if success else 1)
