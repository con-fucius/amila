#!/usr/bin/env python
"""Test backend application startup"""

print("Testing backend application startup...")

try:
    from app.application import create_application
    print(" Application module imported")
    
    app = create_application()
    print(" Application created successfully")
    
    # Check some registered routes
    api_routes = [r.path for r in app.routes if hasattr(r, 'path') and '/api/' in r.path]
    if api_routes:
        print(f" API routes registered: {len(api_routes)} routes found")
        print(f"  Sample routes: {api_routes[:5]}")
    else:
        print(" No API routes found in application")
    
    print("\n Backend startup test PASSED")
    
except Exception as e:
    print(f"\n Backend startup test FAILED: {e}")
    import traceback
    traceback.print_exc()
    exit(1)