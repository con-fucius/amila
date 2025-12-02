#!/usr/bin/env python
"""Test different import paths for LangGraph checkpoint."""

print("Testing LangGraph checkpoint imports...\n")

# Test 1: langgraph.checkpoint.sqlite
try:
    from langgraph.checkpoint.sqlite import SqliteSaver
    print(" SUCCESS: from langgraph.checkpoint.sqlite import SqliteSaver")
    print(f"   Location: {SqliteSaver.__module__}")
except ImportError as e:
    print(f" FAILED: from langgraph.checkpoint.sqlite import SqliteSaver")
    print(f"   Error: {e}")

print()

# Test 2: langgraph_checkpoint_sqlite
try:
    from langgraph_checkpoint_sqlite import SqliteSaver as SqliteSaver2
    print(" SUCCESS: from langgraph_checkpoint_sqlite import SqliteSaver")
    print(f"   Location: {SqliteSaver2.__module__}")
except ImportError as e:
    print(f" FAILED: from langgraph_checkpoint_sqlite import SqliteSaver")
    print(f"   Error: {e}")

print()

# Test 3: Check what's available in langgraph
try:
    import langgraph
    print(f" langgraph package found at: {langgraph.__file__}")
    print(f"   Version: {getattr(langgraph, '__version__', 'unknown')}")
    
    # Check checkpoint submodule
    try:
        import langgraph.checkpoint
        print(f" langgraph.checkpoint found at: {langgraph.checkpoint.__file__}")
    except ImportError as e:
        print(f" langgraph.checkpoint not found: {e}")
        
except ImportError as e:
    print(f" langgraph not installed: {e}")

print()

# Test 4: List all checkpoint-related packages
import sys
import pkgutil

print("All installed checkpoint packages:")
for importer, modname, ispkg in pkgutil.iter_modules():
    if 'checkpoint' in modname.lower() or 'langgraph' in modname.lower():
        print(f"  - {modname} (package: {ispkg})")