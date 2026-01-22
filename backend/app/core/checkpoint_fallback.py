"""
LangGraph Checkpoint Fallback Handler

Provides graceful degradation when SQLite checkpointer is unavailable.
Uses in-memory checkpointing with optional persistence to JSON files.

Features:
- In-memory checkpoint storage
- Optional JSON file persistence
- Automatic cleanup of old checkpoints
- Thread-safe operations
- Compatible with LangGraph checkpoint interface
"""

import logging
import json
import asyncio
from typing import Any, Optional, Dict, List, Tuple
from datetime import datetime, timezone
from 