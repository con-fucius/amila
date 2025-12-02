"""
Global Client Registry
Manages global instances of clients to avoid circular imports
"""

from typing import Optional, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from app.core.mcp_client import UnifiedMCPClient
    from app.core.graphiti_client import GraphitiClient
    from app.core.sqlcl_pool import SQLclProcessPool
    from app.core.doris_client import DorisMCPClient

class ClientRegistry:
    """Global registry for client instances"""
    
    def __init__(self):
        self._mcp_client: Optional["UnifiedMCPClient"] = None
        self._graphiti_client: Optional["GraphitiClient"] = None
        self._sqlcl_pool: Optional["SQLclProcessPool"] = None
        self._doris_client: Optional["DorisMCPClient"] = None
        self._query_orchestrator = None
        self._langgraph_checkpointer: Any = None
        self._langgraph_checkpointer_context: Any = None
    
    def set_mcp_client(self, client: "UnifiedMCPClient"):
        """Set the global MCP client instance"""
        self._mcp_client = client
    
    def get_mcp_client(self) -> Optional["UnifiedMCPClient"]:
        """Get the global MCP client instance"""
        return self._mcp_client
    
    def set_graphiti_client(self, client: "GraphitiClient"):
        """Set the global Graphiti client instance"""
        self._graphiti_client = client
    
    def get_graphiti_client(self) -> Optional["GraphitiClient"]:
        """Get the global Graphiti client instance"""
        return self._graphiti_client
    
    @property
    def mcp_client(self) -> Optional["UnifiedMCPClient"]:
        """Property accessor for MCP client"""
        return self._mcp_client
    
    @property
    def graphiti_client(self) -> Optional["GraphitiClient"]:
        """Property accessor for Graphiti client"""
        return self._graphiti_client
    
    def set_sqlcl_pool(self, pool: "SQLclProcessPool"):
        """Set the global SQLcl process pool instance"""
        self._sqlcl_pool = pool
    
    def get_sqlcl_pool(self) -> Optional["SQLclProcessPool"]:
        """Get the global SQLcl process pool instance"""
        return self._sqlcl_pool
    
    @property
    def sqlcl_pool(self) -> Optional["SQLclProcessPool"]:
        """Property accessor for SQLcl pool"""
        return self._sqlcl_pool
    
    def set_doris_client(self, client: "DorisMCPClient"):
        """Set the global Doris MCP client instance"""
        self._doris_client = client
    
    def get_doris_client(self) -> Optional["DorisMCPClient"]:
        """Get the global Doris MCP client instance"""
        return self._doris_client
    
    @property
    def doris_client(self) -> Optional["DorisMCPClient"]:
        """Property accessor for Doris client"""
        return self._doris_client
    
    def set_query_orchestrator(self, orchestrator):
        """Set the global query orchestrator graph instance"""
        self._query_orchestrator = orchestrator
    
    def get_query_orchestrator(self):
        """Get the global query orchestrator graph instance"""
        return self._query_orchestrator
    
    def set_langgraph_checkpointer(self, checkpointer: Any, context: Any | None = None):
        """Store LangGraph checkpointer and its context for reuse."""
        self._langgraph_checkpointer = checkpointer
        self._langgraph_checkpointer_context = context

    def get_langgraph_checkpointer(self) -> Any:
        """Return the LangGraph checkpointer instance if available."""
        return self._langgraph_checkpointer

    def get_langgraph_checkpointer_context(self) -> Any:
        """Return the LangGraph checkpointer context manager if available."""
        return self._langgraph_checkpointer_context

    def clear_langgraph_checkpointer(self) -> None:
        """Clear LangGraph checkpointer references."""
        self._langgraph_checkpointer = None
        self._langgraph_checkpointer_context = None

# Global registry instance
registry = ClientRegistry()

def get_mcp_client() -> Optional["UnifiedMCPClient"]:
    """Convenience function to get MCP client"""
    return registry.get_mcp_client()

def get_graphiti_client_from_registry() -> Optional["GraphitiClient"]:
    """Convenience function to get Graphiti client"""
    return registry.get_graphiti_client()

def get_sqlcl_pool() -> Optional["SQLclProcessPool"]:
    """Convenience function to get SQLcl process pool"""
    return registry.get_sqlcl_pool()

def get_doris_client() -> Optional["DorisMCPClient"]:
    """Convenience function to get Doris MCP client"""
    return registry.get_doris_client()