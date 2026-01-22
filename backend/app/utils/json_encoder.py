from datetime import datetime, date
from decimal import Decimal
import json
from typing import Any


class CustomJSONEncoder(json.JSONEncoder):
    """
    Custom JSON Encoder that handles:
    - Decimal objects (converts to float or str)
    - Datetime and Date objects (converts to ISO format)
    - Sets (converts to list)
    """
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            # Convert decimal to float or string depending on precision needs
            # For general results, float is usually fine, but str is safer for exactness
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        if hasattr(obj, "dict"): # Pydantic v1
            return obj.dict()
        if hasattr(obj, "model_dump"): # Pydantic v2
            return obj.model_dump()
            
        return super().default(obj)


def json_dumps(obj: Any, **kwargs) -> str:
    """Helper function to dump JSON with CustomJSONEncoder"""
    kwargs.setdefault('cls', CustomJSONEncoder)
    return json.dumps(obj, **kwargs)
