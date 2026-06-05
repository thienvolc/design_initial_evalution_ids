import json
import math
from datetime import date, datetime
from typing import Any, Mapping


def replay_record_to_json(row: Mapping[str, Any]) -> str:
    payload = {str(key): to_json_compatible(value) for key, value in row.items()}
    return json.dumps(payload, ensure_ascii=False)


def to_json_compatible(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    try:
        if value != value:
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if hasattr(value, "item"):
        item = value.item()
        if isinstance(item, float) and math.isnan(item):
            return None
        if isinstance(item, (datetime, date)):
            return item.isoformat()
        return item

    if hasattr(value, "as_py"):
        return to_json_compatible(value.as_py())

    if isinstance(value, dict):
        return {str(key): to_json_compatible(item) for key, item in value.items()}

    if isinstance(value, (list, tuple)):
        return [to_json_compatible(item) for item in value]

    return value
