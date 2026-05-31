import json
from datetime import datetime
from typing import Any

import pandas as pd


def replay_record_to_json(row: pd.Series) -> str:
    payload = {
        str(key): to_json_compatible(value)
        for key, value in row.items()
    }
    return json.dumps(payload, ensure_ascii=False)


def to_json_compatible(value: Any) -> Any:
    if pd.isna(value):
        return None

    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).isoformat()

    if hasattr(value, "item"):
        return value.item()

    return value
