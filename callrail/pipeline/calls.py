from callrail.pipeline.interface import Pipeline
from db.bigquery import get_latest_timestamp

pipeline = Pipeline(
    "Calls",
    "calls.json",
    lambda x: x["calls"],
    lambda rows: [
        {
            "answered": row.get("answered"),
            "business_phone_number": row.get("business_phone_number"),
            "customer_city": row.get("customer_city"),
            "customer_country": row.get("customer_country"),
            "customer_name": row.get("customer_name"),
            "customer_phone_number": row.get("customer_phone_number"),
            "customer_state": row.get("customer_state"),
            "direction": row.get("direction"),
            "duration": row.get("duration"),
            "id": row.get("id"),
            "recording": row.get("recording"),
            "recording_duration": row.get("recording_duration"),
            "recording_player": row.get("recording_player"),
            "start_time": row.get("start_time"),
            "tracking_phone_number": row.get("tracking_phone_number"),
            "voicemail": row.get("voicemail"),
            "agent_email": row.get("agent_email"),
        }
        for row in rows
    ],
    [
        {"name": "answered", "type": "BOOLEAN"},
        {"name": "business_phone_number", "type": "STRING"},
        {"name": "customer_city", "type": "STRING"},
        {"name": "customer_country", "type": "STRING"},
        {"name": "customer_name", "type": "STRING"},
        {"name": "customer_phone_number", "type": "STRING"},
        {"name": "customer_state", "type": "STRING"},
        {"name": "direction", "type": "STRING"},
        {"name": "duration", "type": "INTEGER"},
        {"name": "id", "type": "STRING"},
        {"name": "recording", "type": "STRING"},
        {"name": "recording_duration", "type": "STRING"},
        {"name": "recording_player", "type": "STRING"},
        {"name": "start_time", "type": "TIMESTAMP"},
        {"name": "tracking_phone_number", "type": "STRING"},
        {"name": "voicemail", "type": "BOOLEAN"},
        {"name": "agent_email", "type": "STRING"},
    ],
    params_fn=get_latest_timestamp,
    cursor_key="start_time",
)
