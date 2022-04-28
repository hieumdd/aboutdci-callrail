from callrail.pipeline.interface import Pipeline

pipeline = Pipeline(
    "Trackers",
    "trackers.json",
    lambda x: x["trackers"],
    lambda rows: [
        {
            "id": row.get("id"),
            "name": row.get("name"),
            "type": row.get("type"),
            "status": row.get("status"),
            "destination_number": row.get("destination_number"),
            "tracking_numbers": [i for i in row["tracking_numbers"]]
            if row.get("tracking_numbers")
            else [],
            "whisper_message": row.get("whisper_message"),
            "sms_supported": row.get("sms_supported"),
            "sms_enabled": row.get("sms_enabled"),
            "company": {
                "id": row["company"].get("id"),
                "name": row["company"].get("name"),
            }
            if row.get("company")
            else {},
            "source": {
                "type": row["source"].get("type"),
            }
            if row.get("source")
            else {},
            "call_flow": {
                "type": row["call_flow"].get("type"),
                "recording_enabled": row["call_flow"].get("recording_enabled"),
                "destination_number": row["call_flow"].get("destination_number"),
                "greeting_text": row["call_flow"].get("greeting_text"),
                "greeting_recording_url": row["call_flow"].get(
                    "greeting_recording_url"
                ),
            }
            if row.get("call_flow")
            else {},
            "created_at": row.get("created_at"),
            "disabled_at": row.get("disabled_at"),
        }
        for row in rows
    ],
    [
        {"name": "id", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "type", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "destination_number", "type": "STRING"},
        {"name": "tracking_numbers", "type": "STRING", "mode": "REPEATED"},
        {"name": "whisper_message", "type": "STRING"},
        {"name": "sms_supported", "type": "BOOLEAN"},
        {"name": "sms_enabled", "type": "BOOLEAN"},
        {
            "name": "company",
            "type": "RECORD",
            "fields": [
                {"name": "id", "type": "STRING"},
                {"name": "name", "type": "STRING"},
            ],
        },
        {
            "name": "source",
            "type": "RECORD",
            "fields": [{"name": "type", "type": "STRING"}],
        },
        {
            "name": "call_flow",
            "type": "RECORD",
            "fields": [
                {"name": "type", "type": "STRING"},
                {"name": "recording_enabled", "type": "BOOLEAN"},
                {"name": "destination_number", "type": "STRING"},
                {"name": "greeting_text", "type": "STRING"},
                {"name": "greeting_recording_url", "type": "STRING"},
            ],
        },
        {"name": "created_at", "type": "TIMESTAMP"},
        {"name": "disabled_at", "type": "TIMESTAMP"},
    ],
)
