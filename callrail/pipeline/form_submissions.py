from callrail.pipeline.interface import Pipeline
from db.bigquery import get_latest_timestamp

pipeline = Pipeline(
    "FormSubmissions",
    "form_submissions.json",
    lambda x: x["form_submissions"],
    lambda rows: [
        {
            "id": row.get("id"),
            "company_id": row.get("company_id"),
            "person_id": row.get("person_id"),
            "form_data": {
                "name": row["form_data"].get("name"),
                "email": row["form_data"].get("email"),
                "phone": row["form_data"].get("phone"),
                "contact_method": row["form_data"].get("contact_method"),
                "best_time_to_call": row["form_data"].get("best_time_to_call"),
            }
            if row.get("form_data")
            else {},
            "form_url": row.get("form_url"),
            "landing_page_url": row.get("landing_page_url"),
            "referrer": row.get("referrer"),
            "referring_url": row.get("referring_url"),
            "submitted_at": row.get("submitted_at"),
            "first_form": row.get("first_form"),
        }
        for row in rows
    ],
    [
        {"name": "id", "type": "STRING"},
        {"name": "company_id", "type": "STRING"},
        {"name": "person_id", "type": "STRING"},
        {
            "name": "form_data",
            "type": "record",
            "fields": [
                {"name": "name", "type": "STRING"},
                {"name": "email", "type": "STRING"},
                {"name": "phone", "type": "STRING"},
                {"name": "contact_method", "type": "STRING"},
                {"name": "best_time_to_call", "type": "STRING"},
            ],
        },
        {"name": "form_url", "type": "STRING"},
        {"name": "landing_page_url", "type": "STRING"},
        {"name": "referrer", "type": "STRING"},
        {"name": "referring_url", "type": "STRING"},
        {"name": "submitted_at", "type": "TIMESTAMP"},
        {"name": "first_form", "type": "BOOLEAN"},
    ],
    params_fn=get_latest_timestamp,
    cursor_key="submitted_at",
)
