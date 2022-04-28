from typing import Union, Optional, Any
from datetime import datetime

from compose import compose

from callrail.pipeline.interface import Pipeline
from callrail.callrail_repo import get
from db.bigquery import load, update


def _batched_at_service(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {**row, "_batched_at": datetime.utcnow().isoformat(timespec="seconds")}
        for row in rows
    ]


def _schema_cursorsify(schema: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return schema + [{"name": "_batched_at", "type": "TIMESTAMP"}]


def pipeline_service(
    pipeline: Pipeline,
    start: Optional[str],
    end: Optional[str],
) -> dict[str, Union[str, int]]:
    return compose(
        lambda x: {
            "table": pipeline.name,
            "start": start,
            "end": end,
            "output_rows": x,
        },
        load(
            pipeline.name,
            _schema_cursorsify(pipeline.schema),
            update(pipeline.id_key, pipeline.cursor_key),
        ),
        _batched_at_service,
        pipeline.transform,
        get(pipeline.uri, pipeline.res_fn),
        pipeline.params_fn(pipeline.name, pipeline.cursor_key),
    )((start, end))
