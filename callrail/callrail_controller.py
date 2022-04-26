from callrail.pipeline import pipelines
from callrail.callrail_service import pipeline_service


def callrail_controller(body: dict[str, str]):
    return pipeline_service(
        pipelines[body.get("table", "")],
        body.get("start"),
        body.get("end"),
    )
