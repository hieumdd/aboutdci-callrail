from callrail.pipeline import calls

pipelines = {
    i.name: i
    for i in [
        j.pipeline
        for j in [
            calls,
        ]
    ]
}
