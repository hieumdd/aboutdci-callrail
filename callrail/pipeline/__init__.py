from callrail.pipeline import calls, form_submissions

pipelines = {
    i.name: i
    for i in [
        j.pipeline
        for j in [
            calls,
            # form_submissions,
        ]
    ]
}
