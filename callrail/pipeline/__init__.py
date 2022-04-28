from callrail.pipeline import calls, form_submissions, trackers

pipelines = {
    i.name: i
    for i in [
        j.pipeline
        for j in [
            calls,
            # form_submissions,
            trackers,
        ]
    ]
}
