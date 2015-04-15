
def receive_before_flush(session, flush_context, instances):
    for target in session.dirty:
        existing = target.lookup_existing(session)
        target.snapshot_existing(session, existing)
        target.merge_onto_existing(session, existing)
