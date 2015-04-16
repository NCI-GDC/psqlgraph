"""
Session hooks
"""


def receive_before_flush(session, flush_context, instances):
    for target in session.dirty:
        target._validate()
        existing = target._lookup_existing(session)
        target._snapshot_existing(session, existing)
        target._merge_onto_existing(session, existing)
