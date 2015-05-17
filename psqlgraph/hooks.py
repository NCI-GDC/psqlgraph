"""
Session hooks
"""
from sqlalchemy.inspection import inspect
from node import Node
from edge import Edge


def receive_before_flush(session, flush_context, instances):
    for target in session.dirty:
        target._validate()
        old_props_list = inspect(target).attrs.get('_props').history.deleted
        old_sysan_list = inspect(target).attrs.get('_sysan').history.deleted
        old_props = {} if not old_props_list else old_props_list[0]
        old_sysan = {} if not old_sysan_list else old_sysan_list[0]
        if old_props_list or old_sysan_list:
            target._snapshot_existing(session, old_props, old_sysan)
        target._merge_onto_existing(old_props, old_sysan)
    for target in session.new:
        if isinstance(target, (Node, Edge)):
            target._validate()
