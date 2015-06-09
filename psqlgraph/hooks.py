"""
Session hooks
"""
from sqlalchemy.inspection import inspect
from node import Node
from edge import Edge


def history(target, column, attr):
    """
    """
    old = getattr(inspect(target).attrs.get(column).history, attr)
    return old[0] if old else {}


def get_old_version(target, *attrs):
    """Loops through given attributes (choose from 'deleted', 'unchanged',
    'added') and returns a merged result with the first attr as the
    base and following attrs merged in order.

    :param target: A SQLAlchemy instance object
    :param attrs: A list of history object attribute names

    """

    sysan, props = dict(), dict()
    for attr in attrs:
        props.update(history(target, '_props', attr))
        sysan.update(history(target, '_sysan', attr))
    return props, sysan


def receive_before_flush(session, flush_context, instances):
    """Provide a session hook that gets called before the session is
    flushed.

    A snaphot of a node is created if any of the following are true:

    1. There are 'added' system_annotations
    2. There are 'added' properties
    3. There are 'deleted' system_annotations
    4. There are 'deleted' properties
    5. A node is marked to be deleted

    A snapshot of a node will consist of

    - Start with unchanged props/sysan
    - Merge deleted props/sysan on top of that

    """
    for target in session.dirty:
        target._validate()
        props, sysan = get_old_version(target, 'unchanged', 'deleted')
        props_diff, sysan_diff = get_old_version(target, 'deleted', 'added')
        if props_diff or sysan_diff:
            target._snapshot_existing(session, props, sysan)
        target._merge_onto_existing(props, sysan)
    for target in session.deleted:
        props, sysan = get_old_version(target, 'unchanged', 'deleted', 'added')
        target._snapshot_existing(session, props, sysan)
    for target in session.new:
        if isinstance(target, (Node, Edge)):
            target._validate()
