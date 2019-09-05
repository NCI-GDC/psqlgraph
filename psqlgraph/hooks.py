"""
Session hooks
"""
from sqlalchemy.inspection import inspect
from psqlgraph.node import Node
from psqlgraph.edge import Edge


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
        old_props = history(target, '_props', attr)
        if old_props is None:
            old_props = {}
        props.update(old_props)

        old_sysan = history(target, '_sysan', attr)
        if old_sysan is None:
            old_sysan = {}
        sysan.update(old_sysan)
    return props, sysan


def is_psqlgraph_entity(target):
    """Only attempt to track history on Nodes and Edges

    """
    return target.__class__ in (
        Node.__subclasses__()+Edge.__subclasses__())


def receive_before_flush(session, flush_context, instances):
    """Provide a session hook that gets called before the session is
    flushed.

    A snapshot of a node is created if any of the following are true:

    1. There are 'added' system_annotations
    2. There are 'added' properties
    3. There are 'deleted' system_annotations
    4. There are 'deleted' properties
    5. A node is marked to be deleted

    A snapshot of a node will consist of

    - Start with unchanged props/sysan
    - Merge deleted props/sysan on top of that

    """

    if session._set_flush_timestamps:
        session._flush_timestamp = list(
            session.execute("SELECT CURRENT_TIMESTAMP"))[0][0]

    for target in session.dirty:
        if not is_psqlgraph_entity(target):
            continue

        target._validate()
        props, sysan = get_old_version(target, 'unchanged', 'deleted')
        props_diff, sysan_diff = get_old_version(target, 'deleted', 'added')
        if props_diff or sysan_diff:
            target._snapshot_existing(session, props, sysan)
        target._merge_onto_existing(props, sysan)

        # Call custom session hook
        for f in target._session_hooks_before_update:
            f(target, session, flush_context, instances)

    for target in session.deleted:
        if not is_psqlgraph_entity(target):
            continue

        props, sysan = get_old_version(target, 'unchanged', 'deleted', 'added')
        target._snapshot_existing(session, props, sysan)

        # Call custom session hook
        for f in target._session_hooks_before_delete:
            f(target, session, flush_context, instances)

    for target in session.new:
        if not is_psqlgraph_entity(target):
            continue

        if isinstance(target, (Node, Edge)):
            target._validate()

        # Call custom session hook
        for f in target._session_hooks_before_insert:
            f(target, session, flush_context, instances)
