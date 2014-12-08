# ======== External modules ========
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, Text, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgres import TIMESTAMP, ARRAY, JSONB
from sqlalchemy.exc import IntegrityError
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
import time
import random
import logging
import copy

# ======== PsqlNode modules ========
from validate import PsqlNodeValidator, PsqlEdgeValidator
from exc import QueryError, ProgrammingError, NodeCreationError, \
    EdgeCreationError
import sanitizer

# ======== ORM object base ========
Base = declarative_base()

"""
Driver to implement the graph model in postgres
"""

# ======== Default constants ========
# Used for retrying a write to postgres on exception catch
DEFAULT_RETRIES = 2


@contextmanager
def session_scope(engine, session=None):
    """Provide a transactional scope around a series of operations."""

    if not session:
        Session = sessionmaker(expire_on_commit=False)
        Session.configure(bind=engine)
        local = Session()
        logging.info('Created session {session}'.format(session=local))
    else:
        local = session

    try:
        yield local
        if not session:
            logging.info('Committing session {session}'.format(session=local))
            local.commit()

    except Exception, msg:
        logging.error('Failed to commit session: {msg}'.format(msg=msg))
        logging.error('Rolling back session {session}'.format(session=local))
        local.rollback()
        raise

    finally:
        if not session:
            logging.info('Expunging objects from {session}'.format(
                session=local))
            local.expunge_all()
            logging.info('Closing session {session}'.format(session=local))
            local.close()


class PsqlNode(Base):

    """Node class to represent a node entry in the postgresql table
    'nodes' inherits the SQLAlchemy Base class
    """

    __tablename__ = 'nodes'

    key = Column(Integer, primary_key=True)
    node_id = Column(String(36), nullable=False)
    voided = Column(TIMESTAMP)
    created = Column(TIMESTAMP, nullable=False, default=datetime.now())
    acl = Column(ARRAY(Text))
    system_annotations = Column(JSONB, default={})
    label = Column(Text)
    properties = Column(JSONB, default={})

    def __repr__(self):
        return '<PsqlNode({node_id}, voided={voided})>'.format(
            node_id=self.node_id,
            voided=(self.voided is not None)
        )

    def merge(self, node):

        new_system_annotations = copy.copy(self.system_annotations)
        new_properties = copy.copy(self.properties)
        new_acl = self.acl[:] + (node.acl or [])
        new_label = (node.label or self.label)

        new_system_annotations.update(sanitizer.sanitize(
            node.system_annotations) or {})
        new_properties.update(sanitizer.sanitize(
            node.properties) or {})

        return PsqlNode(
            node_id=self.node_id,
            acl=new_acl,
            system_annotations=new_system_annotations,
            label=new_label,
            properties=new_properties,
        )


class PsqlEdge(Base):

    """Edge class to represent a edge entry in the postgresql table
    'edges' inherits the SQLAlchemy Base class
    """

    __tablename__ = 'edges'

    key = Column(Integer, primary_key=True)
    src_id = Column(String(36), nullable=False)
    dst_id = Column(String(36), nullable=False)
    voided = Column(TIMESTAMP)
    created = Column(TIMESTAMP, nullable=False, default=datetime.now())
    system_annotations = Column(JSONB, default={})
    label = Column(Text)
    properties = Column(JSONB, default={})

    def __repr__(self):
        return '<PsqlEdge(({src_id})->({dst_id}), voided={voided})>'.format(
            src_id=self.src_id,
            dst_id=self.dst_id,
            voided=(self.voided is not None)
        )

    def merge(self, edge):

        new_system_annotations = copy.copy(self.system_annotations)
        new_properties = copy.copy(self.properties)
        new_label = (edge.label or self.label)

        new_system_annotations.update(sanitizer.sanitize(
            edge.system_annotations) or {})
        new_properties.update(sanitizer.sanitize(
            edge.properties) or {})

        return PsqlEdge(
            src_id=self.src_id,
            dst_id=self.dst_id,
            system_annotations=new_system_annotations,
            label=new_label,
            properties=new_properties,
        )


def default_backoff(retries, max_retries):
    """This is the default backoff function used in the case of a retry by
    and function wrapped with the ``@retryable`` decorator.

    The behavior of the default backoff function is to sleep for a
    pseudo-random time between 0 and 2 seconds.

    """

    time.sleep(random.random()*(max_retries-retries)/max_retries*2)


def retryable(func):
    """

    This wrapper can be used to decorate a function to retry an
    operation in the case of an SQLalchemy IntegrityError.  This error
    means that a race-condition has occured and operations that have
    occured within the session may no longer be valid.

    You can set the number of retries by passing the keyword argument
    ``max_retries`` to the wrapped function.  It's therefore important
    that ``max_retries`` is included as a kwarg in the definition of
    the wrapped function.

    Setting ``max_retries`` to 0 will prevent retries upon failure;
    wrapped function will execute once.

    Similar to ``max_retries``, the kwarg ``backoff`` is a callback
    function that allows the user of the library to over-ride the
    default backoff function in the case of a retry.  See `func
    default_backoff`

    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        retries = 0
        max_retries = kwargs.get('max_retries', DEFAULT_RETRIES)
        backoff = kwargs.get('backoff', default_backoff)
        while retries <= max_retries:
            try:
                return func(*args, **kwargs)
            except IntegrityError:
                logging.debug(
                    'Race-condition caught? ({0}/{1} retries)'.format(
                        retries, max_retries))
                if retries >= max_retries:
                    logging.error(
                        'Unabel to execute {f}, max retries exceeded'.format(
                            f=func))
                    raise
                retries += 1
                backoff(retries, max_retries)
            else:
                break
    return wrapper


class PsqlGraphDriver(object):

    def __init__(self, host, user, password, database):

        self.user = user
        self.database = database
        self.host = host
        self.logger = logging.getLogger('psqlgraph')
        self.default_retries = 10

        self.node_validator = PsqlNodeValidator(self)
        self.edge_validator = PsqlEdgeValidator(self)

        conn_str = 'postgresql://{user}:{password}@{host}/{database}'.format(
            user=user, password=password, host=host, database=database)

        self.engine = create_engine(conn_str)

    def set_node_validator(self, node_validator):
        self.node_validator = node_validator

    def set_edge_validator(self, edge_validator):
        self.edge_validator = edge_validator

    @retryable
    def node_merge(self, node_id=None, node=None, acl=[],
                   system_annotations={}, label=None, properties={},
                   session=None, max_retries=DEFAULT_RETRIES,
                   backoff=default_backoff):
        """

        This is meant to be the main interaction function with the
        library. It handles the traditional get_one_or_create while
        overloading the merging of properties, system_annotations.

        - If the node does not exist, it will be created.

        - If the node does exist, it will be updated.

        - This function is thread safe.

        - This function is wrapped by ``retryable`` decorator, set the
          number of maximum number of times that the merge will retry
          after a concurrency error, set the keyword arg ``max_retries``

        """

        with session_scope(self.engine, session) as local:
            if not node:
                """ try and lookup the node """
                node = self.node_lookup_one(node_id=node_id, session=local)

            if node:
                """ there is a pre-existing node """
                new_node = node.merge(PsqlNode(
                    system_annotations=system_annotations,
                    acl=acl,
                    label=label,
                    properties=properties
                ))

            else:
                """ we need to create a new node """
                self.logger.debug('Creating a new node')

                if not node_id:
                    raise NodeCreationError(
                        'Cannot create a node with no node_id.')

                new_node = PsqlNode(
                    node_id=node_id,
                    system_annotations=system_annotations,
                    acl=acl,
                    label=label,
                    properties=properties
                )

            self.logger.debug('Merging node: {0}'.format(new_node.node_id))
            self.node_void_and_create(new_node, node, session=local)

    def node_void_and_create(self, new_node, old_node, session=None):
        """
        This function assumes that you have already done a query for an
        existing node!  This function will take an node, void it and
        create a new node entry in its place
        """

        self.logger.debug('Voiding to update: {0}'.format(new_node.node_id))

        with session_scope(self.engine, session) as local:
            if old_node:
                self.node_void(old_node, session)

            if not self.node_validator(new_node):
                raise NodeCreationError('Node failed schema constraints')

            local.add(new_node)

    def node_void(self, node, session=None):
        """if passed a non-null node, then ``node_void`` will set the
        timestamp on the voided column of the entry.

        If a session is provided, then this action will be carried out
        within the session (and not commited).

        If no session is provided, this function will commit to the
        database in a self-contained session.

        """
        if not node:
            raise ProgrammingError('node_void was passed a NoneType PsqlNode')

        with session_scope(self.engine, session) as local:
            node.voided = datetime.now()
            local.merge(node)

    def node_lookup_one(self, node_id=None, property_matches=None,
                        system_annotation_matches=None, include_voided=False,
                        session=None, label=None):
        """
        This function is simply a wrapper for ``node_lookup`` that
        constrains the query to return a single node.  If multiple
        nodes are found matchin the query, an exception will be raised

        If no nodes match the query, the return will be NoneType.
        """
        nodes = self.node_lookup(
            node_id=node_id,
            property_matches=property_matches,
            system_annotation_matches=system_annotation_matches,
            label=label,
            include_voided=include_voided,
            session=session,
        )

        if len(nodes) > 1:
            raise QueryError(
                'Expected a single result for query, got {n}'.format(
                    n=len(nodes)))
        if len(nodes) < 1:
            return None

        return nodes[0]

    def node_lookup(self, node_id=None, property_matches=None, label=None,
                    system_annotation_matches=None, include_voided=False,
                    session=None):
        """
        This function wraps both ``node_lookup_by_id`` and
        ``node_lookup_by_matches``. If matches are provided then the
        nodes will be queried by id. If id is provided, then the nodes
        will be queried by id.  Providing both id and matches will be
        treated as an invalid query.
        """

        if ((node_id is not None) and
            ((property_matches is not None) or (label is not None)
             or (system_annotation_matches is not None))):
            raise QueryError('Node lookup by node_id and kv/label matches not'
                             ' accepted.')

        if ((node_id is None) and
            ((property_matches is None) and
             (system_annotation_matches is None) and
             (label is None))):
            raise QueryError('No node_id or matches specified')

        if node_id is not None:
            return self.node_lookup_by_id(
                node_id=node_id,
                include_voided=include_voided,
                session=session,
            )

        else:
            return self.node_lookup_by_matches(
                property_matches=property_matches,
                system_annotation_matches=system_annotation_matches,
                include_voided=include_voided,
                label=label,
            )

    def node_lookup_by_id(self, node_id, include_voided=False, session=None):
        """
        This function looks up a node by a given id.  If include_voided is
        true, then the returned list will include nodes that have been
        voided. If one is true then the return will be constrained to
        a single result (if more than a single result is found, then
        an exception will be raised.  If session is specified, the
        query will be performed within the givin session, otherwise a
        new one will be created.
        """

        self.logger.debug('Looking up node by id: {id}'.format(id=node_id))

        with session_scope(self.engine, session) as local:
            query = local.query(PsqlNode).filter(PsqlNode.node_id == node_id)

            if not include_voided:
                query = query.filter(PsqlNode.voided.is_(None))

            result = query.all()

        return result

    def node_lookup_by_matches(self, property_matches=None,
                               system_annotation_matches=None,
                               include_voided=False, label=None,
                               session=None):
        """
        """

        system_annotation_matches = sanitizer.sanitize(
            system_annotation_matches)
        property_matches = sanitizer.sanitize(property_matches)

        with session_scope(self.engine, session) as local:
            query = local.query(PsqlNode)

            # Filter system_annotations
            if system_annotation_matches:
                print system_annotation_matches
                for key, value in system_annotation_matches.iteritems():
                    if value is not None:
                        query = query.filter(
                            PsqlNode.system_annotations[key].cast(
                                sanitizer.get_type(value)) == value
                        )

            # Filter properties
            if property_matches:
                for key, value in property_matches.iteritems():
                    if value is not None:
                        query = query.filter(
                            PsqlNode.properties[key].cast(
                                sanitizer.get_type(value)) == value
                        )

            if not include_voided:
                query = query.filter(PsqlNode.voided.is_(None))

            if label is not None:
                query = query.filter(PsqlNode.label == label)

            result = query.all()
        return result

    @retryable
    def node_clobber(self, node_id=None, node=None, acl=[],
                     system_annotations={}, label=None,
                     properties={}, session=None,
                     max_retries=DEFAULT_RETRIES,
                     backoff=default_backoff):
        """

        It handles the traditional get_one_or_create while
        overloading the complete updating of properties,
        system_annotations.

        - If the node does not exist, it will be created.

        - If the node does exist, it will be updated.

        - This function is thread safe.

        - This function is wrapped by ``retryable`` decorator, set the
          number of maximum number of times that the merge will retry
          after a concurrency error, set the keyword arg ``max_retries``

        """

        with session_scope(self.engine, session) as local:
            if not node:
                """ try and lookup the node """
                node = self.node_lookup_one(
                    node_id=node_id,
                    session=local,
                )

            if not node_id and node:
                node_id = node.node_id

            elif not node_id:
                raise NodeCreationError(
                    'Cannot create a node with no node_id.')

            new_node = PsqlNode(
                node_id=node_id,
                system_annotations=system_annotations,
                acl=acl,
                label=label,
                properties=properties
            )

            self.logger.debug('overwritting node: {0}'.format(node.node_id))
            self.node_void_and_create(new_node, node, session=local)

    @retryable
    def node_delete_property_keys(self, property_keys, node_id=None,
                                  node=None, session=None,
                                  max_retries=DEFAULT_RETRIES,
                                  backoff=default_backoff):
        """

        """

        with session_scope(self.engine, session) as local:
            if not node:
                """ try and lookup the node """
                node = self.node_lookup_one(node_id=node_id, session=local)

            if not node:
                raise QueryError('Node not found')

            properties = node.properties
            for key in property_keys:
                properties.pop(key)

            new_node = PsqlNode(
                node_id=node.node_id,
                system_annotations=node.system_annotations,
                acl=node.acl,
                label=node.label,
                properties=properties
            )

            self.node_void_and_create(new_node, node, session=local)

    @retryable
    def node_delete_system_annotation_keys(self,
                                           system_annotation_keys,
                                           node_id=None, node=None,
                                           session=None,
                                           max_retries=DEFAULT_RETRIES,
                                           backoff=default_backoff):
        """
        """

        with session_scope(self.engine, session) as local:
            if not node:
                """ try and lookup the node """
                node = self.node_lookup_one(node_id=node_id, session=local)

            if not node:
                raise QueryError('Node not found')

            system_annotations = node.system_annotations
            for key in system_annotation_keys:
                system_annotations.pop(key)

            new_node = PsqlNode(
                node_id=node.node_id,
                system_annotations=system_annotations,
                acl=node.acl,
                label=node.label,
                properties=node.properties
            )

            self.logger.debug('updating node properties: {0}'.format(
                node.node_id))
            self.node_void_and_create(new_node, node, session=local)

    @retryable
    def node_delete(self, node_id=None, node=None,
                    session=None, max_retries=DEFAULT_RETRIES,
                    backoff=default_backoff):
        """
        - This function is thread safe.

        - This function is wrapped by ``retryable`` decorator, set the
          number of maximum number of times that the merge will retry
          after a concurrency error, set the keyword arg ``max_retries``

        """

        with session_scope(self.engine, session) as local:
            if not node:
                """ try and lookup the node """
                node = self.node_lookup_one(node_id=node_id, session=local)

            if not node:
                raise QueryError('Node not found')

            # Void this node's edges and the node entry
            self.logger.debug('deleting node: {0}'.format(node.node_id))
            self.edge_delete_by_node_id(node.node_id, session=local)
            self.node_void(node, session=local)

    @retryable
    def edge_merge(self, src_id=None, dst_id=None, edge=None, acl=[],
                   system_annotations={}, properties={}, session=None,
                   max_retries=DEFAULT_RETRIES,
                   backoff=default_backoff):
        """

        This is meant to be the main interaction function with the
        library. It handles the traditional get_one_or_create while
        overloading the merging of properties, system_annotations.

        - If the edge does not exist, it will be created.

        - If the edge does exist, it will be updated.

        - This function is thread safe.

        - This function is wrapped by ``retryable`` decorator, set the
          number of maximum number of times that the merge will retry
          after a concurrency error, set the keyword arg ``max_retries``

        """

        with session_scope(self.engine, session) as local:
            if not edge:
                """ try and lookup the edge """
                edge = self.edge_lookup_one(
                    src_id=src_id,
                    dst_id=dst_id,
                    session=local,
                )

            if edge:
                """ there is a pre-existing edge """
                new_edge = edge.merge(PsqlEdge(
                    system_annotations=system_annotations,
                    properties=properties
                ))

            else:
                """ we need to create a new edge """
                self.logger.debug('Creating a new edge')

                if src_id is None:
                    raise EdgeCreationError(
                        'Cannot create a edge with no src_id.')
                if dst_id is None:
                    raise EdgeCreationError(
                        'Cannot create a edge with no dst_id.')

                new_edge = PsqlEdge(
                    src_id=src_id,
                    dst_id=dst_id,
                    system_annotations=system_annotations,
                    properties=properties
                )

            if edge is not None:
                self.logger.debug('merging edge: ({src})->({dst})'.format(
                    src=edge.src_id, dst_id=edge.dst_id))

            self.edge_void_and_create(new_edge, edge, session=local)

    def edge_void_and_create(self, new_edge, old_edge, session=None):
        """
        This function assumes that you have already done a query for an
        existing edge!  This function will take an edge, void it and
        create a new edge entry in its place
        """

        self.logger.debug('voiding to update edge: ({src})->({dst})'.format(
            src=new_edge.src_id, dst=new_edge.dst_id))

        with session_scope(self.engine, session) as local:
            if old_edge:
                self.edge_void(old_edge, session)

            if not self.edge_validator(new_edge):
                raise EdgeCreationError('Node failed schema constraints')

            local.add(new_edge)

    def edge_lookup_one(self, src_id=None, dst_id=None,
                        include_voided=False, session=None):
        """
        This function is simply a wrapper for ``edge_lookup`` that
        constrains the query to return a single edge.  If multiple
        edges are found matchin the query, an exception will be raised

        If no edges match the query, the return will be NoneType.
        """

        edges = self.edge_lookup(
            src_id=src_id,
            dst_id=dst_id,
            include_voided=include_voided,
            session=session,
        )

        if len(edges) > 1:
            raise QueryError(
                'Expected a single result for query, got {n}'.format(
                    n=len(edges)))
        if len(edges) < 1:
            return None

        return edges[0]

    def edge_lookup(self, src_id=None, dst_id=None,
                    include_voided=False, session=None):
        """
        This function looks up a edge by a given id.  If include_voided is
        true, then the returned list will include edges that have been
        voided. If one is true then the return will be constrained to
        a single result (if more than a single result is found, then
        an exception will be raised.  If session is specified, the
        query will be performed within the givin session, otherwise a
        new one will be created.
        """

        if src_id is None and dst_id is None:
            raise QueryError('Cannot lookup edge, no src_id or dst_id')

        with session_scope(self.engine, session) as local:
            if src_id:
                query = local.query(PsqlEdge).filter(PsqlEdge.src_id == src_id)
            if dst_id:
                query = local.query(PsqlEdge).filter(PsqlEdge.dst_id == dst_id)

            if not include_voided:
                query = query.filter(PsqlEdge.voided.is_(None))

            result = query.all()
        return result

    def edge_void(self, edge, session=None):
        """
        Voids an edge entry
        """

        if not edge:
            raise ProgrammingError('edge_void was passed a NoneType PsqlEdge')

        self.logger.debug('voiding edge: ({src})->({dst})'.format(
            src=edge.src_id, dst=edge.dst_id))
        with session_scope(self.engine, session) as local:
            edge.voided = datetime.now()
            local.merge(edge)

    def edge_delete_by_node_id(self, node_id, session=None):
        """Looks up all edges that reference ``node_id`` and voids them.

        This function is used primarily to cascade node deletions to
        related edges
        """

        if not node_id:
            raise ProgrammingError('node_id cannot be NoneType')

        self.logger.debug('cascading node deletion to edge: {0})'.format(
            node_id))

        with session_scope(self.engine, session) as local:
            src_nodes = self.edge_lookup(src_id=node_id, session=local)
            dst_nodes = self.edge_lookup(dst_id=node_id, session=local)
            for edge in src_nodes + dst_nodes:
                self.edge_void(edge)
