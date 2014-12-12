# ======== External modules ========
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, Text, String, func, \
    UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgres import ARRAY, JSONB, TIMESTAMP
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
    EdgeCreationError, ValidationError
import sanitizer

# ======== ORM object base ========
Base = declarative_base()

"""
Driver to implement the graph model in postgres
"""

# ======== Default constants ========
"""Used for retrying a write to postgres on exception catch
.. |DEFAULT_RETRIES| replace:: 2"""
DEFAULT_RETRIES = 2


@contextmanager
def session_scope(engine, session=None):
    """Provide a transactional scope around a series of operations."""

    if not session:
        Session = sessionmaker(expire_on_commit=False)
        Session.configure(bind=engine)
        local = Session()
        logging.debug('Created session {session}'.format(session=local))
    else:
        local = session

    try:
        yield local
        if not session:
            logging.debug('Committing session {session}'.format(session=local))
            local.commit()

    except Exception, msg:
        logging.error('Failed to commit session: {msg}'.format(msg=msg))
        logging.error('Rolling back session {session}'.format(session=local))
        local.rollback()
        raise

    finally:
        if not session:
            logging.debug('Expunging objects from {session}'.format(
                session=local))
            local.expunge_all()
            logging.debug('Closing session {session}'.format(session=local))
            local.close()


class PsqlNode(Base):

    """Node class to represent a node entry in the postgresql table
    'nodes' inherits the SQLAlchemy Base class
    """

    __tablename__ = 'nodes'

    key = Column(Integer, primary_key=True)
    node_id = Column(String(36), nullable=False)
    label = Column(Text, nullable=False)
    created = Column(TIMESTAMP, nullable=False,
                     default=sanitizer.sanitize(datetime.now()))
    acl = Column(ARRAY(Text))
    system_annotations = Column(JSONB, default={})
    properties = Column(JSONB, default={})
    __table_args__ = (UniqueConstraint('node_id', name='_node_id_uc'),)

    def __repr__(self):
        return '<PsqlNode({node_id}>'.format(node_id=self.node_id)

    def __init__(self, node_id=node_id, label=label, acl=[],
                 system_annotations={},
                 properties={}):

        system_annotations = sanitizer.sanitize(system_annotations)
        properties = sanitizer.sanitize(properties)
        self.node_id = node_id
        self.acl = acl
        self.system_annotations = system_annotations
        self.label = label
        self.properties = properties

    def merge(self, acl=[], system_annotations={}, properties={}):
        """Merges a new node onto this instance.  The parameter ``node``
        should contain the 'new' values with the following effects. In
        general, updates are additive. New properties will be added to
        old properties.  New system annotations will be added system
        annotations. New acl will will be added to old acl.  For
        removal of a property, system_annotation, or acl entry is
        required, see :func
        PsqlGraphDriver.node_delete_property_keys:, :func
        PsqlGraphDriver.node_delete_system_annotation_keys:, :func
        PsqlGraphDriver.node_remove_acl_item:

        .. note::
           If the new node contains an acl list, it be appended to
           the previous acl list

        The following class members cannot be updated: ``label, key, node_id``

        :param PsqlNode node: The new node to be merged onto this instance

        """

        if system_annotations:
            self.syste_annotations = self.system_annotations.update(
                sanitizer.sanitize(system_annotations))

        if properties:
            properties = sanitizer.sanitize(properties)
            self.properties.update(properties)

        if acl:
            self.acl += acl


class PsqlVoidedNode(Base):

    """Node class to represent a node entry in the postgresql table
    'nodes' inherits the SQLAlchemy Base class
    """

    __tablename__ = 'voided_nodes'

    key = Column(Integer, primary_key=True)
    node_id = Column(String(36), nullable=False)
    label = Column(Text, nullable=False)
    voided = Column(TIMESTAMP, nullable=False)
    created = Column(
        TIMESTAMP, nullable=False, default=sanitizer.sanitize(datetime.now()))
    acl = Column(ARRAY(Text))
    system_annotations = Column(JSONB, default={})
    properties = Column(JSONB, default={})

    def __repr__(self):
        return '<PsqlVoidedNode({node_id})>'.format(node_id=self.node_id)

    def __init__(self, node_id, label, acl, system_annotations,
                 properties, created, voided=None):

        system_annotations = sanitizer.sanitize(system_annotations)
        properties = sanitizer.sanitize(properties)
        self.node_id = node_id
        self.acl = acl
        self.system_annotations = system_annotations
        self.label = label
        self.properties = properties
        self.voided = datetime.now()


class PsqlEdge(Base):

    """Edge class to represent a edge entry in the postgresql table
    'edges' inherits the SQLAlchemy Base class.

    See tools/setup_psqlgraph script for details on table setup.
    """

    __tablename__ = 'edges'

    key = Column(Integer, primary_key=True)
    src_id = Column(String(36), nullable=False)
    dst_id = Column(String(36), nullable=False)
    voided = Column(TIMESTAMP)
    created = Column(
        TIMESTAMP, nullable=False, default=sanitizer.sanitize(datetime.now())
    )
    system_annotations = Column(JSONB, default={})
    label = Column(Text, nullable=False)
    properties = Column(JSONB, default={})

    def __init__(self, src_id=src_id, dst_id=dst_id, label=label,
                 system_annotations=system_annotations,
                 properties=properties):

        system_annotations = sanitizer.sanitize(system_annotations)
        properties = sanitizer.sanitize(properties)
        self.src_id = src_id
        self.dst_id = dst_id
        self.system_annotations = system_annotations
        self.label = label
        self.properties = properties

    def __repr__(self):
        return '<PsqlEdge(({src_id})->({dst_id}), voided={voided})>'.format(
            src_id=self.src_id,
            dst_id=self.dst_id,
            voided=(self.voided is not None)
        )

    def merge(self, edge):
        """Merges a new edge onto this instance.  The parameter ``edge``
        should contain the 'new' values with the following effects. In
        general, updates are additive. New properties will be added to
        old properties.  New system annotations will be added system
        annotations. For removal of a property or system_annotation
        entry is required, see :func:
        PsqlGraphDriver.edge_delete_property_keys, :func:
        PsqlGraphDriver.edge_delete_system_annotation_keys

        .. note::
            If the new edge contains an acl list, it be appended
            to the previous acl list

        The following class members cannot be updated: ``label, key,
        src_id, dst_id``

        :param PsqlEdge edge: The new edge to be merged onto this instance

        """

        new_system_annotations = copy.copy(self.system_annotations)
        new_properties = copy.copy(self.properties)

        new_system_annotations.update(sanitizer.sanitize(
            edge.system_annotations) or {})
        new_properties.update(sanitizer.sanitize(
            edge.properties) or {})

        return PsqlEdge(
            src_id=self.src_id,
            dst_id=self.dst_id,
            system_annotations=new_system_annotations,
            label=self.label,
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
    """This wrapper can be used to decorate a function to retry an
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

    """Driver to represent graphical structure with Postgresql using a
    table of node entries and edge entries

    """

    def __init__(self, host, user, password, database,
                 node_validator=None, edge_validator=None):

        self.user = user
        self.database = database
        self.host = host
        self.logger = logging.getLogger('psqlgraph')
        self.default_retries = 10

        if node_validator is None:
            node_validator = PsqlNodeValidator(self)
        if edge_validator is None:
            edge_validator = PsqlEdgeValidator(self)

        self.node_validator = node_validator
        self.edge_validator = edge_validator

        conn_str = 'postgresql://{user}:{password}@{host}/{database}'.format(
            user=user, password=password, host=host, database=database)

        self.engine = create_engine(conn_str)

    def set_node_validator(self, node_validator):
        """Override the node validation callback."""
        self.node_validator = node_validator

    def set_edge_validator(self, edge_validator):
        """Override the edge validation callback."""
        self.edge_validator = edge_validator

    def get_nodes(self, batch_size=1000, session=None):
        with session_scope(self.engine, session) as local:
            query = local.query(PsqlNode)
        return query.yield_per(batch_size)

    def get_edges(self, batch_size=1000, session=None):
        with session_scope(self.engine, session) as local:
            query = local.query(PsqlEdge).filter(PsqlEdge.voided.is_(None))
        return query.yield_per(batch_size)

    def get_node_count(self, session=None):
        with session_scope(self.engine, session) as local:
            query = local.query(func.count(PsqlNode.key))
        return query.one()[0]

    def get_edge_count(self, session=None):
        with session_scope(self.engine, session) as local:
            query = local.query(func.count(PsqlEdge.key)).filter(
                PsqlEdge.voided.is_(None))
        return query.one()[0]

    @retryable
    def node_merge(self, node_id=None, node=None, acl=[],
                   label=None, system_annotations={}, properties={},
                   session=None, max_retries=DEFAULT_RETRIES,
                   backoff=default_backoff):
        """This is meant to be the main interaction function with the
        library. It handles the traditional get_one_or_create while
        overloading the merging of properties, system_annotations.

        - If the node does not exist, it will be created.

        - If the node does exist, it will be updated.

        - This function is thread safe.

        - This function is wrapped by ``retryable`` decorator, set the
          number of maximum number of times that the merge will retry
          after a concurrency error, set the keyword arg ``max_retries``

        .. note::
            You must pass either a ``node_id`` or a ``node`` (not both).

        .. |label| replace:: The label to add to a new entry.  Labels
            on existing entries are immutable. In order to modify a
            label, you must manually delete the entry and recreate it
            with a new label.


        .. |system_annotations| replace::
            The dictionary representing arbitrary system annotation on
            the entry.  These annotations will be sanitized and
            inserted into the table as JSONB.

        .. |properties| replace:: The dictionary representing
            properties on the entry.  These properties will be
            sanitized and validated with the validator instance
            assigned to the entry type before insertion into the table
            as JSONB.

        .. |session| replace:: The SQLAlchemy session within which to
            perform operations.  This session is transactional.  If
            this function or those it calls cause the transaction to
            fail, it will be rolled-back, along with all previous
            actions taken within this session.  If a session is
            provided, it will not be committed before returning from
            this function.  If a session is not provided, a
            transactional session will be created that will be
            committed before returning.

        .. |max_retries| replace:: This function is retriable.  If not
            passed the parameter ``max_retries``, it will only attempt
            to retry modifications to the table ``DEFAULT_RETRIES``
            times. Retries will only occur on IntegrityError, as this
            is the most likely case for an integrity violation due to
            race condition.  This functionality is provided by the
            decorator wrapper function ``retryable``

        .. |backoff| replace::
            This function is retriable.  If not passed the parameter
            ``backoff``, it will only attempt to retry modifications
            to the table using the default backoff function
            DEFAULT_BACKOFF. Retries will only occur on
            IntegrityError, as this is the most likely case for an
            integrity violation due to race condition.  This
            functionality is provided by the decorator wrapper
            function @:func:retryable. See :func:default_backoff for
            details on over-riding this callback

        :param str node_id:
            The id specifying a single node. If the
            node entry does not exist, it will be created. This
            parameter is mutually exlucsive with ``node``
        :param PsqlNode node:
            The ORM instance of a node. If the node
            instance is not represented in the table, it will be
            inserted. This parameter is mutually exlucsive with
            ``node``
        :param str[] acl:
            The acl list to merge.  If a node entry exists in the
            table, the acl list will be appended.
        :param str label: |label|
        :param dict system_annotations: |system_annotations|
        :param dict properties: |properties|
        :param session: |session|
        :param int max_retries: |max_retries|
        :param callable backoff: |backoff|

        """

        with session_scope(self.engine, session) as local:
            if not node:
                """ try and lookup the node """
                node = self.node_lookup_one(node_id=node_id, session=local)

            if node:
                """ there is a pre-existing node """

                if label is not None and label != node.label:
                    raise NodeCreationError(
                        'Labels are immutable.  You must delete the node and '
                        'add a new one with an updated label.'
                    )

                self.node_update(
                    node,
                    system_annotations=system_annotations,
                    acl=acl,
                    properties=properties
                )

            else:
                """ we need to create a new node """
                self.logger.debug('Creating a new node')

                if not node_id:
                    raise NodeCreationError(
                        'Cannot create a node with no node_id.')

                node = self.node_insert(
                    node_id=node_id,
                    label=label,
                    system_annotations=system_annotations,
                    acl=acl,
                    properties=properties,
                    session=local
                )

            return node

    def node_insert(self, node_id, label, system_annotations={},
                    acl=[], properties={}, session=None):
        """
        This function assumes that you have already done a query for an
        existing node!  This function will take an node, void it and
        create a new node entry in its place

        :param PsqlNode new_node: The node with which to overwrite ``old_node``
        :param PsqlNode old_node:
            The existing node in the table to be overwritten

        :param session: |session|

        """

        self.logger.debug('Voiding to update: {0}'.format(node_id))

        node = PsqlNode(node_id=node_id, label=label,
                        system_annotations=system_annotations,
                        acl=acl, properties=properties)

        if not self.node_validator(node):
            raise ValidationError('Node failed schema constraints')

        with session_scope(self.engine, session) as local:
            local.add(node)

        return node

    def node_update(self, node, system_annotations={},
                    acl=[], properties={}, session=None):
        """
        This function assumes that you have already done a query for an
        existing node!  This function will take an node, void it and
        create a new node entry in its place

        :param PsqlNode new_node: The node with which to overwrite ``old_node``
        :param PsqlNode old_node:
            The existing node in the table to be overwritten

        :param session: |session|

        """

        self.logger.debug('Updating: {0}'.format(node.node_id))

        node.merge(system_annotations=system_annotations, acl=acl,
                   properties=properties)

        if not self.node_validator(node):
            raise ValidationError('Node failed schema constraints')

        with session_scope(self.engine, session) as local:
            self._node_void(node, local)
            local.merge(node)

    def _node_void(self, node, session=None):
        """if passed a non-null node, then ``node_void`` will set the
        timestamp on the voided column of the entry.

        .. warning:: This function does not propagate deletion to
        nodes.  If you are deleting a node from the table, use
        :func:node_delete

        If a session is provided, then this action will be carried out
        within the session (and not commited).

        If no session is provided, this function will commit to the
        database in a self-contained session.

        Voiding a node consists of setting the timestamp column
        ``voided`` to be equal to the current time.

        :param PsqlNode node: The node to timestamp as voided.
        :param session: |session|

        """
        if not node:
            raise ProgrammingError('node_void was passed a NoneType PsqlNode')

        with session_scope(self.engine, session) as local:
            voided = PsqlVoidedNode(
                node_id=node.node_id,
                label=node.label,
                properties=node.properties,
                system_annotations=node.system_annotations,
                acl=node.acl,
                created=node.created,
                voided=datetime.now(),
            )
            local.add(voided)

    def node_lookup_one(self, node_id=None, property_matches=None,
                        system_annotation_matches=None, label=None,
                        session=None):
        """This function is a simple wrapper for ``node_lookup`` that
        constrains the query to return a single node.

        .. note::
            If multiple nodes are found matchin the query, an
            exception will be raised.

        .. note::
            If no nodes match the query, the return will be NoneType.

        :param str node_id:
            The unique id that specifies a node in the table.

        :param str node_id:
            The dictionary that specifies a filter for
            node.properties.  The properties of matching nodes in the
            table must be a subset of those provided

        :param str system_annotation_matches:
            The dictionary that specifies a filter for
            node.system_annotations.  The system_annotations of
            matching nodes in the table must be a subset of those
            provided

        :param str label:
            Adds filter to query such that results must have label ``label``

        :param session: |session|

        :returns: PsqlNode or None

        """
        nodes = self.node_lookup(
            node_id=node_id,
            property_matches=property_matches,
            system_annotation_matches=system_annotation_matches,
            label=label,
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

        .. note::
            A query with no resulting nodes will return an empty list ``[]``.

        :param str node_id:
            The unique id that specifies a node in the table.

        :param dict property_matches:
            The dictionary that specifies a filter for
            node.properties.  The properties of matching nodes in the
            table must be a subset of those provided

        :param str system_annotation_matches:
            The dictionary that specifies a filter for
            node.system_annotations.  The system_annotations of
            matching nodes in the table must be a subset of those
            provided

        :param str label:
            Adds filter to query such that results must have label ``label``

        :param bool include_voided:
           Specifies whether results include voided nodes (deleted and
           transactional records).  This parameter defaults to
           ``False`` in order to only return active nodes.

        :param session: |session|

        :returns: list of PsqlNode

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
        """This function looks up a node by a given id.  If include_voided is
        true, then the returned list will include nodes that have been
        voided (deleted or replaced in a transaction).

        .. note::
            A query with no resulting nodes will return an empty list ``[]``.

        :param str node_id:
            The unique id that specifies a node in the table.

        :param bool include_voided:
           Specifies whether results include voided nodes (deleted and
           transactional records).  This parameter defaults to
           ``False`` in order to only return active nodes.

        :param session: |session|

        :returns: list of PsqlNode

        """

        self.logger.debug('Looking up node by id: {id}'.format(id=node_id))

        with session_scope(self.engine, session) as local:
            query = local.query(PsqlNode).filter(PsqlNode.node_id == node_id)
            result = query.all()

            if include_voided:
                query = local.query(PsqlVoidedNode).filter(
                    PsqlVoidedNode.node_id == node_id)
                result += query.all()

        return result

    def node_lookup_by_matches(self, property_matches=None,
                               system_annotation_matches=None,
                               label=None, include_voided=False,
                               session=None):
        """Query the node table for nodes whose properties or
        system_annotation are supersets of ``properties_matches`` and
        ``system_annotation_matches`` respectively

        .. note::
            A query with no resulting nodes will return an empty list ``[]``.

        :param dict property_matches:
            The dictionary that specifies a filter for
            node.properties.  The properties of matching nodes in the
            table must be a subset of those provided

        :param dict system_annotation_matches:
            The dictionary that specifies a filter for
            node.system_annotations.  The system_annotations of
            matching nodes in the table must be a subset of those
            provided

        :param str label:
            Adds filter to query such that results must have label ``label``

        :param bool include_voided:
           Specifies whether results include voided nodes (deleted and
           transactional records).  This parameter defaults to
           ``False`` in order to only return active nodes.

        :param session: |session|

        :returns: list of PsqlNode
        """

        if include_voided:
            raise NotImplementedError(
                'Library does not currently support query for '
                'voided nodes by matches'
            )

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
        """This function will overwrite an ORM node instance in the table or
        a node relating to a give ``node_id``.

        - If the node does not exist, it will be created.

        - If the node does exist, it will be overwritten.

        - This function is thread safe.

        - This function is wrapped by ``retryable`` decorator, set the
          number of maximum number of times that the merge will retry
          after a concurrency error, set the keyword arg ``max_retries``

        .. note:: You must pass either a ``node_id`` or a ``node`` (not both)

        :param str node_id:
            The id specifying a single node. If the
            node entry does not exist, it will be created. This
            parameter is mutually exlucsive with ``node``

        :param PsqlNode node:
            The ORM instance of a node. If the node
            instance is not represented in the table, it will be
            inserted. This parameter is mutually exlucsive with
            ``node``

        :param str[] acl: The acl list to overwrite with.
        :param str label: |label|
        :param dict system_annotations: |system_annotations|
        :param dict properties: |properties|
        :param session: |session|
        :param int max_retries: |max_retries|
        :param callable backoff: |backoff|

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

            self.logger.debug('overwritting node: {0}'.format(node.node_id))

            self.node_update(
                node,
                system_annotations=system_annotations,
                acl=acl,
                properties=properties
            )

    @retryable
    def node_delete_property_keys(self, property_keys, node_id=None,
                                  node=None, session=None,
                                  max_retries=DEFAULT_RETRIES,
                                  backoff=default_backoff):
        """This function will remove properties from the entry
        representing a node in the nodes table.

        - If the node does not exist, a QueryError exception will be raised

        - If the node does exist, the keys in the list
          property_keys will be removed from the
          properties JSONB document

        - This function is thread safe.

        - This function is wrapped by ``retryable`` decorator, set the
          number of maximum number of times that the merge will retry
          after a concurrency error, set the keyword arg ``max_retries``

        note:: You must pass either a ``node_id`` or a ``node``

        :param str node_id:
            The id specifying a single node. This parameter is
            mutually exlucsive with ``node_id``

        :param PsqlNode node:
            The ORM instance of a node. This
            parameter is mutually exlucsive with ``node``

        :param list(str) property_keys:
            A list of string values designating which key value pairs
            to remove from the properties JSONB document of
            the node entry

        :param session: |session|
        :param int max_retries: |max_retries|
        :param callable backoff: |backoff|

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

            self.node_update(node, properties=properties)

    @retryable
    def node_delete_system_annotation_keys(self,
                                           system_annotation_keys,
                                           node_id=None, node=None,
                                           session=None,
                                           max_retries=DEFAULT_RETRIES,
                                           backoff=default_backoff):
        """This function will remove system_annotations from the entry
        representing a node in the nodes table.

        - If the node does not exist, a QueryError exception will be raised

        - If the node does exist, the keys in the list
          system_annotation_keys will be removed from the
          system_annotations JSONB document

        - This function is thread safe.

        - This function is wrapped by ``retryable`` decorator, set the
          number of maximum number of times that the merge will retry
          after a concurrency error, set the keyword arg ``max_retries``

        note:: You must pass either a ``node_id`` or a ``node``

        :param str node_id:
            The id specifying a single node. This parameter is
            mutually exlucsive with ``node_id``

        :param PsqlNode node:
            The ORM instance of a node. This
            parameter is mutually exlucsive with ``node``

        :param list(str) system_annotation_keys:
            A list of string values designating which key value pairs
            to remove from the system_annotations JSONB document of
            the node entry

        :param session: |session|
        :param int max_retries: |max_retries|
        :param callable backoff: |backoff|

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

            self.logger.debug('updating node properties: {0}'.format(
                node.node_id))
            self.node_update(node, system_annotations=system_annotations)

    @retryable
    def node_delete(self, node_id=None, node=None,
                    session=None, max_retries=DEFAULT_RETRIES,
                    backoff=default_backoff):
        """This function will void an ORM node instance, or a node entry with
        node_id=``node_id`` if one exists.

        - If the node does not exist, a QueryError exception will be raised

        - If the node does exist, the keys in the list
          system_annotation_keys will be removed from the
          system_annotations JSONB document

        - This function is thread safe.

        - This function is wrapped by ``retryable`` decorator, set the
          number of maximum number of times that the merge will retry
          after a concurrency error, set the keyword arg ``max_retries``

        note:: You must pass either a ``node_id`` or a ``node``

        :param str node_id:
            The id specifying a single node. This parameter is
            mutually exlucsive with ``node_id``

        :param PsqlNode node:
            The ORM instance of a node. This
            parameter is mutually exlucsive with ``node``

        :param list(str) system_annotation_keys:
            A list of string values designating which key value pairs
            to remove from the system_annotations JSONB document of
            the node entry

        :param session: |session|
        :param int max_retries: |max_retries|
        :param callable backoff: |backoff|

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
            self._node_void(node, session=local)
            local.delete(node)

    @retryable
    def edge_merge(self, src_id=None, dst_id=None, edge=None, label=None,
                   system_annotations={}, properties={}, session=None,
                   max_retries=DEFAULT_RETRIES,
                   backoff=default_backoff):
        """This is meant to be the main interaction function with the library
        when dealing with edges. It handles the traditional
        get_one_or_create while overloading the merging of properties,
        system_annotations.

        - If the edge does not exist, it will be created.

        - If the edge does exist, it will be updated.

        - This function is thread safe.

        - This function is wrapped by ``retryable`` decorator, set the
          number of maximum number of times that the merge will retry
          after a concurrency error, set the keyword arg ``max_retries``

        .. note::
            You must pass either a ``src_id/dst_id`` or a ``edge`` (not both).

        :param str src_id:
            A string specifying the node_id of the source node.
        :param str edge_id:
            A string specifying the node_id of the destination node.
        :param PsqlEdge edge:
            The ORM instance of a edge. If the edge
            instance is not represented in the table, it will be
            inserted. This parameter is mutually exlucsive with
            ``edge``
        :param str label: |label|
        :param dict system_annotations: |system_annotations|
        :param dict properties: |properties|
        :param session: |session|
        :param int max_retries: |max_retries|
        :param callable backoff: |backoff|

        """

        if ((src_id is not None or dst_id is not None) and edge):
            raise ProgrammingError(
                'Specifying src_id/dst_id and an edge is invalid'
            )

        with session_scope(self.engine, session) as local:
            if not edge:
                """ try and lookup the edge """
                edge = self.edge_lookup_one(
                    src_id=src_id,
                    dst_id=dst_id,
                    session=local,
                )

            if edge:
                if label is not None and edge.label != label:
                    raise EdgeCreationError(
                        'Edge labels are immutable: '
                        'new {new} != {old} for edge ({s})->({d})'.format(
                            old=edge.label, new=label,
                            s=edge.src_id, d=edge.dst_id
                        )
                    )

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
                        'Cannot create a edge without src_id.')
                if dst_id is None:
                    raise EdgeCreationError(
                        'Cannot create a edge without dst_id.')

                new_edge = PsqlEdge(
                    src_id=src_id,
                    dst_id=dst_id,
                    label=label,
                    system_annotations=system_annotations,
                    properties=properties
                )

            if edge is not None:
                self.logger.debug('merging edge: ({src})->({dst})'.format(
                    src=edge.src_id, dst=edge.dst_id))

            self.edge_void_and_create(new_edge, edge, session=local)

    def edge_void_and_create(self, new_edge, old_edge, session=None):
        """
        This function assumes that you have already done a query for an
        existing edge!  This function will take an edge, void it and
        create a new edge entry in its place

        :param PsqlEdge new_edge:
            The new edge with which to replace the old one
        :param PsqlEdge old_edge:
            The edge to be voided
        :param session: |session|

        """

        self.logger.debug('voiding to update edge: ({src})->({dst})'.format(
            src=new_edge.src_id, dst=new_edge.dst_id))

        with session_scope(self.engine, session) as local:
            if old_edge:
                self.edge_void(old_edge, session)

            if not self.edge_validator(new_edge):
                raise ValidationError('Edge failed schema constraints')

            local.add(new_edge)

    def edge_lookup_one(self, src_id=None, dst_id=None,
                        include_voided=False, session=None):
        """This function is a simple wrapper for ``edge_lookup`` that
        constrains the query to return a single edge.  If multiple
        edges are found matchin the query, an exception will be raised

        .. note:: If no edges match the query, the return will be NoneType.

        :param str src_id:
            The node_id of the source node
        :param str dst_id: The edge to be voided
            The node_id of the destination node
        :param bool include_voided:
           Specifies whether results include voided nodes (deleted and
           transactional records).  This parameter defaults to
           ``False`` in order to only return active nodes.
        :param session: |session|
        :returns: A single PsqlEdge instances or None

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
        """This function looks up a edge by a given src_id and dst_id.  If
        include_voided is true, then the returned list will include
        edges that have been voided.

        .. note::
            If one is true then the return will be constrained to a
            single result

        .. note::
            If more than a single result is found, then an exception
            will be raised.

        :param str src_id:
            The node_id of the source node
        :param str dst_id: The edge to be voided
            The node_id of the destination node
        :param bool include_voided:
           Specifies whether results include voided nodes (deleted and
           transactional records).  This parameter defaults to
           ``False`` in order to only return active nodes.
        :param session: |session|
        :returns: A list of PsqlEdge instances ([] if none found)

        """

        if src_id is None and dst_id is None:
            raise QueryError('Cannot lookup edge, no src_id or dst_id')

        with session_scope(self.engine, session) as local:
            query = local.query(PsqlEdge)
            if src_id:
                query = query.filter(PsqlEdge.src_id == src_id)
            if dst_id:
                query = query.filter(PsqlEdge.dst_id == dst_id)
            if not include_voided:
                query = query.filter(PsqlEdge.voided.is_(None))

            result = query.all()
        return result

    def edge_void(self, edge, session=None):
        """Voids an edge.

        :param PsqlEdge edge:
            The ORM instance representing the edge entry
        :param session: |session|
        :returns: None

        """

        if not edge:
            raise ProgrammingError('edge_void was passed a NoneType PsqlEdge')

        self.logger.debug('voiding edge: ({src})->({dst})'.format(
            src=edge.src_id, dst=edge.dst_id))
        with session_scope(self.engine, session) as local:
            edge.voided = datetime.now()
            local.merge(edge)

    def edge_delete(self, edge, session=None):
        """Voids an edge.

        .. note:: Included for syntax similarity with :func:edge_delete.

        :param PsqlEdge edge:
            The ORM instance representing the edge entry
        :param session: |session|
        :returns: None

        """
        return self.edge_void(edge, session)

    def edge_delete_by_node_id(self, node_id, session=None):
        """Looks up all edges that reference ``node_id`` and voids them.

        This function is used to cascade node deletions to related
        edges. A query will be performed for nodes that have a src_id
        or dst_id matching parameter ``node_id``.  All of these edges
        will be voided.

        :param str node_id:
             The node_id which any edge contains as a source or destination.
        :returns: None

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
