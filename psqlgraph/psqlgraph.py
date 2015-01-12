#
# Driver to implement the graph model in postgres
#

# External modules
import logging
import itertools
from sqlalchemy import create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import NoResultFound

#  ORM object base
Base = declarative_base()

#  PsqlNode modules
from validate import PsqlNodeValidator, PsqlEdgeValidator
from exc import QueryError, ProgrammingError, NodeCreationError, \
    ValidationError
import sanitizer
from constants import DEFAULT_RETRIES
from node import PsqlNode, PsqlVoidedNode
from edge import PsqlEdge, PsqlVoidedEdge
from util import session_scope, retryable, default_backoff


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

    def session_scope(self):
        return session_scope(self.engine)

    def set_node_validator(self, node_validator):
        """Override the node validation callback."""
        self.node_validator = node_validator

    def set_edge_validator(self, edge_validator):
        """Override the edge validation callback."""
        self.edge_validator = edge_validator

    def get_nodes(self, session=None):
        with session_scope(self.engine, session) as local:
            return local.query(PsqlNode)

    def get_edges(self, session=None):
        with session_scope(self.engine, session) as local:
            return local.query(PsqlEdge)

    def get_node_count(self, session=None):
        with session_scope(self.engine, session) as local:
            query = local.query(func.count(PsqlNode.key))
        return query.one()[0]

    def get_edge_count(self, session=None):
        with session_scope(self.engine, session) as local:
            query = local.query(func.count(PsqlEdge.key))
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
                    properties=properties,
                    session=session
                )

            else:
                """ we need to create a new node """

                if not node_id:
                    raise NodeCreationError(
                        'Cannot create a node with no node_id.')

                node = self.node_insert(PsqlNode(
                    node_id=node_id,
                    label=label,
                    system_annotations=system_annotations,
                    acl=acl,
                    properties=properties,
                ), session=local)

            return node

    def node_insert(self, node, session=None):
        """Takes a PsqlNode and inserts it into the graph.

        :param PsqlNode node: The node to be insterted into table ``nodes``
        :param session: |session|

        """

        self.logger.debug('Creating a new node: {}'.format(node))

        with session_scope(self.engine, session) as local:
            local.add(node)
            if not self.node_validator(node):
                raise ValidationError('Node failed schema constraints')
        return node

    def node_update(self, node, system_annotations={},
                    acl=[], properties={}, session=None):
        """
        This function assumes that you have already done a query for an
        existing node!  This function will take an node, void it and
        create a new node entry in its place

        :param PsqlNode node: The node to override
        :param session: |session|

        """

        self.logger.debug('Updating: {}'.format(node))
        if ((properties == node.properties) and (
                system_annotations == node.system_annotations) and (
                    acl == node.acl)):
            logging.debug('Node left unchanged: {}'.format(node))
            return node

        node.merge(system_annotations=system_annotations, acl=acl,
                   properties=properties)

        with session_scope(self.engine, session) as local:
            self._node_void(node, local)
            local.merge(node)
            if not self.node_validator(node):
                raise ValidationError('Node failed schema constraints')

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
            voided = PsqlVoidedNode(node)
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
        try:
            return self.node_lookup(
                node_id=node_id,
                property_matches=property_matches,
                system_annotation_matches=system_annotation_matches,
                label=label,
                session=session).one()
        except NoResultFound:
            return None

    def node_lookup(self, node_id=None, property_matches=None,
                    label=None, system_annotation_matches=None,
                    voided=False, session=None):
        """This function wraps both ``node_lookup_by_id`` and
        ``node_lookup_by_matches``. If matches are provided then the
        nodes will be queried by id. If id is provided, then the nodes
        will be queried by id.  Providing both id and matches will be
        treated as an invalid query.

        .. note::
            A query with no resulting nodes will return an empty list ``[]``.

        .. |voided| replace::
               Specifies whether the resulting query should represent
               voided nodes (deleted and transactional records) in the
               voided table.  This parameter defaults to ``False`` in
               order to only return active nodes.

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

        :param bool voided:

        :param session: |voided|
        :param session: |session|

        :returns: SqlAlchemy query object

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
                voided=voided,
                session=session,
            )

        else:
            return self.node_lookup_by_matches(
                property_matches=property_matches,
                system_annotation_matches=system_annotation_matches,
                voided=voided,
                label=label,
                session=session
            )

    def node_lookup_by_id(self, node_id, voided=False, session=None):
        """This function looks up a node by a given id.  If voided is True,
        then the returned query will consist of nodes that have been
        voided (deleted or replaced in a transaction).

        .. note::
            A query with no resulting nodes will return an empty list ``[]``.

        :param str node_id:
            The unique id that specifies a node in the table.

        :param session: |voided|
        :param session: |session|

        :returns:
            SqlAlchemy query object (or iterator of results if
            `included_voided` is True)

        """

        self.logger.debug('Looking up node by id: {id}'.format(id=node_id))

        with session_scope(self.engine, session) as local:
            if voided:
                return local.query(PsqlVoidedNode).filter(
                    PsqlVoidedNode.node_id == node_id)
            else:
                return local.query(PsqlNode).filter(
                    PsqlNode.node_id == node_id)

    def node_lookup_by_matches(self, property_matches=None,
                               system_annotation_matches=None,
                               label=None, voided=False, session=None):
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

        :param session: |voided|
        :param session: |session|

        :returns: SqlAlchemy query object
        """

        if voided:
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

            return query

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
                properties=properties,
                session=session
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

            self._node_void(node, session=local)
            properties = node.properties
            for key in property_keys:
                properties.pop(key)
            node.properties = {}
            local.flush()
            node.properties = properties

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

            self._node_void(node, session=local)
            system_annotations = node.system_annotations
            for key in system_annotation_keys:
                system_annotations.pop(key)
            node.system_annotations = {}
            local.flush()
            node.system_annotations = system_annotations

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

            # Void this noode's edges and the node entry
            self.logger.debug('deleting node: {0}'.format(node.node_id))
            self.edge_delete_by_node_id(node.node_id, session=local)
            self._node_void(node, session=local)
            local.delete(node)

    @retryable
    def edge_insert(self, edge, max_retries=DEFAULT_RETRIES,
                    backoff=default_backoff, session=None):
        """
        This function assumes that you have already done a query for an
        existing node!  This function will take an node, void it and
        create a new node entry in its place

        :param PsqlEdge edge: The PsqlEdge object to add to database.
        :param session: |max_retries|
        :param session: |backoff|
        :param session: |session|

        """

        self.logger.debug('Inserting: {0}'.format(edge))

        with session_scope(self.engine, session) as local:
            local.add(edge)
            local.flush()
            if not self.edge_validator(edge):
                raise ValidationError(
                    'Edge {} failed validation.'.format(edge))

        return edge

    def edge_update(self, edge, system_annotations={}, properties={},
                    session=None):
        """
        This function assumes that you have already done a query for an
        existing edge!  This function will take an edge, void it and
        create a new edge entry in its place

        :param PsqlEdge edge: The edge to update with the provided values
        :param dict properties: |properties|
        :param dict system_annotations: |system_annotations|
        :param session: |session|

        """

        self.logger.debug('Updating: {}'.format(edge))
        if ((properties == edge.properties) and (
                system_annotations == edge.system_annotations)):
            logging.debug('Edge left unchanged: {}'.format(edge))
            return edge

        edge.merge(system_annotations=system_annotations,
                   properties=properties)
        properties = edge.properties

        with session_scope(self.engine, session) as local:
            if not self.edge_validator(edge):
                raise ValidationError('Edge failed schema constraints')
            self._edge_void(edge, local)
            return local.merge(edge)

    def edge_lookup_one(self, src_id=None, dst_id=None, label=None,
                        voided=False, session=None):
        """This function is a simple wrapper for ``edge_lookup`` that
        constrains the query to return a single edge.  If multiple
        edges are found matchin the query, an exception will be raised

        .. note:: If no edges match the query, the return will be NoneType.

        :param str src_id:
            The node_id of the source node
        :param str dst_id: The edge to be voided
            The node_id of the destination node
        :param session: |voided|
        :param session: |session|
        :returns: A single PsqlEdge instances or None

        """

        try:
            return self.edge_lookup(
                src_id=src_id,
                dst_id=dst_id,
                label=label,
                voided=voided,
                session=session).one()
        except NoResultFound:
            return None

    def edge_lookup(self, src_id=None, dst_id=None, label=None,
                    voided=False, session=None):
        """This function looks up a edge by a given src_id and dst_id.  If
        voided is true, then the returned query will consist only of
        edges that have been voided.

        :param str src_id:
            The node_id of the source node
        :param str dst_id: The edge to be voided
            The node_id of the destination node
        :param session: |voided|
        :param session: |session|
        :returns:
            SqlAlchemy query object (or iterator of edges if
            `include_voided` is true)

        """

        if src_id is None and dst_id is None:
            raise QueryError('Cannot lookup edge, no src_id or dst_id')

        with session_scope(self.engine, session) as local:
            if voided:
                return self.edge_lookup_voided(src_id, dst_id, local)

            query = local.query(PsqlEdge)
            if src_id:
                query = query.filter(PsqlEdge.src_id == src_id)
            if dst_id:
                query = query.filter(PsqlEdge.dst_id == dst_id)
            if label:
                query = query.filter(PsqlEdge.label == label)
            return query

    def edge_lookup_voided(self, src_id=None, dst_id=None, label=None,
                           session=None):
        """This function looks up a edge by a given src_id and dst_id.  If
        voided is true, then the returned query will consist only of
        edges that have been voided.

        :param str src_id:
            The node_id of the source node
        :param str dst_id: The edge to be voided
            The node_id of the destination node
        :param session: |voided|
        :param session: |session|
        :returns: A list of PsqlEdge instances ([] if none found)

        """

        if src_id is None and dst_id is None:
            raise QueryError('Cannot lookup edge, no src_id or dst_id')

        with session_scope(self.engine, session) as local:
            query = local.query(PsqlVoidedEdge)
            if src_id:
                query = query.filter(PsqlVoidedEdge.src_id == src_id)
            if dst_id:
                query = query.filter(PsqlVoidedEdge.dst_id == dst_id)
            if label:
                query = query.filter(PsqlVoidedEdge.label == label)
            return query

    def _edge_void(self, edge, session=None):
        """Voids an edge.

        :param PsqlEdge edge:
            The ORM instance representing the edge entry
        :param session: |session|
        :returns: None

        """

        if not edge:
            raise ProgrammingError('edge_void was passed a NoneType PsqlEdge')

        self.logger.debug('Voiding edge: {}'.format(edge))
        with session_scope(self.engine, session) as local:
            voided = PsqlVoidedEdge(edge)
            local.add(voided)

    def edge_delete(self, edge, session=None):
        """Voids an edge.

        .. note:: Included for syntax similarity with :func:edge_delete.

        :param PsqlEdge edge:
            The ORM instance representing the edge entry
        :param session: |session|
        :returns: None

        """

        if not edge:
            raise ProgrammingError('edge_delete was passed a NoneType edge')

        self.logger.debug('Deleting edge: {}'.format(edge))
        with session_scope(self.engine, session) as local:
            self._edge_void(edge, session=local)
            local.delete(edge)

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
            for edge in itertools.chain(src_nodes, dst_nodes):
                self.edge_delete(edge, local)
