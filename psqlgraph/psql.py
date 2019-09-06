# Driver to implement the graph model in postgres
#

import logging
# External modules
from contextlib import contextmanager
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, configure_mappers
from xlocal import xlocal
import socket

# Custom modules
from psqlgraph.edge import Edge
from psqlgraph.hooks import receive_before_flush
from psqlgraph.node import PolyNode, Node
from psqlgraph.query import GraphQuery
from psqlgraph.util import retryable, default_backoff
from psqlgraph.voided_edge import VoidedEdge
from psqlgraph.voided_node import VoidedNode
from psqlgraph.session import GraphSession

DEFAULT_RETRIES = 0


class PsqlGraphDriver(object):

    acceptable_isolation_levels = ['REPEATABLE_READ', 'SERIALIZABLE']

    def __init__(self, host, user, password, database, **kwargs):
        """Create a Postgresql Graph Driver

        :param bool set_flush_timestamps:
            Is `True` by default.  Setting this to `True` will
            perform an extra database query to get the server time at
            flush and store `session._flush_timestamp`.

        """

        # Parse kwargs
        connect_args = kwargs.pop('connect_args', {})
        kwargs.pop('node_validator', None)
        kwargs.pop('edge_validator', None)
        self.set_flush_timestamps = kwargs.pop('set_flush_timestamps', True)
        if 'isolation_level' not in kwargs:
            kwargs['isolation_level'] = 'REPEATABLE_READ'
        if 'application_name' in kwargs:
            connect_args['application_name'] = kwargs.pop('application_name')
        else:
            connect_args['application_name'] = socket.gethostname()

        # Construct connection string
        host = '' if host is None else host
        conn_str = 'postgresql://{user}:{password}@{host}/{database}'.format(
            user=user, password=password, host=host, database=database)
        if kwargs['isolation_level'] not in self.acceptable_isolation_levels:
            logging.warn((
                "Using an isolation level '{}' that is not in the list of "
                "acceptable isolation levels {} is not safe and should be "
                "avoided.  Doing this can result in one session overwriting "
                "the commit of a concurrent session and losing data!"
            ).format(
                kwargs['isolation_level'], self.acceptable_isolation_levels))

        # Create driver engine
        self.engine = create_engine(
            conn_str,
            encoding='latin1',
            connect_args=connect_args,
            **kwargs
        )

        # Create context for xlocal sessions
        self.context = xlocal()

    def _new_session(self):
        Session = sessionmaker(expire_on_commit=False, class_=GraphSession)
        Session.configure(bind=self.engine, query_cls=GraphQuery)
        session = Session()
        session._flush_timestamp = None
        session._set_flush_timestamps = self.set_flush_timestamps
        event.listen(session, 'before_flush', receive_before_flush)
        return session

    def has_session(self):
        return hasattr(self.context, "session")

    def current_session(self):
        return self.context.session

    @contextmanager
    def session_scope(self, session=None, can_inherit=True,
                      must_inherit=False):
        """Provide a transactional scope around a series of operations.

        This session scope has a deceptively complex behavior, so be
        careful when nesting sessions.

        .. note::
            A session scope that is not nested has the following
            properties:

        1. Driver calls within the session scope will, by default,
           inherit the scope's session.

        2. Explicitly passing a session as ``session`` will cause driver
           calls within the session scope to use the explicitly passed
           session.

        3. Setting ``can_inherit`` to True will have no effect
           if not wrapped in a parent session

        4. Setting ``must_inherit`` to True will raise a RuntimeError
           if not wrapped in a parent session

        .. note::
            A session scope that is nested has the following
            properties given ``driver`` is a PsqlGraphDriver instance:

        Example::

            with driver.session_scope() as A:
                driver.node_insert()  # uses session A
                with driver.session_scope(A) as B:
                    B == A  # is True
                with driver.session_scope() as C:
                    C == A  # is True
                with driver.session_scope():
                    driver.node_insert()  # uses session A still
                with driver.session_scope(can_inherit=False):
                    driver.node_insert()  # uses new session D
                with driver.session_scope(can_inherit=False) as D:
                    D != A  # is True
                with driver.session_scope() as E:
                    E.rollback()  # rolls back session A
                with driver.session_scope(can_inherit=False) as F:
                    F.rollback()  # does not roll back session A
                with driver.session_scope(can_inherit=False) as G:
                    G != A  # is True
                    driver.node_insert()  # uses session G
                    with driver.session_scope(A) as H:
                        H == A  # true
                        H != G  # true
                        H.rollback()  # rolls back A but not G
                    with driver.session_scope(A):
                        driver.node_insert()  # uses session A

        :param session:
            The SQLAlchemy session to force the session scope to
            inherit
        :param bool can_inherit:
            The boolean value which determines whether the session
            scope inherits the session from any parent sessions in a
            nested context.  The default behavior is to inherit the
            parent's session.  If the session stack is empty for the
            driver, then this parameter is moot, there is no session
            to inherit, so one must be created.
        :param bool must_inherit:
            The boolean value which determines whether the session
            scope must inherit a session from a parent session.  This
            parameter can be set to true to prevent session leaks from
            functions which return raw query objects

        """

        if must_inherit and not self.has_session():
            raise RuntimeError(
                'Session scope requires it to be wrapped in a pre-existing '
                'session.  This was likely done to prevent a leaked session '
                'from a function which returns a query object.')

        # Set up local session
        inherited_session = True
        if session:
            local = session
        elif not (can_inherit and self.has_session()):
            inherited_session = False
            local = self._new_session()
        else:
            local = self.current_session()

        # Context manager functionality
        try:
            with self.context(session=local):
                yield local

            if not inherited_session:
                local.commit()

        except Exception as msg:
            logging.error('Rolling back session {}'.format(msg))
            local.rollback()
            raise

        finally:
            if not inherited_session:
                local.expunge_all()
                local.close()

    def nodes(self, query=Node):
        """.. _nodes:

        """
        self._configure_driver_mappers()
        with self.session_scope(must_inherit=True) as local:
            if isinstance(query, list) or isinstance(query, tuple):
                return local.query(*query)
            else:
                return local.query(query)

    def __call__(self, *args, **kwargs):
        return self.nodes(*args, **kwargs)

    def edges(self, query=Edge):
        self._configure_driver_mappers()
        with self.session_scope(must_inherit=True) as local:
            if isinstance(query, list) or isinstance(query, tuple):
                return local.query(*query)
            else:
                return local.query(query)

    def _configure_driver_mappers(self):
        try:
            configure_mappers()
        except Exception as e:
            logging.error((
                '{}: Unable to configure mappers. '
                'Have you imported your models?'
            ).format(str(e)))

    def voided_nodes(self, query=VoidedNode):
        with self.session_scope(must_inherit=True) as local:
            if isinstance(query, list) or isinstance(query, tuple):
                return local.query(*query)
            else:
                return local.query(query)

    def voided_edges(self, query=VoidedEdge):
        with self.session_scope(must_inherit=True) as local:
            if isinstance(query, list) or isinstance(query, tuple):
                return local.query(*query)
            else:
                return local.query(query)

    def set_node_validator(self, node_validator):
        raise NotImplemented('Deprecated.')

    def set_edge_validator(self, edge_validator):
        raise NotImplemented('Deprecated.')

    def get_nodes(self, session=None, batch_size=1000):
        return self.nodes().yield_per(batch_size)

    def get_edges(self, session=None, batch_size=1000):
        return self.edges().yield_per(batch_size)

    def get_node_count(self, session=None):
        return self.nodes().count()

    def get_edge_count(self, session=None):
        return self.edges().count()

    def node_merge(self, node_id=None, node=None, acl=None,
                   label=None, system_annotations=None, properties=None,
                   session=None, max_retries=DEFAULT_RETRIES,
                   backoff=default_backoff):

        with self.session_scope() as local:
            if not node and not label:
                node = self.nodes().ids([node_id]).scalar()

            elif not node and label:
                cls = Node.get_subclass(label)
                node = self.nodes(cls).ids([node_id]).scalar()

            properties = properties or {}
            system_annotations = system_annotations or {}
            if not node:
                node = PolyNode(
                    node_id, label, acl, system_annotations, properties)
            else:
                self.node_update(
                    node, system_annotations, acl, properties, local)

            local.merge(node)

        return node

    def node_insert(self, node, session=None):
        with self.session_scope() as local:
            local.add(node)

    def node_update(self, node, system_annotations=None,
                    acl=None, properties=None, session=None):

        properties = properties or {}
        system_annotations = system_annotations or {}

        with self.session_scope() as local:
            node.system_annotations.update(system_annotations)
            if acl is not None:
                node.acl = acl
            node.properties.update(properties)
            local.merge(node)

    def _node_void(self, node, session=None):
        raise NotImplementedError('Deprecated.')

    def node_lookup(self, node_id=None, property_matches=None,
                    label=None, system_annotation_matches=None,
                    voided=False, session=None):
        if voided:
            query = self.voided_nodes()
        elif not label:
            query = self.nodes()
        else:
            cls = Node.get_subclass(label)
            query = self.nodes(cls)

        if node_id is not None:
            node_id = node_id.split(",") if isinstance(node_id, str) else node_id
            query = query.ids(node_id)
        if property_matches is not None:
            query = query.props(property_matches)
        if system_annotation_matches is not None:
            query = query.sysan(system_annotation_matches)
        return query

    def node_lookup_one(self, *args, **kwargs):
        return self.node_lookup(*args, **kwargs).scalar()

    def node_lookup_by_id(self, node_id, voided=False, session=None):
        return self.node_lookup(
            node_id=node_id, voided=voided, session=session)

    def node_lookup_by_matches(self, property_matches=None,
                               system_annotation_matches=None,
                               label=None, voided=False, session=None):
        return self.node_lookup(
            property_matches=property_matches,
            system_annotation_matches=system_annotation_matches,
            voided=voided, session=session)

    @retryable
    def node_clobber(self, node_id=None, node=None, acl=None,
                     system_annotations=None, properties=None,
                     session=None, max_retries=DEFAULT_RETRIES,
                     backoff=default_backoff):
        with self.session_scope(session) as local:
            if not node:
                node = self.nodes().ids(node_id).one()
            if acl is not None:
                node.acl = acl
            if system_annotations is not None:
                node.system_annotations = system_annotations
            if properties is not None:
                node.properties = properties
            local.merge(node)

    @retryable
    def node_delete_property_keys(self, property_keys, node_id=None,
                                  node=None, session=None,
                                  max_retries=DEFAULT_RETRIES,
                                  backoff=default_backoff):
        raise NotImplemented('Deprecated.')

    @retryable
    def node_delete_system_annotation_keys(self,
                                           system_annotation_keys,
                                           node_id=None, node=None,
                                           session=None,
                                           max_retries=DEFAULT_RETRIES,
                                           backoff=default_backoff):
        raise NotImplementedError()
        # with self.session_scope(session) as local:
        #     if not node:
        #         node = self.node_lookup_one(node_id=node_id)
        #
        #     if not node:
        #         raise QueryError('Node not found')
        #
        #     for key in system_annotation_keys:
        #         del node.system_annotations[key]
        #     local.merge(node)

    @retryable
    def node_delete(self, node_id=None, node=None,
                    session=None, max_retries=DEFAULT_RETRIES,
                    backoff=default_backoff):
        with self.session_scope(session) as local:
            local.flush()
            if node is None:
                node = self.node_lookup(node_id=node_id).one()
            local.delete(node)

    @retryable
    def edge_insert(self, edge, max_retries=DEFAULT_RETRIES,
                    backoff=default_backoff, session=None):
        with self.session_scope(session) as local:
            local.flush()
            local.add(edge)
            local.flush()
        return edge

    def edge_update(self, edge, system_annotations=None, properties=None,
                    session=None):
        system_annotations = system_annotations or {}
        properties = properties or {}
        with self.session_scope(session) as local:
            for key, val in system_annotations.items():
                edge.system_annotations[key] = val
            edge.properties.update(properties)
            local.merge(edge)
        return edge

    def edge_lookup_one(self, src_id=None, dst_id=None, label=None,
                        voided=False, session=None):
        return self.edge_lookup(src_id, dst_id, label, voided, session)\
                   .scalar()

    def edge_lookup(self, src_id=None, dst_id=None, label=None,
                    voided=False, session=None):
        if voided:
            queries = [self.voided_edges()]
        elif label is not None:
            queries = [self.edges(cls) for cls in Edge._get_subclasses_labeled(label)]
        else:
            queries = [self.edges()]

        if src_id is not None:
            queries = [q.src(src_id) for q in queries]
        if dst_id is not None:
            queries = [q.dst(dst_id) for q in queries]
        if len(queries) > 1:
            return queries[0].union_all(*queries[1:])
        else:
            return queries[0]

    def edge_lookup_voided(self, src_id=None, dst_id=None, label=None,
                           session=None):
        return self.edge_lookup(src_id, dst_id, label, True, session)\
                   .scalar()

    def _edge_void(self, edge, session=None):
        raise NotImplemented('Deprecated.')

    def edge_delete(self, edge, session=None):
        with self.session_scope(session) as local:
            local.delete(edge)

    def edge_delete_by_node_id(self, node_id, session=None):
        with self.session_scope(session) as local:
            for edge in self.edges().filter(Edge.src_id == node_id):
                local.delete(edge)
            for edge in self.edges().filter(Edge.dst_id == node_id):
                local.delete(edge)

    def get_edge_by_labels(self, src_label, edge_label, dst_label):
        src_classes = [n for n in Node.get_subclasses()
                       if n.get_label() == src_label]
        dst_classes = [n for n in Node.get_subclasses()
                       if n.get_label() == dst_label]
        assert len(src_classes) == 1,\
            'No classes found with src_label {}'.format(src_label)
        assert len(dst_classes) == 1,\
            'No classes found with dst_label {}'.format(dst_label)
        edges = [edge for edge in Edge.get_subclasses()
                 if edge.__src_class__ == src_classes[0].__name__
                 and edge.__dst_class__ == dst_classes[0].__name__
                 and edge.get_label() == edge_label]
        assert len(edges) == 1,\
            'Expected 1 edge {}-{}->{}, found {}'.format(
                src_label, edge_label, dst_label, len(edges))
        return edges[0]

    def get_PsqlEdge(self, src_id=None, dst_id=None, label=None,
                     acl=[], system_annotations={}, properties={},
                     src_label=None, dst_label=None):
        Type = self.get_edge_by_labels(src_label, label, dst_label)
        return Type(
            src_id=src_id,
            dst_id=dst_id,
            properties=properties,
            acl=acl,
            system_annotations=system_annotations,
            label=label
        )

    def reload(self, *entities):
        reloaded = []
        for e in entities:
            if isinstance(e, Edge):
                reloaded.append(self.edges(type(e)).src(e.src_id)
                                .dst(e.dst_id).one())
            else:
                reloaded.append(self.nodes(type(e)).ids(e.node_id).one())
        return reloaded
