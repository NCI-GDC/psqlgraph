# Driver to implement the graph model in postgres
#

from __future__ import annotations

import enum
import logging
import socket
from contextlib import contextmanager

import sqlalchemy
import xlocal
from sqlalchemy import event
from sqlalchemy.orm import configure_mappers, sessionmaker
from sqlalchemy.orm.attributes import flag_modified
from typing_extensions import Literal

from psqlgraph import ext, graph, poly, voided
from psqlgraph.exc import QueryError
from psqlgraph.hooks import receive_before_flush
from psqlgraph.query import GraphQuery
from psqlgraph.session import GraphSession

logger = logging.getLogger(__name__)

ENGINE_SCHEME = "postgresql+psycopg2"


class PostgresDriver(enum.Enum):
    """The currently supported postgres drivers for sqlalchemy (1.4/2.0)

    NOTE: Currently psqlgraph only supports PSYCOPG2.
    """

    PSYCOPG2 = "psycopg2"
    PSYCOPG = "psycopg"
    PG8000 = "pg8000"
    ASYNCPG = "asyncpg"


def engine_scheme(driver: PostgresDriver = PostgresDriver.PSYCOPG2) -> str:
    """Generates the engine scheme for the given driver.

    Args:
        driver: The driver which the `sqlalchemy.Engine` should use when interacting
            with the database.

    Returns:
        the url scheme for connecting to the database e.g. 'postgresql+psycopg2'
    """
    return f"postgresql+{driver.value}"


class PsqlGraphDriver:
    acceptable_isolation_levels = ["REPEATABLE_READ", "SERIALIZABLE"]

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        driver: PostgresDriver = PostgresDriver.PSYCOPG2,
        application_name: str | None = None,
        auto_flush: bool = True,
        connect_args: dict | None = None,
        isolation_level: Literal["REPEATABLE_READ", "SERIALIZABLE"] = "REPEATABLE_READ",
        package_namespace=None,
        read_only=False,
        set_flush_timestamps=True,
        **kwargs,
    ):
        """A driver for interacting with the graph represented in the package-namespace.

        Args:
            host: The name of the postgres host with which the driver should connect.
            user: The user name the driver should use when connecting with the postgres
                host.
            password: The password the driver should use for the given user.
            database: The name of the database backing the graph represented in the
                package namespace.
            driver: The postgres driver to use when making the connecting for to the
                database via the `sqlalchemy.Engine`.
            application_name: The name of this application by default will use the host
                name. See connection_args.
            auto_flush: Defaults to `True`; force all newly created sessions to set
                autoflush. This value will be the default autoflush value and used while
                creating new sessions. If the user passes a different value while
                creating the session, this value will be ignored.
            connection_args: The are the arguments which will be passed to
                `sqlalchemy.create_engine`. Within this, the application name will be set
                to the given value or its default.
            isolation_level: Set the isolation_level kwarg of the
                `sqlalchemy.create_engine` function. For more details, see its
                documentation. Will be superseded by any value of the same name within
                the kwargs.
            package_namespace: The namespace for the graph that backs this driver. This
                can be the default Edge/Node based graph provided in within `psqlgraph`
                or a Edge/Node pair generated in the `psqlgraph.ext` module e.g. `bio`.
            read_only: Defaults to `False`; controls whether new sessions are set to
                only allow read only queries or not. This value is used while creating
                new sessions and can be replaced by passing a different value while
                creating the session.
            set_flush_timestamps: Is `True` by default. Setting this to `True` will
                perform an extra database query to get the server time at flush and
                store `session._flush_timestamp`.
            kwargs: Any additional parameters which should be passed to
                `sqlalchemy.create_engine`. For details on what these are and their
                usage see their official documentation.
        """

        # Parse kwargs
        self.auto_flush = auto_flush
        self.read_only = read_only
        self.package_namespace = package_namespace
        self.set_flush_timestamps = set_flush_timestamps

        connect_args = connect_args or {}
        connect_args["application_name"] = application_name or socket.gethostname()

        kwargs.setdefault("isolation_level", isolation_level)

        if kwargs["isolation_level"] not in self.acceptable_isolation_levels:
            logger.warning(
                (
                    "Using an isolation level '{}' that is not in the list of "
                    "acceptable isolation levels {} is not safe and should be "
                    "avoided.  Doing this can result in one session overwriting "
                    "the commit of a concurrent session and losing data!"
                ).format(kwargs["isolation_level"], self.acceptable_isolation_levels)
            )

        # Create driver engine
        self.engine = sqlalchemy.create_engine(
            f"{engine_scheme(driver)}://{user}:{password}@{host}/{database}",
            encoding="latin1",
            connect_args=connect_args,
            **kwargs,
        )

        # Create context for xlocal sessions
        self.context = xlocal.xlocal()

    def _new_session(self, auto_flush=None, read_only=None):
        # use instance level value for auto_flush if nothing is passed
        auto_flush = self.auto_flush if auto_flush is None else auto_flush
        read_only = self.read_only if read_only is None else read_only

        Session = sessionmaker(
            autoflush=auto_flush, expire_on_commit=False, class_=GraphSession
        )
        Session.configure(bind=self.engine, query_cls=GraphQuery)
        session = Session(package_namespace=self.package_namespace)
        session._flush_timestamp = None
        session._set_flush_timestamps = self.set_flush_timestamps
        event.listen(session, "before_flush", receive_before_flush)

        if read_only:
            session.execute("SET TRANSACTION READ ONLY")

        return session

    def has_session(self):
        return hasattr(self.context, "session")

    def current_session(self):
        return self.context.session

    @contextmanager
    def session_scope(
        self,
        session=None,
        can_inherit=True,
        must_inherit=False,
        auto_flush=None,
        read_only=None,
    ):
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
        :param bool auto_flush:
            Enable/disable autoflush, defaults to True (self.auto_flush)
        :param bool read_only:
            Enforce a read only transaction, defaults to False (self.read_only)

        """

        if must_inherit and not self.has_session():
            raise RuntimeError(
                "Session scope requires it to be wrapped in a pre-existing "
                "session.  This was likely done to prevent a leaked session "
                "from a function which returns a query object."
            )

        # Set up local session
        inherited_session = True
        if session:
            local = session
        elif not (can_inherit and self.has_session()):
            inherited_session = False
            local = self._new_session(auto_flush, read_only)
        else:
            local = self.current_session()

        if inherited_session and (read_only is not None or auto_flush is not None):
            logger.warning(
                "Attempt to mark an inherited session with read_only={} or auto_flush={} will be ignored.".format(
                    read_only, auto_flush
                )
            )

        # Context manager functionality
        try:
            with self.context(session=local):
                yield local

            if not inherited_session:
                local.commit()

        except Exception as msg:
            logger.error(f"Rolling back session {msg}")
            local.rollback()
            raise

        finally:
            if not inherited_session:
                local.expunge_all()
                local.close()

    def nodes(self, query=None):
        """.. _nodes:"""
        query = query or ext.get_abstract_node(self.package_namespace)
        self._configure_driver_mappers()
        return self.__expand_query(query)

    def __call__(self, query=None):
        return self.nodes(query=query)

    def edges(self, query=None):
        query = query or ext.get_abstract_edge(self.package_namespace)
        self._configure_driver_mappers()
        return self.__expand_query(query)

    def _configure_driver_mappers(self):
        try:
            configure_mappers()
        except Exception as e:
            logger.error(
                "{}: Unable to configure mappers. "
                "Have you imported your models?".format(str(e))
            )

    def __expand_query(self, query=None):
        with self.session_scope(must_inherit=True) as local:
            if isinstance(query, list) or isinstance(query, tuple):
                return local.query(*query)
            else:
                return local.query(query)

    def voided_nodes(self, query=voided.VoidedNode):
        return self.__expand_query(query)

    def voided_edges(self, query=voided.VoidedEdge):
        return self.__expand_query(query)

    def get_nodes(self, batch_size=1000):
        return self.nodes().yield_per(batch_size)

    def get_edges(self, batch_size=1000):
        return self.edges().yield_per(batch_size)

    def get_node_count(self):
        return self.nodes().count()

    def get_edge_count(self):
        return self.edges().count()

    def node_merge(
        self,
        node_id=None,
        node=None,
        acl=None,
        label=None,
        system_annotations=None,
        properties=None,
    ):
        properties = properties or {}
        system_annotations = system_annotations or {}

        with self.session_scope() as local:
            if not node and not label:
                node = self.nodes().ids([node_id]).scalar()

            elif not node and label:
                cls = ext.get_abstract_node(self.package_namespace).get_subclass(label)
                node = self.nodes(cls).ids([node_id]).scalar()

            if not node:
                node = poly.poly_node(
                    node_id=node_id,
                    label=label,
                    acl=acl,
                    system_annotations=system_annotations,
                    properties=properties,
                )
            else:
                self.node_update(node, system_annotations, acl, properties)

            local.merge(node)

        return node

    def node_insert(self, node):
        with self.session_scope() as local:
            local.add(node)

    def node_update(self, node, system_annotations=None, acl=None, properties=None):
        properties = properties or {}
        system_annotations = system_annotations or {}

        with self.session_scope() as local:
            node.system_annotations.update(system_annotations)
            if acl is not None:
                node.acl = acl
            node.properties.update(properties)
            local.merge(node)

    def node_lookup(
        self,
        node_id=None,
        property_matches=None,
        label=None,
        system_annotation_matches=None,
        voided=False,
    ):
        if voided:
            query = self.voided_nodes()
        elif not label:
            query = self.nodes()
        else:
            cls = ext.get_abstract_node(self.package_namespace).get_subclass(label)
            query = self.nodes(cls)

        if node_id is not None:
            node_id = node_id.split(",") if isinstance(node_id, str) else node_id
            query = query.ids(node_id)
        if property_matches is not None:
            query = query.props(property_matches)
        if system_annotation_matches is not None:
            query = query.sysan(system_annotation_matches)
        return query

    def node_lookup_one(
        self,
        node_id=None,
        property_matches=None,
        label=None,
        system_annotation_matches=None,
        voided=False,
    ):
        return self.node_lookup(
            node_id, property_matches, label, system_annotation_matches, voided
        ).scalar()

    def node_lookup_by_id(self, node_id, voided=False):
        return self.node_lookup(node_id=node_id, voided=voided)

    def node_lookup_by_matches(
        self, property_matches=None, system_annotation_matches=None, voided=False
    ):
        return self.node_lookup(
            property_matches=property_matches,
            system_annotation_matches=system_annotation_matches,
            voided=voided,
        )

    def node_clobber(
        self,
        node_id=None,
        node=None,
        acl=None,
        system_annotations=None,
        properties=None,
        session=None,
    ):
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

    def node_delete_system_annotation_keys(
        self, system_annotation_keys, node_id=None, node=None, session=None
    ):
        with self.session_scope(session) as local:
            if not node:
                node = self.node_lookup_one(node_id=node_id)

            if not node:
                raise QueryError("Node not found")

            for key in system_annotation_keys:
                del node.system_annotations[key]

            flag_modified(node, "_sysan")
            local.merge(node)

    def node_delete(self, node_id=None, node=None, session=None):
        with self.session_scope(session) as local:
            local.flush()
            if node is None:
                node = self.node_lookup(node_id=node_id).one()
            local.delete(node)

    def edge_insert(self, edge, session=None):
        with self.session_scope(session) as local:
            local.flush()
            local.add(edge)
            local.flush()
        return edge

    def edge_update(self, edge, system_annotations=None, properties=None, session=None):
        system_annotations = system_annotations or {}
        properties = properties or {}
        with self.session_scope(session) as local:
            for key, val in system_annotations.items():
                edge.system_annotations[key] = val
            edge.properties.update(properties)
            local.merge(edge)
        return edge

    def edge_lookup_one(self, src_id=None, dst_id=None, label=None, voided=False):
        return self.edge_lookup(src_id, dst_id, label, voided).scalar()

    def edge_lookup(self, src_id=None, dst_id=None, label=None, voided=False):
        if voided:
            queries = [self.voided_edges()]
        elif label is not None:
            edge_cls = ext.get_abstract_edge(self.package_namespace)
            queries = [
                self.edges(cls) for cls in edge_cls._get_subclasses_labeled(label)
            ]
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

    def edge_lookup_voided(self, src_id=None, dst_id=None, label=None):
        return self.edge_lookup(src_id, dst_id, label, True).scalar()

    def edge_delete(self, edge, session=None):
        with self.session_scope(session) as local:
            local.delete(edge)

    def edge_delete_by_node_id(self, node_id, session=None):
        with self.session_scope(session) as local:
            edge_cls = ext.get_abstract_edge(self.package_namespace)
            for edge in self.edges().filter(edge_cls.src_id == node_id):
                local.delete(edge)
            for edge in self.edges().filter(edge_cls.dst_id == node_id):
                local.delete(edge)

    def get_edge_by_labels(self, src_label, edge_label, dst_label):
        node_cls = ext.get_abstract_node(self.package_namespace)
        src_classes = [
            n for n in node_cls.get_subclasses() if n.get_label() == src_label
        ]
        dst_classes = [
            n for n in node_cls.get_subclasses() if n.get_label() == dst_label
        ]
        assert len(src_classes) == 1, f"No classes found with src_label {src_label}"
        assert len(dst_classes) == 1, f"No classes found with dst_label {dst_label}"

        edge_cls = ext.get_abstract_edge(self.package_namespace)
        edges = [
            edge
            for edge in edge_cls.get_subclasses()
            if edge.__src_class__ == src_classes[0].__name__
            and edge.__dst_class__ == dst_classes[0].__name__
            and edge.get_label() == edge_label
        ]
        assert len(edges) == 1, "Expected 1 edge {}-{}->{}, found {}".format(
            src_label, edge_label, dst_label, len(edges)
        )
        return edges[0]

    def get_PsqlEdge(
        self,
        src_id=None,
        dst_id=None,
        label=None,
        acl=None,
        system_annotations=None,
        properties=None,
        src_label=None,
        dst_label=None,
    ):
        Type = self.get_edge_by_labels(src_label, label, dst_label)
        return Type(
            src_id=src_id,
            dst_id=dst_id,
            properties=properties or {},
            acl=acl or [],
            system_annotations=system_annotations or {},
            label=label,
        )

    def reload(self, *entities):
        reloaded = []
        for e in entities:
            if isinstance(e, graph.AbstractEdge):
                reloaded.append(self.edges(type(e)).src(e.src_id).dst(e.dst_id).one())
            else:
                reloaded.append(self.nodes(type(e)).ids(e.node_id).one())
        return reloaded
