# Driver to implement the graph model in postgres
#

# External modules
import logging
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from xlocal import xlocal
from node import Node
from util import retryable, default_backoff
from query import GraphQuery
from sqlalchemy import create_engine

DEFAULT_RETRIES = 0


class PsqlGraphDriver(object):

    def __init__(self, host, user, password, database,
                 node_validator=None, edge_validator=None):
        conn_str = 'postgresql://{user}:{password}@{host}/{database}'.format(
            user=user, password=password, host=host, database=database)
        self.engine = create_engine(conn_str)

        self.context = xlocal()

    def _new_session(self):
        Session = sessionmaker(expire_on_commit=False)
        Session.configure(bind=self.engine, query_cls=GraphQuery)
        session = Session()
        logging.debug('Created session {}'.format(session))
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

        2. Explicitly passing a session as `session` will cause driver
        calls within the session scope to use the explicitly passed
        session.

        3. Setting `can_inherit` to false will have no effect

        4. Setting `must_inherit` to will raise a RuntimeError

        .. note::
            A session scope that is nested has the following
            properties given `driver` is a PsqlGraphDriver instance:

        .. code-block:: python
            driver.session_scope() as A:
                driver.node_insert()  # uses session A
                driver.session_scope(A) as B:
                    B == A  # is True
                driver.session_scope() as C:
                    C == A  # is True
                driver.session_scope():
                    driver.node_insert()  # uses session A still
                driver.session_scope(can_inherit=False):
                    driver.node_insert()  # uses new session D
                driver.session_scope(can_inherit=False) as D:
                    D != A  # is True
                driver.session_scope() as E:
                    E.rollback()  # rolls back session A
                driver.session_scope(can_inherit=False) as F:
                    F.rollback()  # does not roll back session A
                driver.session_scope(can_inherit=False) as G:
                    G != A  # is True
                    driver.node_insert()  # uses session G
                    driver.session_scope(A) as H:
                        H == A; H != G  # are true
                        H.rollback()  # rolls back A but not G
                    driver.session_scope(A):
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
                logging.debug('Committing session {}'.format(local))
                local.commit()

        except Exception, msg:
            logging.error('Rolling back session {}'.format(msg))
            local.rollback()
            raise

        finally:
            if not inherited_session:
                local.expunge_all()
                local.close()

    def nodes(self, query=Node):
        with self.session_scope(must_inherit=True) as local:
            if isinstance(query, list) or isinstance(query, tuple):
                return local.query(*query)
            else:
                return local.query(query)

    def set_node_validator(self, node_validator):
        pass

    def set_edge_validator(self, edge_validator):
        pass

    def get_nodes(self, session=None, batch_size=1000):
        pass

    def edges(self, query=None):
        pass

    def get_edges(self, session=None, batch_size=1000):
        pass

    def get_node_count(self, session=None):
        pass

    def get_edge_count(self, session=None):
        pass

    def node_merge(self, node_id=None, node=None, acl=None,
                   label=None, system_annotations={}, properties={},
                   session=None, max_retries=DEFAULT_RETRIES,
                   backoff=default_backoff):

        if not node:
            node = Node(node_id, label, acl, system_annotations, properties)

        with self.session_scope(must_inherit=True) as local:
            local.merge(node)
            local.commit()
        print type(node._id)

    def node_insert(self, node, session=None):
        pass

    def node_update(self, node, system_annotations={},
                    acl=None, properties={}, session=None):
        pass

    def _node_void(self, node, session=None):
        pass

    def _node_lookup(self, node_id=None, property_matches=None,
                     label=None, system_annotation_matches=None,
                     voided=False, session=None):
        query = self.nodes()
        if node_id is not None:
            query = query.ids(node_id)
        return query

    def node_lookup_one(self, *args, **kwargs):
        return self._node_lookup(*args, **kwargs).scalar()

    def node_lookup(self, *args, **kwargs):
        return self._node_lookup(*args, **kwargs).all()

    def node_lookup_by_id(self, node_id, voided=False, session=None):
        pass

    def node_lookup_by_matches(self, property_matches=None,
                               system_annotation_matches=None,
                               label=None, voided=False, session=None):
        pass

    @retryable
    def node_clobber(self, node_id=None, node=None, acl=[],
                     system_annotations={}, properties={},
                     session=None, max_retries=DEFAULT_RETRIES,
                     backoff=default_backoff):
        pass

    @retryable
    def node_delete_property_keys(self, property_keys, node_id=None,
                                  node=None, session=None,
                                  max_retries=DEFAULT_RETRIES,
                                  backoff=default_backoff):
        pass

    @retryable
    def node_delete_system_annotation_keys(self,
                                           system_annotation_keys,
                                           node_id=None, node=None,
                                           session=None,
                                           max_retries=DEFAULT_RETRIES,
                                           backoff=default_backoff):
        pass

    @retryable
    def node_delete(self, node_id=None, node=None,
                    session=None, max_retries=DEFAULT_RETRIES,
                    backoff=default_backoff):
        pass

    def node_records(self, node_id):
        pass

    @retryable
    def edge_insert(self, edge, max_retries=DEFAULT_RETRIES,
                    backoff=default_backoff, session=None):
        pass

    def edge_update(self, edge, system_annotations={}, properties={},
                    session=None):
        pass

    def edge_lookup_one(self, src_id=None, dst_id=None, label=None,
                        voided=False, session=None):
        pass

    def edge_lookup(self, src_id=None, dst_id=None, label=None,
                    voided=False, session=None):
        pass

    def edge_lookup_voided(self, src_id=None, dst_id=None, label=None,
                           session=None):
        pass

    def _edge_void(self, edge, session=None):
        pass

    def edge_delete(self, edge, session=None):
        pass

    def edge_delete_by_node_id(self, node_id, session=None):
        pass
