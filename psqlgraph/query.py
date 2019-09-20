# from edge import Edge, PsqlEdge, PsqlVoidedEdge

from copy import copy

import six
from sqlalchemy import not_, or_
from sqlalchemy.orm import Query

from psqlgraph.edge import Edge
from psqlgraph.node import Node

"""

"""


class GraphQuery(Query):
    """Query subclass implementing graph specific operations.

    .. |qobj| replace:: Returns a SQLAlchemy query object

    """

    def _iterable(self, val):
        if hasattr(val, '__iter__') and not isinstance(val, six.string_types):
            return val
        return val,

    def entity(self):
        """It is useful for us to be able to get the last entity in a chained
        join.  Therfore, if there are _join_entities on the query, the
        entity will be the last one in the chain.  If there is noo
        join in the query, then the entity is simply the specified
        entity.

        """

        return self._joinpoint_zero().entity

    # ======== Edges ========
    def with_edge_to_node(self, edge_type, target_node):
        """Filter query to nodes with edges to a given node

        :param edge_type:
            Edge model whose destination is `target_node`
        :param target_node:
            The node that is a neighbor to other nodes through edge
            `edge_type`
        :returns: |qobj|

        .. code-block:: python

            g.nodes().with_edge_to_node(Edge1, node1).filter(...

        """
        assert not isinstance(edge_type, str),\
            'Argument edge_type must be a subclass of Edge not a string'
        session = self.session
        sq = session.query(edge_type).filter(
            edge_type.dst_id == target_node.node_id).subquery()
        return self.filter(self.entity().node_id == sq.c.src_id)

    def with_edge_from_node(self, edge_type, source_node):
        """Filter query to nodes with edges from a given node

        :param edge_type:
            Edge model whose source is `target_node`
        :param source_node:
            The node that is a neighbor to other nodes through edge
            `edge_type`
        :returns: |qobj|

        .. code-block:: python

            g.nodes().with_edge_from_node(Edge1, node1).filter(...

        """
        assert not isinstance(edge_type, str),\
            'Argument edge_type must be a subclass of Edge not a string'
        session = self.session
        sq = session.query(edge_type).filter(
            edge_type.src_id == source_node.node_id).subquery()
        return self.filter(self.entity().node_id == sq.c.dst_id)

    def src(self, ids):
        """Filter edges by src_id

        :param ids:
            A list of ids or single id to filter on Edge.src_id == ids
        :returns: |qobj|

        .. code-block:: python

            g.nodes().src(node1.node_id).filter(...

        """
        if isinstance(ids, six.string_types):
            ids = [ids]

        assert hasattr(self.entity(), 'src_id')
        return self.filter(self.entity().src_id.in_(ids))

    def dst(self, ids):
        """Filter edges by dst_id

        :param ids:
            A list of ids or single id to filter on Edge.dst_id == ids
        :returns: |qobj|

        .. code-block:: python

            g.nodes().dst('id1').filter(...

        """
        if isinstance(ids, six.string_types):
            ids = [ids]

        assert hasattr(self.entity(), 'dst_id')
        return self.filter(self.entity().dst_id.in_(ids))

    # ====== Nodes ========
    def ids(self, ids):
        """Filter node by node_id

        :param ids:
            A list of ids or single id to filter on Node.node_id == ids
        :returns: |qobj|

        .. code-block:: python

            g.nodes().ids('id1').filter(...
            g.nodes().ids(['id1', 'id2']).filter(...

        """
        if isinstance(ids, six.string_types):
            ids = [ids]

        _id = self.entity().node_id
        return self.filter(_id.in_(ids))

    def not_ids(self, ids):
        """Filter node such that returned nodes do not have node_id

        :param ids:
            A list of ids or single id to filter on Node.node_id != ids
        :returns: |qobj|

        .. code-block:: python

            g.nodes().not_ids('id1').filter(...
            g.nodes().not_ids('id1').filter(...

        """

        _id = self.entity().node_id

        if isinstance(ids, six.string_types):
            ids = [ids]

        return self.filter(not_(_id.in_(ids)))

    # ======== Traversals ========
    def path(self, *paths):
        """Traverses a path in the graph given a list of AssociationProxy
        attributes

        :param paths:
            Either list of paths or a string with paths
            each separated by '.'
        :returns: |qobj|

        .. code-block:: python

            # The following are identical and filter for Nations who
            # have states who have cities who have streets named Main
            # St.
            g.nodes(Nation).path('states.cities.streets')\\
                           .props(name='Main St.')\\
                           .count()
            g.nodes(Nation).path('states', 'cities', 'streets')\\
                           .props(name='Main St.')\\
                           .count()

            # The following filters for nations that have states named
            # Illinois and at least one city and at least one street
            # named Main St.
            g.nodes(Nation).path('states')\\
                           .props(name='Illinois')\\
                           .path('cities.streets')\\
                           .props(name='Main St.')\\
                           .count()

            # The following filters on a forked path, i.e. a nation
            # with a democratic government and also a state with a
            # city with a street named Main St.
            g.nodes(Nation).path('governments')\\
                           .props(type='Democracy')\\
                           .reset_joinpoin()\\
                           .path('states.cities.streets')\\
                           .props(name='Main St.')\\
                           .count()

        .. note::
            In order to fork a path, you can append .reset_joinpoint()
            to the returned query.  This will set the join point back
            to the main entity, **but not** the filter point, i.e. any
            filter applied after .reset_joinpoint() will still attempt
            to filter on the last entity in .path(...).  In order to
            filter at the beginning of the path, simply filter before
            path, i.e. query.filter().path() not
            query.path().reset_joinpoint().filter()

        """
        entities = [p.strip() for path in paths for p in path.split('.')]
        assert self.entity() != Node,\
            'Please narrow your search by specifying a node subclass'
        for e in entities:
            self = self.join(*getattr(self.entity(), e).attr)
        return self

    @staticmethod
    def _get_link_details(entity, link_name):
        """"Lookup the (edge_class, left_edge_id, right_edge_id, node_class)
        for the given entity and link_name.

        param entity: The current entity Node subclass
        :param str link_name: The association proxy name

        edge_class: The Edge subclass the association is proxied through
        left_edge_id: The edge.{src,dst}_id
        right_edge_id: The edge.{dst,src}_id (opposit of left_edge_id)
        node_class: The target node the association_proxy points to

        """

        # Look for the link_name in OUTBOUND edges from the current
        # entity
        for edge in Edge._get_edges_with_src(entity.__name__):
            if edge.__src_dst_assoc__ == link_name:
                return (edge, edge.src_id, edge.dst_id,
                        Node.get_subclass_named(edge.__dst_class__))

        # Look for the link_name in INBOUND edges from the current
        # entity
        for edge in Edge._get_edges_with_dst(entity.__name__):
            if edge.__dst_src_assoc__ == link_name:
                return (edge, edge.dst_id, edge.src_id,
                        Node.get_subclass_named(edge.__src_class__))

        raise AttributeError(
            "type object '{}' has no attribute '{}'"
            .format(entity.__name__, link_name))

    def subq_path(self, path, filters=None, __recurse_level=0):
        """This function will performs very similarly to `path()`.  It emits a
        query, however, that is not base on `joins` but on sub queries.

        Passing filters: Because of the warning below, you must pass
        any filters you want to filter the end of this path traversal
        with as a list of functions. The function list is a stack,
        from which filters will be applied from the end of the path
        backwards.

        This example will filter on governments with presidents named Dave:

        ``g.nodes(Nation).subq_path('governments',
                  [lambda q: q.props(president='Dave')])``


        WARNING: Filters applied after calling this filter will be
        applied to the selection entity, not the end of the path.
        There is not join point.

        example:

        ``g.nodes(Nation).subq_path('governments').props(president='Dave')``

        will apply the `props` filter to `Nation`, not to `Government`.

        """

        if __recurse_level == 0:
            # we only want to mutate for recursive calls!
            filters = copy(filters)

        assert self.entity() != Node,\
            'Please narrow your search by specifying a node subclass'

        if not path:
            return self

        # Munge arguments to lists
        if isinstance(path, six.string_types):
            path = path.strip().split('.')
        if not isinstance(filters, list):
            filters = [filters]

        entity = self.entity()

        # Lookup link details
        link_name = path.pop(0)
        link_details = self._get_link_details(entity, link_name)
        edge, this_id, next_id, target_class = link_details

        # Construct the next recursive level's base query and recurse
        next_node_q = self.session.query(target_class)
        next_node_q = next_node_q.subq_path(path, filters, __recurse_level+1)

        # Pop a filter from the filter stack and apply if non-null
        if filters:
            f = filters.pop(0)
            if f is not None:
                next_node_q = f(next_node_q)
        next_node_sq = next_node_q.subquery()

        return self.filter(entity.node_id == this_id)\
                   .filter(next_id == next_node_sq.c.node_id)

    def subq_without_path(self, path, filters=None, __recurse_level=0):
        """This function is similar to ``subq_path`` but will filter for
        results that **do not** have the given path/filter combination

        """
        filters = filters or []
        return self.except_(self.subq_path(path, filters))

    def path_via_assoc_proxy(self, *entities):
        """Similar to :func:`path`, but more cumbersome.

        :param entities:
            A list of AssociationProxy entities to walk through.
        :returns: |qobj|

        .. code-block:: python

            # Filter for Nations who have states who have cities who
            # have streets named Main St.
            g.nodes(Nation).path_via_assoc_proxy(
                               Nation.states, States.cities, City.streets)\\
                           .props(name='Main St')\\
                           .count()

        """

        for entity in entities:
            self = self.join(*entity.attr)

        return self

    # ======== Properties ========
    def props(self, props=None, **kwargs):
        """Filter query results by properties.  Results in query will all
        contain given properties as a subset of _props.

        :param props:
            *Optional* A dictionary of properties which must be a
             subset of all result's properties.
        :param kwargs:
            This function also takes a list of key word arguments to
            include in the filter. This can be used in conjunction
            with `props`.
        :returns: |qobj|

        .. code-block:: python

            # The following all count the number of nodes with
            # key1 == True
            # key2 == 'Yes'
            g.props({'key1': True, 'key2': 'Yes'}).count()
            g.props(key1=True, key2='Yes').count()
            g.props({'key1': True}, key2='Yes').count()

        """
        props = props or {}
        assert isinstance(props, dict)
        kwargs.update(props)
        return self.filter(self.entity()._props.contains(kwargs))

    def not_props(self, props=None, **kwargs):
        """Filter query results by property exclusion. See :func:`props` for
        usage.

        :param props:
            *Optional* A dictionary of properties which must not be a
             subset of all result's properties.
        :param kwargs:
            This function also takes a list of key word arguments to
            include in the filter. This can be used in conjunction
            with `props`.
        :returns: |qobj|

        .. code-block:: python

            # Count the number of nodes with
            # key1 != True and key2 != 'Yes'
            g.props({'key1': True, 'key2': 'Yes'}).count()
            g.props(key1=True, key2='Yes').count()
            g.props({'key1': True}, key2='Yes').count()

        """
        props = props or {}
        assert isinstance(props, dict)
        kwargs.update(props)
        return self.filter(not_(self.entity()._props.contains(kwargs)))

    def null_props(self, keys=None, *args):
        """Filter query results by key, value pairs where either (a) the key
        is not present or (b) the key is present but the value is None

        This is necessary because a JSONB contains query (like
        `.props(key1=None)`) will emit a statement like

        .. code-block:: SQL

            select count(*) from node_test where _props @> '{"key1": null}'

        which will not match entries where `'key1'` is not present in
        the JSONB document.  This function will consider both cases
        (present but null/not preset)

        :param keys:
            A string or list of string keys to filter by null values
            or missing keys.  Additional keys can be added as
            :param:`*args`

        :param *args:
            A list of keys to filter by null values or missing keys.
            Additional keys can be added as :param:`keys`

        :returns: |qobj|

        .. code-block:: python

            # Count the number of nodes with null keys
            g.null_props('key1').count()
            g.null_props(['key1', 'key2']).count()
            g.null_props(['key1', 'key2', 'key3']).count()

        """

        keys = [keys] if isinstance(keys, six.string_types) else keys
        keys += args if args else []

        assert keys, 'No keys provided to `null_prop()` filter'

        for key in keys:
            self = self.filter(or_(self.entity()._props.contains({key: None}),
                not_(self.entity()._props.has_key(key))
            ))

        return self

    def prop_in(self, key, values):
        """Filter on entities that have a value corresponding to `key` that is
        in the list of keys `values`

        :param str key:
            Specifies which property to filter on.
        :param list values:
            The value in property `key` must be in `list`
        :returns: |qobj|


        .. code-block:: python

            g.prop_in('key1', ['Yes', 'yes', 'True', 'true']).count()

        """

        assert isinstance(key, six.string_types) and isinstance(values, list)
        return self.filter(self.entity()._props[key].astext.in_([
            str(v) for v in values]))

    def prop(self, key, value):
        """Filter query results by key value pair.

        :param str key:
            Specifies which property to filter on.
        :param value:
           The value in property `key` must be equal to `value`
        :returns: |qobj|

        .. code-block:: python

            g.prop('key1', True).count()

        """

        return self.filter(self.entity()._props.contains({key: value}))

    # ======== System Annotations ========
    def sysan(self, sysans=None, **kwargs):
        """Filter query results by system_annotations.  Results in query will
        all contain given properties as a subset of `system_annotations`.

        :param sysans:
            *Optional* A dictionary of annotations which must be a
             subset of all result's system_annotations.
        :param kwargs:
            This function also takes a list of key word arguments to
            include in the filter. This can be used in conjunction
            with `sysans`.
        :returns: |qobj|

        .. code-block:: python

            g.sysan({'key1': True, 'key2': 'Yes'})
            g.sysan(key1=True, key2='Yes')
            g.sysan({'key1': True}, key2='Yes')

        """

        sysans = sysans or {}
        assert isinstance(sysans, dict)
        kwargs.update(sysans)
        return self.filter(self.entity()._sysan.contains(kwargs))

    def not_sysan(self, sysans={}, **kwargs):
        """Filter query results by system_annotation exclusion. See
        :func:`sysan` for usage.

        :param sysans:
            *Optional* A dictionary of system_annotations which must not be a
             subset of all result's system_annotations.
        :param kwargs:
            This function also takes a list of key word arguments to
            include in the filter. This can be used in conjunction
            with `props`.
        :returns: |qobj|

        """

        assert isinstance(sysans, dict)
        kwargs.update(sysans)
        return self.filter(
            not_(self.entity()._sysan.contains(kwargs)))

    def has_sysan(self, keys):
        """Filter only entities that have a key `key` in system_annotations

        :param str key: System annotation key

        """
        if isinstance(keys, six.string_types):
            keys = [keys]
        for key in keys:
            self = self.filter(key in self.entity()._sysan)
        return self
