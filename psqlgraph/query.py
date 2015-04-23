# from edge import Edge, PsqlEdge, PsqlVoidedEdge
from node import Node
from edge import Edge
from sqlalchemy.orm import Query, joinedload
from sqlalchemy import or_, not_

"""

"""


class GraphQuery(Query):
    """Query subclass implementing graph specific operations.

    .. |qobj| replace:: Returns a SQLAlchemy query object

    """

    def _iterable(self, val):
        if hasattr(val, '__iter__'):
            return val
        else:
            return (val,)

    def entity(self, root=False):
        """It is useful for us to be able to get the last entity in a chained
        join.  Therfore, if there are _join_entities on the query, the
        entity will be the last one in the chain.  If there is noo
        join in the query, then the entity is simply the specified
        entity.

        """

        if root or not hasattr(self._joinpoint_zero(), 'entity'):
            return self._entity_zero().type
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
        :param target_node:
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

        assert hasattr(self.entity(), 'src_id')
        if hasattr(ids, '__iter__'):
            return self.filter(Edge.src_id.in_(ids))
        else:
            return self.filter(Edge.src_id == str(ids))

    def dst(self, ids):
        """Filter edges by dst_id

        :param ids:
            A list of ids or single id to filter on Edge.dst_id == ids
        :returns: |qobj|

        .. code-block:: python

            g.nodes().dst('id1').filter(...

        """

        assert hasattr(self.entity(), 'dst_id')
        if hasattr(ids, '__iter__'):
            return self.filter(Edge.dst_id.in_(ids))
        else:
            return self.filter(Edge.dst_id == str(ids))

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

        _id = self.entity().node_id
        if hasattr(ids, '__iter__'):
            return self.filter(_id.in_(ids))
        else:
            return self.filter(_id == str(ids))

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
        if hasattr(ids, '__iter__'):
            return self.filter(not_(_id.in_(ids)))
        else:
            return self.filter(not_(_id == str(ids)))

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
            self = self.join(*getattr(self.entity(), e).attr, aliased=True)
        return self

    def path2(self, *entities):
        """Similar to :func:`path`, but more cumbersome.

        :param entities:
            A list of AssociationProxy entities to walk through.
        :returns: |qobj|

        .. code-block:: python

            # Filter for Nations who have states who have cities who
            # have streets named Main St.
            g.nodes(Nation).path(Nation.states, States.cities, City.streets)\\
                           .props(name='Main St')\\
                           .count()

        """
        for e in entities:
            self = self.join(*e.attr)
        return self

    # ======== Labels ========
    def labels(self, labels):
        """Filters on nodes that have certain labels.

        :param labels:
           A list of labels or a single scalar string.  The filtered
           results will all have label in `labels`
        :returns: |qobj|

        .. note::
           This is largely **deprecated**, rather you should specify the
           actual model class entity you want to return when you begin
           your query, e.g. driver.nodes(TestNode)

        """

        entity = self.entity()
        if hasattr(labels, '__iter__'):
            return self.filter(entity._label.in_(labels))
        else:
            return self.filter(entity._label == labels)

    # ======== Properties ========
    def props(self, props={}, **kwargs):
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

        assert isinstance(props, dict)
        kwargs.update(props)
        return self.filter(self.entity()._props.contains(kwargs))

    def not_props(self, props={}, **kwargs):
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

        assert isinstance(props, dict)
        kwargs.update(props)
        return self.filter(not_(self.entity()._props.contains(kwargs)))

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

        assert isinstance(key, str) and isinstance(values, list)
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
    def sysan(self, sysans={}, **kwargs):
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
            not_(self.entity().system_annotations.contains(kwargs)))

    def has_sysan(self, keys):
        """Filter only entities that have a key `key` in system_annotations

        :param str key: System annotation key

        """
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            self = self.filter(self.entity()._sysan.has_key(key))
        return self
