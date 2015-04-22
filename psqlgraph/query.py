# from edge import Edge, PsqlEdge, PsqlVoidedEdge
from node import Node
from edge import Edge
from sqlalchemy.orm import Query, joinedload
from sqlalchemy import or_, not_


class GraphQuery(Query):
    """Query subclass implementing graph specific operations."""

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

        if root:
            return self._entity_zero().type
        else:
            return self._joinpoint_zero().entity

    # ======== Edges ========
    def with_edge_to_node(self, edge_type, target_node):
        session = self.session
        sq = session.query(edge_type).filter(
            edge_type.dst_id == target_node.node_id).subquery()
        return self.filter(self.entity().node_id == sq.c.src_id)

    def with_edge_from_node(self, edge_type, source_node):
        session = self.session
        sq = session.query(edge_type).filter(
            edge_type.src_id == source_node.node_id).subquery()
        return self.filter(self.entity().node_id == sq.c.src_id)

    def src(self, ids):
        assert hasattr(self.entity(), 'src_id')
        if hasattr(ids, '__iter__'):
            return self.filter(Edge.src_id.in_(ids))
        else:
            return self.filter(Edge.src_id == str(ids))

    def dst(self, ids):
        assert hasattr(self.entity(), 'dst_id')
        if hasattr(ids, '__iter__'):
            return self.filter(Edge.dst_id.in_(ids))
        else:
            return self.filter(Edge.dst_id == str(ids))

    # ====== Nodes ========
    def ids(self, ids):
        _id = self.entity().node_id
        if hasattr(ids, '__iter__'):
            return self.filter(_id.in_(ids))
        else:
            return self.filter(_id == str(ids))

    def not_ids(self, ids):
        _id = self.entity().node_id
        if hasattr(ids, '__iter__'):
            return self.filter(not_(_id.in_(ids)))
        else:
            return self.filter(not_(_id == str(ids)))

    # ======== Traversals ========
    def path(self, *paths):
        """
        g.nodes(Participant).path('samples.aliquots.analytes').filter()
             -- or --
        g.nodes(Participant).path('samples', 'aliquots', 'analytes').filter()
        """
        entities = [p.strip() for path in paths for p in path.split('.')]
        assert self.entity() != Node,\
            'Please narrow your search by specifying a node subclass'
        for e in entities:
            self = self.join(*getattr(self.entity(), e).attr)
        return self

    def path2(self, *entities):
        """ g.nodes(Participant).path(
                Participant.samples, Sample.aliquots).filter()
        """
        for e in entities:
            self = self.join(*e.attr)
        return self

    # ======== Labels ========
    def labels(self, labels):
        entity = self.entity()
        if hasattr(labels, '__iter__'):
            return self.filter(entity._label.in_(labels))
        else:
            return self.filter(entity._label == labels)

    # ======== Properties ========
    def props(self, props={}, **kwargs):
        assert isinstance(props, dict)
        kwargs.update(props)
        return self.filter(self.entity()._props.contains(kwargs))

    def not_props(self, props={}, **kwargs):
        assert isinstance(props, dict)
        kwargs.update(props)
        return self.filter(not_(self.entity().properties.contains(kwargs)))

    def prop_in(self, key, values):
        assert isinstance(key, str) and isinstance(values, list)
        return self.filter(self.entity().properties[key].astext.in_([
            str(v) for v in values]))

    def prop(self, key, value):
        return self.filter(self.entity()._props.contains({key: value}))

    # ======== System Annotations ========
    def sysan(self, sysans={}, **kwargs):
        assert isinstance(sysans, dict)
        kwargs.update(sysans)
        return self.filter(self.entity()._sysan.contains(kwargs))

    def not_sysan(self, sysans=lambda: {}, **kwargs):
        assert isinstance(sysans, dict)
        kwargs.update(sysans)
        return self.filter(
            not_(self.entity().system_annotations.contains(kwargs)))

    def has_sysan(self, keys):
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            self = self.filter(self.entity()._sysan.has_key(key))
        return self
