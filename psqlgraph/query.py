from edge import Edge, PsqlEdge
from node import Node, PsqlNode
from sqlalchemy.orm import Query, joinedload, aliased
from sqlalchemy import or_, not_
from pprint import pprint


class GraphQuery(Query):
    """Query subclass implementing graph specific operations."""

    def _iterable(self, val):
        if hasattr(val, '__iter__'):
            return val
        else:
            return (val,)

    # ======== Edges ========
    def with_edge_to_node(self, edge_label, target_node):
        """Returns a new query that filters the query to just those nodes that
        have an edge with label ``edge_label`` to ``target_node``.

        :param str edge_label: label of the edge to use for restricting
        :param Node target_node: node used to filter the result set such
        that each result node must be the source of an edge whose
        destination is ``target_node``

        """

        assert self._entity_zero().type in [PsqlNode, Node]
        # first we construct a subquery for edges of the correct label
        # to the target_node
        session = self.session
        sq = session.query(Edge).filter(Edge.label == edge_label)\
                                .filter(Edge.dst_id == target_node.node_id)\
                                .subquery()
        return self.filter(Node.node_id == sq.c.src_id)

    def with_edge_from_node(self, edge_label, source_node):
        """Like ``with_edge_to_node``, but in the opposite direction.  Filters
        the query such that ``source_node`` must have an edge pointing
        to a node in the result set.

        :param str edge_label: label of the edge to use for
        restricting :param Node source_node: node used to filter
        the result set such that each result node must be the dst of an edge that
        originates at ``source_node``.

        """

        assert self._entity_zero().type in [PsqlNode, Node]
        # first we construct a subquery for edges of the correct label
        # to the target_node
        session = self.session
        sq = session.query(Edge).filter(Edge.label == edge_label)\
                                .filter(Edge.src_id == source_node.node_id)\
                                .subquery()
        return self.filter(Node.node_id == sq.c.dst_id)

    def src(self, src_id):
        return self.filter(Edge.src_id == src_id)

    def dst(self, dst_id):
        return self.filter(Edge.src_id == dst_id)

    # ======== Nodes ========
    def has_edges(self):
        """Adds filter to query to return only nodes with edges

        """
        return self.filter(
            or_(Node.edges_in.any(), Node.edges_out.any()))

    def has_no_edges(self):
        """Adds filter to query to return only nodes without edges

        """
        return self.\
            filter(not_(Node.edges_in.any())).\
            filter(not_(Node.edges_out.any()))

    def _path(self, labels, edges, node, reset=False):
        for label in self._iterable(labels):
            self = self.outerjoin(
                edges, node, aliased=True, from_joinpoint=True
            ).filter(Node.label == label)
        if reset:
            self = self.reset_joinpoint()
        return self

    def path_out(self, labels, reset=False):
        """Takes a list of labels and filters on nodes that have that path out
        from source node.  If `reset` is False, then this filter
        leaves you at the end of the path. You can at any point
        manually add a .reset_joinpoint to the query to bring it back
        to the path source.

        Returns a query pointing to the nodes at the beginning of the path.

        :param list labels: list of labels in path
        :param bool reset: reset the join point to the source node

        """
        return self._path(labels, 'edges_out', 'dst', reset)

    def path_in(self, labels, reset=False):
        """Takes a list of labels and filters on nodes that have that path into
        the source node.  If `reset` is False, then this filter
        leaves you at the end of the path. You can at any point
        manually add a .reset_joinpoint to the query to bring it back
        to the path source.

        Returns a query pointing to the nodes at the beginning of the path.

        :param list labels: list of labels in path
        :param bool reset: reset the join point to the source node

        """
        return self._path(labels, 'edges_in', 'src', reset)

    def path_in_end(self, labels):
        """Same as path_in, but forces the select statement to return with
        the entities from the end of the query

        Returns a query pointing to the nodes at the end of the path.

        """
        for label in self._iterable(labels):
            self = self.outerjoin(Edge, Node.node_id == Edge.dst_id)\
                       .outerjoin(Node, Edge.src_id == Node.node_id)\
                       .filter(Node.label == label)
        return self

    def path_out_end(self, labels):
        """Same as path_out, but forces the select statement to return with
        the entities from the end of the query

        Returns a query pointing to the nodes at the end of the path.

        """
        for label in self._iterable(labels):
            self = self.outerjoin(Edge, Node.node_id == Edge.src_id)\
                       .outerjoin(Node, Edge.dst_id == Node.node_id)\
                       .filter(Node.label == label)
        return self

    def path_end(self, labels):
        """Start at last query endpoint, walk a path through `labels` to
        destination nodes.

        .. note: This function will return the source node if your
        path includes the same label as the source node

        """
        for label in self._iterable(labels):
            self = self.outerjoin(Edge, or_(
                Node.node_id == Edge.src_id, Node.node_id == Edge.dst_id)
            ).outerjoin(Node, or_(
                Node.node_id == Edge.src_id, Node.node_id == Edge.dst_id)
            ).filter(Node.label == label)
        return self

    def ids_path_end(self, ids, labels):
        """Start at node with node_id and walk a path through `labels` to
        destination nodes.

        .. note: This function is particularly useful when walking
        from one known node to another with the same label.

        Returns a query pointing to the nodes at the end of the path.

        """
        self = self.ids(ids)
        for label in self._iterable(labels):
            self = self.outerjoin(Edge, or_(
                Node.node_id == Edge.src_id, Node.node_id == Edge.dst_id)
            ).outerjoin(Node, or_(
                Node.node_id == Edge.src_id, Node.node_id == Edge.dst_id)
            ).filter(
                Node.label == label
            ).filter(
                not_(Node.node_id.in_(self._iterable(ids))))
        return self

    def ids(self, ids):
        """Filter on nodes with either specific id, or nodes with ids in a
        provided list

        """
        if hasattr(ids, '__iter__'):
            return self.filter(Node.node_id.in_(ids))
        else:
            return self.filter(Node.node_id == str(ids))

    def neighbors(self):
        """Filter on nodes with either specific id, or nodes with ids in a
        provided list

        """
        self = self.outerjoin(Edge, or_(
            Node.node_id == Edge.src_id, Node.node_id == Edge.dst_id)
        ).outerjoin(Node, or_(
            Node.node_id == Edge.src_id, Node.node_id == Edge.dst_id))
        return self

    def load_edges(self):
        return self.options(joinedload(Node.edges_in))\
                   .options(joinedload(Node.edges_out))

    def load_neighbors(self):
        return self.options(
            joinedload(Node.edges_in).joinedload(Edge.src)
        ).options(
            joinedload(Node.edges_out).joinedload(Edge.dst))

    def _flatten_tree(self, tree):
        """Filter on nodes with either specific id, or nodes with ids in a
        provided list

        """
        nonleaves = [key for key in tree if tree[key]]
        if not nonleaves:
            return self
        n = self.load_neighbors().neighbors().labels(
            nonleaves).load_neighbors()
        for key in tree.keys():
            if tree[key]:
                self = self.union(
                    n._flatten_tree(tree[key])).load_neighbors()
        return self

    @staticmethod
    def _reconstruct_tree(node, nodes, doc, tree, visited):
        neighbors = [e.src for e in node.edges_in if e.src.label in tree] + \
                    [e.dst for e in node.edges_out if e.dst.label in tree]
        for node in neighbors:
            doc[node] = {}
            if tree[node.label]:
                GraphQuery._reconstruct_tree(
                    node, nodes, doc[node], tree[node.label], visited)
        return doc

    def tree(self, root_id, tree):
        """Filter on nodes with either specific id, or nodes with ids in a
        provided list

        """
        nodes = {n.node_id: n for n in self.ids(root_id)._flatten_tree(tree)}
        return {nodes[root_id]:
                self._reconstruct_tree(
                    nodes[root_id], nodes, {}, tree, [root_id])}

    def path_whole(self, path):
        """Filter on nodes with either specific id, or nodes with ids in a
        provided list

        """
        if not path:
            return self
        endpoint = self
        for label in self._iterable(path)[:-1]:
            endpoint = endpoint.load_neighbors().neighbors().labels(
                label).load_neighbors()
            self = self.union(endpoint)
        return self.union(
            endpoint.neighbors().labels(self._iterable(path)[-1]))

    def path_linked(self, src_id, path):
        tree = {}
        tree_temp = tree
        for label in path:
            tree_temp[label] = {}
            tree_temp = tree_temp[label]
        nodes = {n.node_id: n for n in self.ids(src_id)._flatten_tree(tree)}
        return {nodes[src_id]:
                self._reconstruct_tree(
                    nodes[src_id], nodes, {}, tree, None)}

    def not_ids(self, ids):
        if hasattr(ids, '__iter__'):
            return self.filter(not_(Node.node_id.in_(ids)))
        else:
            return self.filter(not_(Node.node_id == str(ids)))

    # ======== Labels ========
    def labels(self, labels):
        """With (Node.label in labels).  If label is type `str` then
        filter will check for equality.

        """
        entity = self._entity_zero().type
        if hasattr(labels, '__iter__'):
            return self.filter(entity.label.in_(labels))
        else:
            return self.filter(entity.label == str(labels))

    def not_labels(self, labels):
        """With (Node.label not in labels).  If label is type `str` then
        filter will check for equality.

        """
        entity = self._entity_zero().type
        if hasattr(labels, '__iter__'):
            return self.filter(not_(entity.label.in_(labels)))
        else:
            return self.filter(entity.label != labels)

    # ======== Properties ========
    def props(self, props):
        """With properties. Subset props in properties.

        :param dict props:
            kv dictionary to filter with. Results are nodes with
        properties which are superset of `props`

        """
        assert isinstance(props, dict)
        return self.filter(Node.properties.contains(props))

    def not_props(self, props):
        """Without properties. Subset props not in properties.

        :param dict props:
            kv dictionary to filter with. Results are nodes with
        properties which are not superset of `props`

        """
        assert isinstance(props, dict)
        return self.filter(not_(Node.properties.contains(props)))

    def prop_in(self, key, values):
        """With (properties[key] in values).

        :param str key: the property key
        :param list values: a list of possible properties

        """
        assert isinstance(key, str) and isinstance(values, list)
        return self.filter(Node.properties[key].astext.in_([
            str(v) for v in values]))

    def prop(self, key, value):
        """With (properties[key] in values).

        :param str key: the property key
        :param list values: a list of possible properties

        """
        return self.filter(Node.properties[key].astext == str(value))

    def has_props(self, keys):
        for key in self._iterable(keys):
            self = self.filter(Node.properties.has_key(key))
        return self

    def null_props(self, keys):
        """

        """
        return self.filter(Node.properties.contains(
            {key: None for key in self._iterable(keys)}))

    # ======== System Annotations ========
    def sysan(self, sysans):
        """With system_annotations. Subset sysans in system_annotations.

        """
        assert isinstance(sysans, dict)
        return self.filter(Node.system_annotations.contains(sysans))

    def not_sysan(self, sysans):
        """Without system_annotations. Subset sysans not in system_annotations.

        """
        assert isinstance(sysans, dict)
        return self.filter(not_(Node.system_annotations.contains(sysans)))

    def not_sysan_in(self, key, values):
        """With (system_annotations[key] in values).

        """
        assert isinstance(key, str) and isinstance(values, list)
        return self.filter(Node.system_annotations[key].astext.in_([
            str(v) for v in values]))

    def has_sysan(self, keys):
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            self = self.filter(
                self._entity_zero().type.system_annotations.has_key(key))
        return self
