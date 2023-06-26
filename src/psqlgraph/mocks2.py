import copy
import logging
import random
import uuid
from collections import defaultdict, deque
from typing import Dict, Iterable, List, Optional, Set

from psqlgraph import Node
from psqlgraph.mocks import PropertyFactory


class NodeFactory:
    def __init__(self, models, graph_globals=None):
        self.models = models
        self.property_factories = {
            node_class.label: PropertyFactory(node_class._dictionary["properties"])
            for node_class in models.Node.__subclasses__()
        }
        self.graph_globals = graph_globals or {}

    def create(self, label, override=None, all_props=False):
        """
        Create a minimal node of `label` type with only required properties
        being set. Override the values of properties by providing an `override`
        dictionary. If all properties should be set to some value, set
        `all_props` to True

        :param label: target node type
        :type label: String
        :param override: properties to override
        :type override: Dict
        :param all_props: set all properties to an arbitrary value or not
        :type all_props: Boolean

        :return: psqlgraph.Node object
        """
        node_class = self.models.Node.get_subclass(label)
        if not node_class:
            raise ValueError(f"Node with label '{label}' does not exist")

        if not override:
            override = {}

        node_json = {
            "node_id": override.pop("node_id", str(uuid.uuid4())),
            "acl": override.pop("acl", []),
            "properties": {},
            "system_annotations": override.pop("system_annotations", {}),
        }

        if all_props:
            prop_list = node_class._dictionary.get("properties", [])
        else:
            prop_list = node_class._dictionary.get("required", [])

        for prop in prop_list:
            # these two props are excluded during the real node creation
            # see `excluded_props` in gdcdatamodel/models/__init__.py
            if prop in ["id", "type"]:
                continue

            try:
                supplied_value = override.get(prop)
                override_val = (
                    supplied_value
                    if self.validate_override_value(prop, label, supplied_value)
                    else self.get_global_value(prop)
                )

                _, value = self.property_factories[label].create(prop, override_val)
            except (KeyError, ValueError):
                logging.debug(f"No factory for property: '{prop}'")
                continue

            node_json["properties"][prop] = value

        node_cls = self.models.Node.get_subclass(label)

        return node_cls(**node_json)

    def get_global_value(self, prop):
        return self.graph_globals.get("properties", {}).get(prop)

    def validate_override_value(self, prop, label, override):
        # we allow specific passed values to override if they are valid
        try:
            return self.property_factories[label].type_factories[prop].validate_value(override)
        except (KeyError, ValueError):
            # if this fails for whatever reason, we'll default to random value
            return False


class GraphFactory:
    def __init__(self, models, graph_globals=None):
        self.models = models
        self.node_factory = NodeFactory(models, graph_globals)
        self.relation_cache: Dict[str, Set[str]] = {}

    @staticmethod
    def validate_nodes_metadata(nodes, unique_key):
        for node_meta in nodes:
            if "label" not in node_meta or unique_key not in node_meta:
                msg = "Node 'label' or unique property '{}' is missing: {}" "".format(
                    unique_key, node_meta
                )
                raise ValueError(msg)

    @staticmethod
    def validate_edges_metadata(edges):
        for edge_meta in edges:
            if "src" not in edge_meta or "dst" not in edge_meta:
                raise ValueError(f"Edge metadata is missing 'src' or 'dst': {edge_meta}")

    def create_from_nodes_and_edges(
        self,
        nodes: List[Dict[str, str]],
        edges: List[Dict[str, str]],
        unique_key: str = "submitter_id",
        all_props: bool = False,
    ) -> List[Node]:
        """Create a graph from nodes and edges.

        Given a list of nodes and edges, create a graph. The edge between 2
        nodes is based on property provided via `unique_key` param.

        Args:
            nodes: list of nodes metadata in format:
                [{'label': 'read_group', 'submitter_id': 'read_group_1'},
                {'label': 'aliquot', 'submitter_id': 'aliquot_1'}]
            edges:  list of edges in format:
                [{'src': 'read_group_1', 'dst': 'aliquot_1'}]
            unique_key:  a name of the property that will be used to connect nodes
            all_props: generate all node properties or not

        Returns:
            List of psqlgraph nodes
        """
        nodes = copy.deepcopy(nodes)

        self.validate_nodes_metadata(nodes, unique_key)

        self.validate_edges_metadata(edges)

        nodes_map = {}

        for node_meta in nodes:
            node_object = self.node_factory.create(
                node_meta.pop("label"), override=node_meta, all_props=all_props
            )
            nodes_map[node_object[unique_key]] = node_object

        for edge_meta in edges:
            sub_id1 = edge_meta["src"]
            sub_id2 = edge_meta["dst"]
            edge_label = edge_meta.get("label")
            node1 = nodes_map.get(sub_id1)
            node2 = nodes_map.get(sub_id2)

            if not node1 or not node2:
                logging.debug(
                    "Could not find nodes for edge: '{}'<->'{}'".format(sub_id1, sub_id2)
                )
                continue

            self.make_association(node1, node2, edge_label)

        return list(nodes_map.values())

    def create_random_subgraph(
        self,
        label: str,
        max_depth: int = 10,
        leaf_labels: Optional[Iterable[str]] = None,
        skip_relations: Optional[Iterable[str]] = None,
        all_props: bool = False,
    ) -> List[Node]:
        """
        Generate a randomized graph with root at the given Node `label` type.

        NOTE: recommendation is to terminate on `file` and `annotation` type
        nodes and relationships as well, since those 2 can be linked to almost
        anything, which makes this randomized walk a nightmare.

        :param label: Node label, e.g. 'case', 'read_group', 'project' etc
        :type label: String
        :param max_depth: Maximum random walk depth
        :type max_depth: Integer
        :param leaf_labels: Node `label`s that will serve as leaf nodes
        :type leaf_labels: Iterable (List, Tuple, Set, you choose it and it will
            be converted to Set anyways)
        :param skip_relations: _pg_edges association name (e.g. `cases`), which
            will be completely skipped and not walked
        :type skip_relations: Iterable (see above)
        :param all_props: generate all node properties or not
        :type all_props: Boolean

        :return: List[psqlgraph.Node]
        """
        if not leaf_labels:
            leaf_labels = {"file", "annotation"}
        else:
            leaf_labels = set(leaf_labels)

        skip_relations = set(skip_relations) if skip_relations else set()

        unique_key = "submitter_id"

        # node adjacency map
        adj_set = defaultdict(set)
        # label to node objects map
        label_node_map = defaultdict(set)
        # submitter_id to node object map
        nodes_map = {}

        root = self.node_factory.create(label, all_props=all_props)

        if not hasattr(root, unique_key):
            unique_key = "node_id"

        label_node_map[root.label].add(root[unique_key])
        nodes_map[root[unique_key]] = root

        queue = deque([(root, 0)])

        while queue:
            curr_node, depth = queue.popleft()

            if depth + 1 > max_depth:
                continue

            if curr_node.label in leaf_labels:
                continue

            for relation, edge_info in curr_node._pg_edges.items():
                if relation in skip_relations:
                    continue

                # NOTE: Skipping edges going to the parents to avoid infinite
                # cycles
                if self.is_parent_relation(curr_node, relation):
                    continue

                # 80% of the time we will walk to children
                if random.randrange(5) == 0:
                    continue

                child_cls = edge_info["type"]

                child_node = self.node_factory.create(child_cls.get_label(), all_props=all_props)

                label_node_map[child_node.get_label()].add(child_node[unique_key])
                nodes_map[child_node[unique_key]] = child_node

                adj_set[curr_node[unique_key]].add(child_node[unique_key])
                adj_set[child_node[unique_key]].add(curr_node[unique_key])

                queue.append((child_node, depth + 1))

        for node_label, unique_key_set in label_node_map.items():
            # randomly merge half of the nodes of same type
            for _ in range(len(unique_key_set) // 2):
                unique_key1 = unique_key_set.pop()
                unique_key2 = unique_key_set.pop()

                # sub_id2 will be merged into sub_id1
                unique_key_set.add(unique_key1)

                # remove node with sub_id2
                nodes_map.pop(unique_key2)

                # remove adjacency set for sub_id2
                sub_id2_edges = adj_set.pop(unique_key2)

                # remove all references of sub_id2 from other adjacency sets
                for _, adj in adj_set.items():
                    if unique_key2 in adj:
                        adj.remove(unique_key2)

                # link sub_id2's neighbours to sub_id1
                for sub_id in sub_id2_edges:
                    adj_set[unique_key1].add(sub_id)
                    adj_set[sub_id].add(unique_key1)

        for src_id, dst_node_ids in adj_set.items():
            src_node = nodes_map.get(src_id)
            if not src_node:
                continue

            for dst_id in dst_node_ids:
                dst_node = nodes_map.get(dst_id)
                if not dst_node:
                    continue

                self.make_association(src_node, dst_node)

        return list(nodes_map.values())

    def is_parent_relation(self, node: Node, relation):
        """
        Given a relation name (e.g. `cases`), determine whether this relation
        is a link to a parent node (e.g. Sample.cases, `cases` is a parent link)

        :param node: Node
        :param relation: relation name (e.g. `cases`, `files` etc)
        :type relation: String
        :return: Boolean
        """
        if node.label in self.relation_cache:
            return relation in self.relation_cache[node.label]

        parent_links = node._dictionary["links"]

        links = set()
        for parent_link in parent_links:
            if "subgroup" not in parent_link:
                links.add(parent_link.get("name"))
                continue

            links |= {link.get("name") for link in parent_link["subgroup"]}

        self.relation_cache[node.label] = links

        return relation in links

    def make_association(
        self, src_node: Node, dst_node: Node, edge_label: Optional[str] = None
    ) -> None:
        """Create an Edge between 2 nodes

        Given 2 instances of a Node, find appropriate association between the
        2 nodes and create a relation between them

        There are some special cases like auxiliary_files and
        structural_variant_calling_workflow, there are 2 different edges between them
        in opposite directions. In this case, we use the label to differentiate them.

        Bonus: Label could be added to all edges and will make the lookup faster.

        Args:
            src_node: first node of the edge
            dst_node: second node of the edge
            edge_label: label of the edge
        """
        if edge_label:
            edge_class = self.models.Edge.get_subclass(edge_label)
            if not edge_class:
                logging.warning(f"Edge with label {edge_label} not found")
            elif (
                edge_class.__src_class__ == src_node.__class__.__name__
                and edge_class.__dst_class__ == dst_node.__class__.__name__
            ):
                getattr(src_node, edge_class.__src_dst_assoc__).append(dst_node)
                return
            elif (
                edge_class.__src_class__ == dst_node.__class__.__name__
                and edge_class.__dst_class__ == src_node.__class__.__name__
            ):
                getattr(dst_node, edge_class.__src_dst_assoc__).append(src_node)
                return
            else:
                logging.warning(
                    "Edge with label {} is not allowed between nodes {} and {}".format(
                        edge_label, src_node.label, dst_node.label
                    )
                )

        link_found = False
        for assoc_name, assoc_meta in src_node._pg_edges.items():
            if isinstance(dst_node, assoc_meta["type"]):
                getattr(src_node, assoc_name).append(dst_node)
                link_found = True
                break

        if not link_found:
            logging.debug(
                "Could not find a direct relation between '{}'<->'{}'".format(
                    src_node.label, dst_node.label
                )
            )
