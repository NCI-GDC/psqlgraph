from __future__ import annotations

import abc
import copy
import logging
import random
import uuid
from collections import defaultdict, deque
from collections.abc import Iterable

import rstr

import psqlgraph
from psqlgraph import Node
from psqlgraph.exc import PSQLGraphError

logger = logging.getLogger(__name__)


class Randomizer:
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.exhausted = False

    def __iter__(self):
        return self

    def next(self):
        if self.exhausted:
            raise StopIteration()

        return self.random_value()

    def exhaust(self):
        self.exhausted = True

    @abc.abstractmethod
    def random_value(self, override=None):
        pass

    @abc.abstractmethod
    def validate_value(self, value):
        pass


class EnumRand(Randomizer):
    def __init__(self, values):
        super().__init__()
        self.values = values

    def random_value(self, override=None):
        if self.validate_value(override):
            return override
        return random.choice(self.values)

    def validate_value(self, value):
        return value in self.values


class NumberRand(Randomizer):
    def __init__(self, type_def):
        super().__init__()
        self.minimum = type_def.get("minimum", 0)
        self.maximum = type_def.get("maximum", 10000)

    def random_value(self, override=None):
        if self.validate_value(override):
            return override
        return random.randrange(self.minimum, self.maximum + 1)

    def validate_value(self, value):
        return isinstance(value, int) and self.minimum <= value <= self.maximum


class StringRand(Randomizer):
    def __init__(self, type_def):
        super().__init__()
        if type_def.get("format") == "date-time":
            self.pattern = "201[0-9]-0[1-9]-[0-2][1-9]T00:00:00"
        else:
            self.pattern = type_def.get("pattern", "[A-Za-z0-9]{32}")

    def random_value(self, override=None):
        if self.validate_value(override):
            return override
        return rstr.xeger(self.pattern)

    def validate_value(self, value):
        return isinstance(value, str)


class BooleanRand(Randomizer):
    def random_value(self, override=None):
        if self.validate_value(override):
            return override
        return random.choice([True, False])

    def validate_value(self, value):
        return isinstance(value, bool)


class ArrayRand(Randomizer):
    def __init__(self, item_randomizer):
        super().__init__()
        self.item_randomizer = item_randomizer

    def random_value(self, override=None):
        if self.validate_value(override):
            return override

        return [self.item_randomizer.random_value() for _ in range(0, random.randint(1, 5))]

    def validate_value(self, value):
        return isinstance(value, list) and all(
            self.item_randomizer.validate_value(i) for i in value
        )


class TypeRandFactory:
    @staticmethod
    def get_randomizer(type_def):
        _type = type_def.get("type")
        if isinstance(_type, list):
            _type = _type[0]

        if _type in ["object"]:
            raise ValueError("Resolve relationships outside of this factory")

        if _type == "array":
            items = type_def.get("items")

            if not items:
                raise ValueError("Array configuration missing items.")

            return ArrayRand(TypeRandFactory.resolve_type(items))

        if _type in ["integer", "number", "float"]:
            return NumberRand(type_def)

        if _type in ["boolean"]:
            return BooleanRand()

        return StringRand(type_def)

    @staticmethod
    def resolve_type(definition):
        if "enum" in definition:
            return EnumRand(definition["enum"])

        if "type" in definition:
            return TypeRandFactory.get_randomizer(definition)

        if "oneOf" in definition:
            if "enum" in definition["oneOf"][0]:
                values = [v for d in definition["oneOf"] for v in d.get("enum", [])]
                return EnumRand(values)
            return TypeRandFactory.resolve_type(definition["oneOf"][0])

        if "anyOf" in definition:
            return TypeRandFactory.resolve_type(definition["anyOf"][0])

        return StringRand(definition)


class PropertyFactory:
    def __init__(self, properties):
        self.properties = properties
        self.type_factories = {}
        for name in properties:
            try:
                self.type_factories[name] = TypeRandFactory.resolve_type(
                    self.properties.get(name, {})
                )
            except ValueError as ve:
                logger.debug(f"Property: '{name}' is most likely a relationship. Error: {ve}")

    def create(self, name, override=None):
        """
        Given a property name, create a property-name, property-value pair.
        If override is provided, use override value as property-value

        :param name: property name
        :type name: String
        :param override: property value
        :type override: type
        :return: Tuple(str, type)
        """
        if name not in self.properties:
            raise ValueError(f"Unknown property: '{name}'")

        if name not in self.type_factories:
            raise ValueError(
                f"No factory defined for property: '{name}'. Most likely a relationship."
            )

        return name, self.type_factories[name].random_value(override)


class NodeFactory:
    def __init__(self, models, schema, graph_globals=None):
        self.models = models
        self.schema = schema
        self.property_factories = {
            label: PropertyFactory(node_def["properties"])
            for label, node_def in schema.items()
        }
        self.graph_globals = graph_globals or {}

    def create(
        self,
        label: str,
        override: dict[str, bool | int | str] | None = None,
        all_props: bool = False,
    ) -> psqlgraph.Node:
        """Create a node instance of `label` type.

        Values are autogenerated based on schema if not set in `override` parameter.

        Args:
            label: target node type label, e.g. case, aliquot
            override: Key value pairs containing custom values for node fields
            all_props: If True all properties are autogenerated
        Returns:
            psqlgraph.Node instance with the target type
        """
        if label not in self.schema:
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
            prop_list = self.schema[label].get("properties", [])
        else:
            prop_list = set(self.schema[label].get("required", []) + list(override.keys()))

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
                logger.debug(f"No factory for property: '{prop}'")
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
    def __init__(self, models, dictionary, graph_globals=None):
        self.models = models
        self.dictionary = dictionary
        self.node_factory = NodeFactory(models, dictionary.schema, graph_globals)
        self.relation_cache = {}

    @staticmethod
    def validate_nodes_metadata(nodes, unique_key):
        for node_meta in nodes:
            if "label" not in node_meta or unique_key not in node_meta:
                msg = f"Node 'label' or unique property '{unique_key}' is missing: {node_meta}"
                raise ValueError(msg)

    @staticmethod
    def validate_edges_metadata(edges):
        for edge_meta in edges:
            if "src" not in edge_meta or "dst" not in edge_meta:
                raise ValueError(f"Edge metadata is missing 'src' or 'dst': {edge_meta}")

    def create_from_nodes_and_edges(
        self,
        nodes: list[dict[str, str]],
        edges: list[dict[str, str]],
        unique_key: str = "submitter_id",
        all_props: bool = False,
        strict: bool = False,
    ) -> list[Node]:
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
            strict: raises error if invalid links are provided

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
                logger.debug(f"Could not find nodes for edge: '{sub_id1}'<->'{sub_id2}'")
                continue

            self.make_association(node1, node2, edge_label, strict)

        return list(nodes_map.values())

    def create_random_subgraph(
        self,
        label: str,
        max_depth: int = 10,
        leaf_labels: Iterable[str] | None = None,
        skip_relations: Iterable[str] | None = None,
        all_props: bool = False,
    ) -> list[Node]:
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
                if self.is_parent_relation(curr_node.label, relation):
                    continue

                # 80% of the time we will walk to children
                if random.randrange(5) == 0:
                    continue

                child_cls = edge_info["type"]

                child_node = self.node_factory.create(
                    child_cls.get_label(), all_props=all_props
                )

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

    def is_parent_relation(self, label, relation):
        """
        Given a relation name (e.g. `cases`), determine whether this relation
        is a link to a parent node (e.g. Sample.cases, `cases` is a parent link)

        :param label: Node `label`
        :type label: String
        :param relation: relation name (e.g. `cases`, `files` etc)
        :type relation: String
        :return: Boolean
        """
        if label in self.relation_cache:
            return relation in self.relation_cache[label]

        parent_links = self.dictionary.schema[label]["links"]

        links = set()
        for parent_link in parent_links:
            if "subgroup" not in parent_link:
                links.add(parent_link.get("name"))
                continue

            links |= {link.get("name") for link in parent_link["subgroup"]}

        self.relation_cache[label] = links

        return relation in links

    def make_association(
        self,
        src_node: Node,
        dst_node: Node,
        edge_label: str | None = None,
        strict: bool = False,
    ) -> None:
        """Create an Edge between two nodes

        Given 2 instances of a Node, find appropriate association between the
        two nodes and create a relation between them

        There are some special cases like auxiliary_files and
        structural_variant_calling_workflow, there are two different edges between them
        in opposite directions. In this case, we use the label to differentiate them.

        Bonus: Label could be added to all edges and will make the lookup faster.

        Args:
            src_node: source node of the edge
            dst_node: destination node of the edge
            edge_label: identifier defined in the dictionary `links` section. It can be
                the name of the edge, or the label
            strict: raise error is edge is invalid
        Raises:
            PSQLGraphError if either no association is found or multiple associations are found
            for the source and destination nodes. Specifying a label reduces the chances of
            finding multiple associations.
        """
        association_name = None
        if edge_label:
            # attempt to get association using link name - e.g., performed_on
            association_name = self.get_association_by_edge_label(
                src_node, dst_node, edge_label
            )

        if association_name:
            getattr(src_node, association_name).append(dst_node)
            return

        # attempt to get association by going through all links defined on the
        # source node if needed.
        association_names = self.get_association_by_edge_name(src_node, dst_node, edge_label)

        if len(association_names) == 1:
            getattr(src_node, association_names[0]).append(dst_node)

        elif len(association_names) > 1:
            if strict:
                raise PSQLGraphError(
                    f"Multiple associations '{association_names}' found between "
                    f"'{src_node.label}' and '{dst_node.label}'"
                )
            logger.warning(
                "Multiple association '%s' found between '%s' and '%s'",
                association_names,
                src_node.label,
                dst_node.label,
            )
        else:
            if strict:
                raise PSQLGraphError(
                    f"Could not find a direct relation (edge name or label = '{edge_label}') "
                    f"between '{src_node.label}' and '{dst_node.label}' "
                )
            logger.warning(
                "Could not find a direct relation (edge name or label = '%s') "
                "between '%s' and '%s'",
                edge_label,
                src_node.label,
                dst_node.label,
            )

            # try the reverse
            self.make_association(dst_node, src_node, edge_label, strict=True)

    def get_association_by_edge_name(
        self,
        src_node: psqlgraph.Node,
        dst_node: psqlgraph.Node,
        edge_name: str | None = None,
    ) -> list[str]:
        """Get the association name used to link the src and dst nodes
        Args:
            src_node: the source node
            dst_node: the destination node
            edge_name: the edge name as defined in the dictionary.
                If None, a unique association between the two nodes will be used.
                An exception is raised if no unique association is found.
        Returns:
            The name of the edge from the source node.
            For example, if the source node is case and the destination
            node is aliquot, the name of the edge from the `case` node is `aliquots`
        """
        association_names = []
        for assoc_name, assoc_meta in src_node._pg_edges.items():
            if edge_name and assoc_name != edge_name:
                continue

            if isinstance(dst_node, assoc_meta["type"]):
                association_names.append(assoc_name)
        return association_names

    def get_association_by_edge_label(
        self, src_node: psqlgraph.Node, dst_node: psqlgraph.Node, edge_label: str
    ) -> str | None:
        """Get association name for the unique combination of src and dst node

        Args:
            src_node: source node
            dst_node: destination node
            edge_label: dictionary defined edge label - see links section in the dictionary

        Returns:
            association name
        """
        logger.debug("Resolving association using edge labels")
        edge_class = self.models.Edge.get_unique_subclass(
            src_node.label, edge_label, dst_node.label
        )
        if edge_class:
            return edge_class.__src_dst_assoc__
        return None
