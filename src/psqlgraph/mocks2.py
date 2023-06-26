import copy
import logging
import random
import uuid
from collections import defaultdict, deque
from typing import Dict, Iterable, List, Optional, Set

from psqlgraph import Node, mocks


class NodeFactory(mocks.NodeFactory):
    def __init__(self, models, graph_globals=None):
        self.models = models
        self.property_factories = {
            node_class.label: mocks.PropertyFactory(node_class._dictionary["properties"])
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


class GraphFactory(mocks.GraphFactory):
    def __init__(self, models, graph_globals=None):
        self.models = models
        self.node_factory = NodeFactory(models, graph_globals)
        self.relation_cache: Dict[str, Set[str]] = {}

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
