import abc
import copy
import logging
import random
import uuid
from collections import defaultdict, deque

import rstr


class Randomizer(object):
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
        super(EnumRand, self).__init__()
        self.values = values

    def random_value(self, override=None):
        if self.validate_value(override):
            return override
        return random.choice(self.values)

    def validate_value(self, value):
        return value in self.values


class NumberRand(Randomizer):
    def __init__(self, type_def):
        super(NumberRand, self).__init__()
        self.minimum = type_def.get('minimum', 0)
        self.maximum = type_def.get('maximum', 10000)

    def random_value(self, override=None):
        if self.validate_value(override):
            return override
        return random.randrange(self.minimum, self.maximum + 1)

    def validate_value(self, value):
        return isinstance(value, int) and \
                self.minimum <= value <= self.maximum


class StringRand(Randomizer):
    def __init__(self, type_def):
        super(StringRand, self).__init__()
        if type_def.get('format') == 'date-time':
            self.pattern = '201[0-9]-0[1-9]-[0-2][1-9]T00:00:00'
        else:
            self.pattern = type_def.get('pattern', '[A-Za-z0-9]{32}')

    def random_value(self, override=None):
        if self.validate_value(override):
            return override
        return rstr.xeger(self.pattern)

    def validate_value(self, value):
        if type(value).__name__ == 'unicode':
            value = str(value)
        return isinstance(value, str)


class BooleanRand(Randomizer):
    def random_value(self, override=None):
        if self.validate_value(override):
            return override
        return random.choice([True, False])

    def validate_value(self, value):
        return isinstance(value, bool)


class TypeRandFactory(object):
    @staticmethod
    def get_randomizer(type_def):
        _type = type_def.get('type')
        if isinstance(_type, list):
            _type = _type[0]

        if _type in ['object', 'array']:
            raise ValueError('Resolve relationships outside of this factory')

        if _type in ['integer', 'number', 'float']:
            return NumberRand(type_def)

        if _type in ['boolean']:
            return BooleanRand()

        return StringRand(type_def)


class PropertyFactory(object):
    def __init__(self, properties):
        self.properties = properties
        self.type_factories = {}
        for name in properties:
            try:
                self.type_factories[name] = self.resolve_type(
                    self.properties.get(name, {})
                )
            except ValueError as ve:
                logging.debug(
                    "Property: '{}' is most likely a relationship. Error: {}"
                    "".format(name, ve)
                )
                pass

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
            raise ValueError("Unknown property: '{}'".format(name))

        if name not in self.type_factories:
            raise ValueError(
                "No factory defined for property: '{}'. Most likely a "
                "relationship.".format(name)
            )

        return name, self.type_factories[name].random_value(override)

    def resolve_type(self, definition):
        if 'enum' in definition:
            return EnumRand(definition['enum'])

        if 'type' in definition:
            return TypeRandFactory.get_randomizer(definition)

        if 'oneOf' in definition:
            if 'enum' in definition['oneOf'][0]:
                values = [
                    v for d in definition['oneOf'] for v in d.get('enum', [])
                ]
                return EnumRand(values)
            return self.resolve_type(definition['oneOf'][0])

        if 'anyOf' in definition:
            return self.resolve_type(definition['anyOf'][0])

        return StringRand(definition)


class NodeFactory(object):
    def __init__(self, models, schema, graph_globals=None):
        self.models = models
        self.schema = schema
        self.property_factories = {
            label: PropertyFactory(node_def['properties'])
            for label, node_def in schema.items()
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
        if label not in self.schema:
            raise ValueError(
                "Node with label '{}' does not exist".format(label)
            )

        if not override:
            override = {}

        node_json = {
            'node_id': override.pop('node_id', str(uuid.uuid4())),
            'acl': override.pop('acl', []),
            'properties': {},
            'system_annotations': override.pop('system_annotations', {}),
        }

        if all_props:
            prop_list = self.schema[label].get('properties', [])
        else:
            prop_list = self.schema[label].get('required', [])

        for prop in prop_list:
            if prop == 'type':
                continue

            try:
                override_val = self.validate_override_value(
                    prop, label, override) or \
                    self.get_global_value(prop)

                _, value = self.property_factories[label].create(
                    prop, override_val)
            except (KeyError, ValueError):
                logging.debug("No factory for property: '{}'".format(prop))
                continue

            node_json['properties'][prop] = value

        node_cls = self.models.Node.get_subclass(label)

        return node_cls(**node_json)

    def get_global_value(self, prop):
        return self.graph_globals.get('properties', {}).get(prop)

    def validate_override_value(self, prop, label, override):
        # we allow specific passed values to override if they are valid
        if not override:
            return

        override_val = override.get(prop)
        try:
            if self.property_factories[label]. \
                    type_factories[prop].validate_value(override_val):
                return override_val
        except (KeyError, ValueError):
            # if this fails for whatever reason, we'll default to random value
            return


class GraphFactory(object):
    def __init__(self, models, dictionary, graph_globals=None):
        self.models = models
        self.dictionary = dictionary
        self.node_factory = NodeFactory(models, dictionary.schema,
                                        graph_globals)
        self.relation_cache = {}

    @staticmethod
    def validate_nodes_metadata(nodes, unique_key):
        for node_meta in nodes:
            if 'label' not in node_meta or unique_key not in node_meta:
                msg = (
                    "Node 'label' or unique property '{}' is missing: {}"
                    "".format(unique_key, node_meta)
                )
                raise ValueError(msg)

    @staticmethod
    def validate_edges_metadata(edges):
        for edge_meta in edges:
            if 'src' not in edge_meta or 'dst' not in edge_meta:
                raise ValueError(
                    "Edge metadata is missing 'src' or 'dst': {}".format(
                        edge_meta
                    )
                )

    def create_from_nodes_and_edges(self, nodes, edges,
                                    unique_key='submitter_id', all_props=False):
        """
        Given a list of nodes and edges, create a graph. The edge between 2
        nodes is based on property provided via `unique_key` param.

        :param nodes: list of nodes metadata in format:
            [{'label': 'read_group', 'submitter_id': 'read_group_1'},
             {'label': 'aliquot', 'submitter_id': 'aliquot_1'}]
        :type nodes: List[Dict]
        :param edges: list of edges in format:
            [{'src': 'read_group_1', 'dst': 'aliquot_1'}]
        :type edges: List[Dict]
        :param unique_key: a name of the property that will be used to connect
            nodes
        :type unique_key: String
        :param all_props: generate all node properties or not
        :type all_props: Boolean

        :return: List[psqlgraph.Node]
        """
        nodes = copy.deepcopy(nodes)

        self.validate_nodes_metadata(nodes, unique_key)

        self.validate_edges_metadata(edges)

        nodes_map = {}

        for node_meta in nodes:
            node_object = self.node_factory.create(node_meta.pop('label'),
                                                   override=node_meta,
                                                   all_props=all_props)
            nodes_map[node_object[unique_key]] = node_object

        for edge_meta in edges:
            sub_id1 = edge_meta['src']
            sub_id2 = edge_meta['dst']
            node1 = nodes_map.get(sub_id1)
            node2 = nodes_map.get(sub_id2)

            if not node1 or not node2:
                logging.debug(
                    "Could not find nodes for edge: '{}'<->'{}'".format(
                        sub_id1, sub_id2
                    )
                )
                continue

            self.make_association(node1, node2)

        return list(nodes_map.values())

    def create_random_subgraph(self, label, max_depth=10, leaf_labels=None,
                               skip_relations=None, all_props=False):
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
            leaf_labels = {'file', 'annotation'}
        else:
            leaf_labels = set(leaf_labels)

        skip_relations = set(skip_relations) if skip_relations else set()

        unique_key = 'submitter_id'

        # node adjacency map
        adj_set = defaultdict(set)
        # label to node objects map
        label_node_map = defaultdict(set)
        # submitter_id to node object map
        nodes_map = {}

        root = self.node_factory.create(label, all_props=all_props)

        if not hasattr(root, unique_key):
            unique_key = 'node_id'

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

                child_cls = edge_info['type']

                child_node = self.node_factory.create(
                    child_cls.get_label(), all_props=all_props)

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

        parent_links = self.dictionary.schema[label]['links']

        links = set()
        for parent_link in parent_links:
            if 'subgroup' not in parent_link:
                links.add(parent_link.get('name'))
                continue

            links |= {
                link.get('name') for link in parent_link['subgroup']
            }

        self.relation_cache[label] = links

        return relation in links

    @staticmethod
    def make_association(node1, node2):
        """
        Given 2 instances of a Node, find appropriate association between the
        2 nodes and create a relation between them

        :param node1: first node
        :type node1: psqlgraph.Node
        :param node2: second node
        :type node2: psqlgraph.Node
        :return: None
        """
        link_found = False
        for assoc_name, assoc_meta in node1._pg_edges.items():
            if isinstance(node2, assoc_meta['type']):
                getattr(node1, assoc_name).append(node2)
                link_found = True
                break

        if not link_found:
            logging.debug(
                "Could not find a direct relation between '{}'<->'{}'".format(
                    node1.label, node2.label
                )
            )
