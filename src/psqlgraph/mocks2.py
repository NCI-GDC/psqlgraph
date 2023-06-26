"""Second mocks for generating mock psqlgraph for testing.

The difference between this mocks2.py and mocks.py is that, this mocks2.py assumes
node links and node properties are in node_class._dictionary. So we do not have to
provide gdcdictioanry separately to generate nock graph.
"""
import logging
import uuid
from typing import Dict, Set

from psqlgraph import mocks


class NodeFactory(mocks.NodeFactory):
    """Class to create fake node for test.

    Given gdcdatamodel,
    Then call the create method with the label of the node you want to create,
    It will return the node with some random properties.
    """

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

        node_class = self.models.Node.get_subclass(label)

        parent_links = node_class._dictionary["links"]

        links = set()
        for parent_link in parent_links:
            if "subgroup" not in parent_link:
                links.add(parent_link.get("name"))
                continue

            links |= {link.get("name") for link in parent_link["subgroup"]}

        self.relation_cache[label] = links

        return relation in links
