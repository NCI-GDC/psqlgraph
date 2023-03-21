from collections import defaultdict

from base import CommonBase, LocalConcreteBase
from sqlalchemy.ext import declarative

from psqlgraph import Edge, Node
from psqlgraph.edge import AbstractEdge
from psqlgraph.node import AbstractNode

BASE_CLASSES = defaultdict(dict)
BASE_CLASSES[None] = {"node": Node, "edge": Edge}

ORM_BASES = defaultdict(lambda: declarative.declarative_base(cls=CommonBase))


def get_orm_base(package_namespace):
    """Helper function to get the appropriate sqlalchemy base class
    Args:
       package_namespace (str): module namespace
    """
    return ORM_BASES[package_namespace]


def get_abstract_edge(package_namespace=None):
    return BASE_CLASSES[package_namespace]["edge"]


def get_abstract_node(package_namespace=None):
    return BASE_CLASSES[package_namespace]["node"]


def get_class_prefix(pkg_namespace):
    """Return an abstract class name prefix for the provided package
    Args:
        pkg_namespace (str): valid package name e.g gpas, bio
    Returns:
        str: class name prefix eg. AbstractGpas
    """
    return "{}".format(pkg_namespace.title())


def create_base_class(pkg_namespace, is_node=True):
    """Dynamically creates an abstract base class that extends either the Node or Edge class
    Args:
        pkg_namespace (str): package namespace
        is_node (bool): if True creates a node abstract class else creates an edge
    Returns:
        class: A dynamically generated abstract class
    """

    base_class = AbstractNode if is_node else AbstractEdge
    name = "{}{}".format(get_class_prefix(pkg_namespace), base_class.__name__)
    return type(name, (LocalConcreteBase, base_class, get_orm_base(pkg_namespace)), {})


def register_base_class(package_namespace=None):
    """Registers or returns a registered base node and edge classes as tuple for the package namespace
        Example:
            if package_namespace = `bio`
            This function will dynamically create and cache the following classes
                * `psqlgraph.ext.BioAbstractNode`
                * `psqlgraph.ext.BioAbstractEdge`
            Custom entities can now extend these class
            >>> N, E = register_base_class("bio")
            >>> class Node1(N):
            >>>    prop = sqlalchemy.Column(sqlalchemy.String)
            >>> class Edge1(E):
            >>>    prop = sqlalchemy.Column(sqlalchemy.TEXT)
    Args:
        package_namespace (str): If None, defaults to normal Node and Edge class
    Returns:
        tuple (class, class):
    """

    abstract_node = BASE_CLASSES[package_namespace].get("node")
    abstract_edge = BASE_CLASSES[package_namespace].get("edge")

    if abstract_node:
        return abstract_node, abstract_edge

    # dynamically create base classes
    abstract_node = create_base_class(package_namespace, is_node=True)
    abstract_edge = create_base_class(package_namespace, is_node=False)

    @classmethod
    def get_node_class(cls):
        return abstract_node

    @classmethod
    def get_edge_class(cls):
        return abstract_edge

    abstract_node.get_edge_class = get_edge_class
    abstract_edge.get_node_class = get_node_class

    globals()[abstract_node.__name__] = abstract_node
    globals()[abstract_edge.__name__] = abstract_edge

    BASE_CLASSES[package_namespace]["node"] = abstract_node
    BASE_CLASSES[package_namespace]["edge"] = abstract_edge

    return abstract_node, abstract_edge
