import warnings

from psqlgraph import hydrator

warnings.warn(
    f"This module {__name__} is deprecated, please import {hydrator.__name__}", DeprecationWarning
)

from psqlgraph.hydrator import GraphFactory, NodeFactory
