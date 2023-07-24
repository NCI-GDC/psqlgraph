import logging

from psqlgraph import hydrator

logger = logging.getLogger(__name__)
logger.warning(f"This module {__name__} is depreciated, please import {hydrator.__name__}")

from psqlgraph.hydrator import GraphFactory, NodeFactory
