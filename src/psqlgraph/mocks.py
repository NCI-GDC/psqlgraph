import logging

from psqlgraph_test_utils import mocks

logger = logging.getLogger(__name__)
logger.warning(f"This module {__name__} is depreciated, please import {mocks.__name__}")

from psqlgraph_test_utils.mocks import GraphFactory, NodeFactory
