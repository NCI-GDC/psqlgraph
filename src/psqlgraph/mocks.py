import logging

from psqlgraph_test_utils import mocks

logger = logging.getLogger(__name__)
logger.warning(f"This module is depreciated, please import from {mocks.__package__}")

from psqlgraph_test_utils.mocks import *
