import pytest
from sqlalchemy import exc

from test import models


def test_param__with_read_only(pg_driver):
    with pytest.raises(exc.InternalError):
        with pg_driver.session_scope(read_only=True) as s:
            m1 = models.Foo(node_id="test")
            s.add(m1)


def test_nested_param__with_read_only(pg_driver):
    """Tests read only flag is applied within a nested session if parent is marked as read only"""

    with pytest.raises(exc.InternalError):
        with pg_driver.session_scope(read_only=True):

            with pg_driver.session_scope() as s:
                m1 = models.Foo(node_id="test")
                s.add(m1)


def test_nested_param__without_read_only(pg_driver):
    """Tests read only flag is not applied within a nested session that does not inherit"""

    with pg_driver.session_scope(read_only=True):
        with pg_driver.session_scope(can_inherit=False) as s:
            m1 = models.Foo(node_id="test")
            s.add(m1)


@pytest.mark.parametrize("inherits", [True, False])
def test_nested_param__inherit_read_only(pg_driver, inherits):
    """Tests read only flag is not applied within a nested session that does not inherit"""

    with pytest.raises(exc.InternalError):
        with pg_driver.session_scope(read_only=True):
            with pg_driver.session_scope(must_inherit=inherits) as s:
                m1 = models.Foo(node_id="test")
                s.add(m1)
