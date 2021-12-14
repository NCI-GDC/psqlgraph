import pytest
from sqlalchemy import exc

import psqlgraph
from test import models


@pytest.fixture(scope="module")
def pg_no_auto_flush(pg_conf, pg_driver):
    return psqlgraph.PsqlGraphDriver(auto_flush=False, **pg_conf)


@pytest.fixture(scope="module")
def pg_read_only(pg_conf, pg_driver):
    return psqlgraph.PsqlGraphDriver(read_only=True, **pg_conf)


def test_params__default_no_auto_flush(pg_no_auto_flush):
    """Tests default auto flush settings"""

    with pg_no_auto_flush.session_scope() as s:
        assert s.autoflush is False

    with pg_no_auto_flush.session_scope(auto_flush=True) as s:
        assert s.autoflush is True

    with pg_no_auto_flush.session_scope(auto_flush=False) as s:
        assert s.autoflush is False


def test_params__with_auto_flush(pg_driver):
    """Tests default auto flush settings"""

    with pg_driver.session_scope() as s:
        assert s.autoflush is True

    with pg_driver.session_scope(auto_flush=True) as s:
        assert s.autoflush is True

    with pg_driver.session_scope(auto_flush=False) as s:
        assert s.autoflush is False


def test_params__nested_auto_flush(pg_driver):
    """Tests inherited sessions cannot be changed"""

    with pg_driver.session_scope() as s:
        assert s.autoflush is True

        # not changing auto_flush for an inherited transaction
        with pg_driver.session_scope(auto_flush=False) as t:
            assert t.autoflush is True

        with pg_driver.session_scope(can_inherit=False, auto_flush=False) as t:
            assert t.autoflush is False


def test_reading__with_read_only(pg_driver, samples_with_array):
    """Tests querying is possible with read only flag set to true"""

    with pg_driver.session_scope(read_only=True):
        r = pg_driver.nodes(models.Foo).prop_in("fobble", [25]).count()
        assert r == 3


def test_param__with_read_only(pg_driver):
    with pytest.raises(exc.InternalError):
        with pg_driver.session_scope(read_only=True) as s:
            m1 = models.Foo(node_id="test")
            s.add(m1)


def test_pg_driver__default_read_only(pg_read_only):
    """Tests read only flag applied to the psqlgraph instance works with new sessions"""

    with pytest.raises(exc.InternalError):
        with pg_read_only.session_scope() as s:
            m1 = models.Foo(node_id="test")
            s.add(m1)


def test_pg_driver__override_default_read_only(pg_read_only):
    """Tests default read only flag is not applied within a session that specifies its own read only flag"""

    with pg_read_only.session_scope(read_only=False) as s:
        m1 = models.Foo(node_id="test")
        s.add(m1)

    # nested sessions
    with pg_read_only.session_scope():
        with pg_read_only.session_scope(can_inherit=False, read_only=False) as s:
            m1 = models.Foo(node_id="test2")
            s.add(m1)


def test_merge__with_read_only(pg_driver, samples_with_array):
    """Tests that merging fails for transactions marked as read only"""

    with pytest.raises(exc.InternalError):
        with pg_driver.session_scope(read_only=True) as s:
            nodes = pg_driver.nodes(models.Foo).prop_in("fobble", [25])
            for node in nodes:
                node.fobble += 1
                s.merge(node)


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
    """Tests read only flag is applied within a nested session that must inherit"""

    with pytest.raises(exc.InternalError):
        with pg_driver.session_scope(read_only=True):
            with pg_driver.session_scope(must_inherit=inherits) as s:
                m1 = models.Foo(node_id="test")
                s.add(m1)
