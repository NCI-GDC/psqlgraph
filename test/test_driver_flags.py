"""Tests auto_flush and read_only flags are use correctly when applied to psqlgraph

   auto_flush and read_only flags can be applied via the PsqlGraphDriver constructor or when starting new
   transactions using ` with session_scope()`

   flags applied to the constructor can be overridden by applying the same flag while starting a transaction.
   Once a flag is applied to an active session, all child sessions will use the same flag values specified in the
   parent no matter what was passed in.
"""

from test import models

import pytest
from sqlalchemy import exc

import psqlgraph


@pytest.fixture(scope="module")
def pg_no_auto_flush(pg_conf, pg_driver):
    return psqlgraph.PsqlGraphDriver(auto_flush=False, **pg_conf)


@pytest.fixture(scope="module")
def pg_read_only(pg_conf, pg_driver):
    return psqlgraph.PsqlGraphDriver(read_only=True, **pg_conf)


def test_no_auto_flush_driver(pg_no_auto_flush):
    """Tests auto flush settings are applied appropriately"""

    # Uses value set by constructor
    with pg_no_auto_flush.session_scope() as s:
        assert s.autoflush is False

    # Nested still uses value set by constructor
    with pg_no_auto_flush.session_scope() as s:
        with pg_no_auto_flush.session_scope(can_inherit=False):
            assert s.autoflush is False

    # override default value
    with pg_no_auto_flush.session_scope(auto_flush=True) as s:
        assert s.autoflush is True


def test_auto_flush_driver(pg_driver):
    """Tests default auto flush settings"""

    # uses default constructor value
    with pg_driver.session_scope() as s:
        assert s.autoflush is True

    # override constructor value to True
    with pg_driver.session_scope(auto_flush=True) as s:
        assert s.autoflush is True

    # override constructor value to False
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


def test_read_only_driver__read(pg_driver, pg_read_only, samples_with_array):
    """Tests querying is possible with read only flag set to true"""

    with pg_driver.session_scope(read_only=True):
        r = pg_driver.nodes(models.Foo).prop_in("fobble", [25]).count()
        assert r == 3

    with pg_read_only.session_scope():
        r = pg_read_only.nodes(models.Foo).prop_in("fobble", [25]).count()
        assert r == 3


def test_read_only_driver__write_failure(pg_driver):
    """Tests read only sessions do not allow operations that write to the database"""
    with pytest.raises(exc.InternalError):
        with pg_driver.session_scope(read_only=True) as s:
            m1 = models.Foo(node_id="test")
            s.add(m1)


def test_read_only_driver__write_failure_with_default(pg_read_only):
    """Tests read only flag applied to the psqlgraph instance works with new sessions"""

    with pytest.raises(exc.InternalError):
        with pg_read_only.session_scope() as s:
            m1 = models.Foo(node_id="test")
            s.add(m1)


def test_read_only_driver__override_default(pg_read_only):
    """Tests default read only flag is not applied within a session that specifies its own read only flag"""

    with pg_read_only.session_scope(read_only=False) as s:
        m1 = models.Foo(node_id="test-r")
        s.add(m1)

    # nested sessions
    with pg_read_only.session_scope():
        with pg_read_only.session_scope(can_inherit=False, read_only=False) as s:
            m1 = models.Foo(node_id="test-r2")
            s.add(m1)


def test_read_only_driver__merge(pg_read_only, samples_with_array):
    """Tests that merging fails for transactions marked as read only"""

    with pytest.raises(exc.InternalError):
        with pg_read_only.session_scope() as s:
            nodes = pg_read_only.nodes(models.Foo).prop_in("fobble", [25])
            for node in nodes:
                node.fobble += 1
                s.merge(node)


def test_read_only_driver__nested_inherit(pg_read_only):
    """Tests read only flag is applied within a nested session if parent is marked as read only"""

    with pytest.raises(exc.InternalError):
        with pg_read_only.session_scope():

            with pg_read_only.session_scope() as s:
                m1 = models.Foo(node_id="test-n")
                s.add(m1)


def test_read_only_driver__nested(pg_read_only):
    """Tests read only flag is not applied within a nested session that does not inherit"""

    with pg_read_only.session_scope():
        # new session with read only override
        with pg_read_only.session_scope(can_inherit=False, read_only=False) as s:
            m1 = models.Foo(node_id="test")
            s.add(m1)


@pytest.mark.parametrize("inherits", [True, False])
def test_read_only_driver__nest_must_inherit(pg_read_only, inherits):
    """Tests read only flag is applied within a nested session that must inherit"""

    with pytest.raises(exc.InternalError):
        with pg_read_only.session_scope():
            with pg_read_only.session_scope(must_inherit=inherits) as s:
                m1 = models.Foo(node_id="test")
                s.add(m1)


def test_read_only_driver__delete(pg_read_only, samples_with_array):
    """Tests that delete fails for transactions marked as read only"""

    with pytest.raises(exc.InternalError):
        with pg_read_only.session_scope() as s:
            nodes = pg_read_only.nodes(models.Foo).prop_in("fobble", [25])
            for node in nodes:
                s.delete(node)
