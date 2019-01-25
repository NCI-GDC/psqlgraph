import pytest

from psqlgraph import sanitize


def test_sanitize():
    props = dict(state="PASSED", versions=["a", "b"])
    sprops = sanitize(props)
    assert props["state"] == sprops["state"]
