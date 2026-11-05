import more_itertools

from test import models


def test_value_chain_nodes() -> None:
    first = models.Foo(node_id="first")
    second = models.Foo(node_id="second")

    assert list(more_itertools.value_chain(first, second)) == [first, second]
    assert list(more_itertools.value_chain(first, [second])) == [first, second]
