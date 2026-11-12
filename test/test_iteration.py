import pytest

from test import models


def test_node_is_not_iterable() -> None:
    node = models.Foo(node_id="first")

    with pytest.raises(TypeError):
        iter(node)
