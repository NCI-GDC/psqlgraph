import uuid

import pytest
from sqlalchemy import exc as sqlalchemy_exc

import test
from psqlgraph import exc
from test import models


class TestPGProperties(test.PsqlgraphBaseTest):
    def test_select_property(self) -> None:
        """Ensures properties can be selected from."""
        with self.g.session_scope():
            self.g.node_insert(models.Foo(str(uuid.uuid4()), fobble=5))

            fobbles = tuple(f for (f,) in self.g.nodes(models.Foo.fobble))

            assert fobbles == (5,)

    def test_filter_property(self) -> None:
        """Ensures that filters using properties function as expected."""
        with self.g.session_scope():
            self.g.node_insert(models.Foo(str(uuid.uuid4()), fobble=5))
            self.g.node_insert(models.Foo(str(uuid.uuid4()), fobble=7))

            assert self.g.nodes(models.Foo).filter(models.Foo.fobble == 7).count() == 1

    def test_mixed_types(self) -> None:
        """Ensures that values with mixed types are read correctly and can be used in
        queries w/ casting.
        """
        with self.g.session_scope():
            self.g.node_insert(models.Foo(str(uuid.uuid4()), mixed_value=True))
            self.g.node_insert(models.Foo(str(uuid.uuid4()), mixed_value="very important"))

            assert frozenset(self.g.nodes(models.Foo.mixed_value)) == frozenset(
                ((True,), ("very important",))
            )
            assert (
                self.g.nodes(models.Foo)
                .filter(models.Foo.mixed_value.as_string() == "very important")
                .count()
                == 1
            )
            assert (
                self.g.nodes(models.Foo)
                .filter(models.Foo.mixed_value.as_string() == "true")
                .count()
                == 1
            )

            with pytest.raises(sqlalchemy_exc.DataError):
                self.g.nodes(models.Foo).filter(
                    models.Foo.mixed_value == "very important"
                ).one()

    def test_type_validation(self):
        """Ensures you cannot set property to the incorrect type."""
        with self.assertRaises(exc.ValidationError):
            models.Foo().fobble = "test"

    def test_enum_validation(self):
        n = models.Foo("foonode")
        n.baz = "allowed_1"
        n.baz = "allowed_2"

        with self.assertRaises(exc.ValidationError):
            n.baz = "not allowed"

    def test_list_contains_string(self):
        """Ensures that a basic contains filer works with a list of strings."""
        with self.g.session_scope():
            self.g.node_insert(models.Foo(str(uuid.uuid4()), studies=["math", "history"]))

            assert (
                self.g.nodes(models.Foo.node_id)
                .filter(models.Foo.studies.contains(["math"]))
                .count()
                == 1
            )
            assert (
                self.g.nodes(models.Foo.node_id)
                .filter(models.Foo.studies.contains(["history"]))
                .count()
                == 1
            )
            assert (
                self.g.nodes(models.Foo.node_id)
                .filter(models.Foo.studies.contains(["history", "science"]))
                .count()
                == 0
            )

    def test_list_contains_int(self):
        """Ensures that a basic contains filer works with a list of integers."""
        with self.g.session_scope():
            self.g.node_insert(models.Foo(str(uuid.uuid4()), ages=[40, 30]))

            assert (
                self.g.nodes(models.Foo.node_id).filter(models.Foo.ages.contains([40])).count()
                == 1
            )
