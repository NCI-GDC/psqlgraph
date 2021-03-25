import uuid

import pytest
from parameterized import parameterized

from psqlgraph import Edge, Node
from test import models, PsqlgraphBaseTest


def no_allowed_2_please(edge):
    if not isinstance(edge.src, models.Foo):
        return True

    node = edge.src
    if node.baz == "allowed_2":
        return False

    return True


def clean_tables(pg_driver):
    conn = pg_driver.engine.connect()
    conn.execute("commit")
    for table in Node().get_subclass_table_names():
        if table != Node.__tablename__:
            conn.execute("delete from {}".format(table))
    for table in Edge.get_subclass_table_names():
        if table != Edge.__tablename__:
            conn.execute("delete from {}".format(table))
    conn.execute("delete from _voided_nodes")
    conn.execute("delete from _voided_edges")
    conn.close()


@pytest.fixture
def fake_graph(pg_driver):
    clean_tables(pg_driver)

    yield pg_driver

    clean_tables(pg_driver)


@pytest.fixture
def fake_nodes(fake_graph):
    """
    Setting up a subgraph that we can later traverse.
    NOTE: Edge directions are as follows: Test->Test->Foo->FooBar and
    Test->FooBar, i.e. Foo/Test will be in FooBar's edges_in, Test will be
    in Foo's edges_in and Test will be in Test's edges_in

    Edges look like this:
    root_node <- foo1
    root_node <- foo2
    root_node <- foo3
    root_node <- test1
    foo1 <- test1
    foo1 <- test2
    foo2 <- test3
    test1 <- test4
    test2 <- test5

    """
    with fake_graph.session_scope() as session:
        root_node = models.FooBar(node_id="root", bar="root")

        foo1 = models.Foo(node_id="foo1", bar="foo1", baz="allowed_2")
        foo2 = models.Foo(node_id="foo2", bar="foo2", baz="allowed_1")
        foo3 = models.Foo(node_id="foo3", bar="foo3", baz="allowed_1")

        test1 = models.Test(node_id="", key1="test1")
        test2 = models.Test(node_id="test2", key1="test2")
        test3 = models.Test(node_id="test3", key1="test3")
        test4 = models.Test(node_id=str(uuid.uuid4()), key1="test4")
        test5 = models.Test(node_id="test5", key1="test5")

        root_node.tests.append(test1)

        for foo in [foo1, foo2, foo3]:
            root_node.foos.append(foo)

        for test in [test1, test2]:
            foo1.tests.append(test)

        test1.sub_tests.append(test4)

        foo2.tests.append(test3)

        test2.sub_tests.append(test5)

        session.add(root_node)

        nodes = {
            # These nodes should have the sysan_flag set, when predicate active
            "sysan_flag_nodes": [root_node, foo2, foo3, test1, test3, test4],
            # These nodes shouldn't have sysan_flag set, when predicate active
            "not_sysan_flag_nodes": [foo1, test2, test5],
            # These are expected nodes for a given depth
            "depths_results": {
                0: [root_node],
                1: [root_node, foo1, foo2, foo3, test1],
                2: [root_node, foo1, foo2, foo3, test1, test2, test3, test4],
                3: [root_node, foo1, foo2, foo3, test1, test2, test3, test4, test5],
            },
        }

        return nodes


@pytest.mark.parametrize("mode", ("bfs", "dfs"))
def test_default_traversal(fake_nodes, fake_graph, mode):
    """
    Default traversal should return all nodes
    """
    with fake_graph.session_scope():
        root = fake_graph.nodes(models.FooBar).first()
        traversal = {n.node_id for n in root.traverse(mode=mode)}

        nodes_all_set = {n.node_id for n in fake_graph.nodes().all()}

    assert traversal == nodes_all_set


@pytest.mark.parametrize("mode", ("bfs", "dfs"))
def test_default_traversal(fake_nodes, fake_graph, mode):
    """
    Traversal with predicate should return only self.sysan_flag_nodes
    """
    with fake_graph.session_scope():
        root = fake_graph.nodes(models.FooBar).first()

        gen = root.traverse(mode=mode, edge_predicate=no_allowed_2_please)
        traversal = {n.node_id for n in gen}

    expected_ids = {n.node_id for n in fake_nodes["sysan_flag_nodes"]}

    assert traversal == expected_ids


@pytest.mark.parametrize("depth", [0, 1, 2, 3])
@pytest.mark.parametrize("mode", ("bfs", "dfs"))
def test_traversal_with_max_depth(depth, fake_graph, fake_nodes, mode):
    """
    Traversal should return only self.depths_results[depth] nodes
    """
    with fake_graph.session_scope():
        root = fake_graph.nodes(models.FooBar).first()

        gen = root.traverse(mode=mode, max_depth=depth)
        traversal = [n for n in gen]

    expected_ids = {n.node_id for n in fake_nodes["depths_results"][depth]}
    traversal_ids = {n.node_id for n in traversal}
    # make sure that traversal size is as expected
    assert len(traversal) == len(fake_nodes["depths_results"][depth])
    # make sure the results of the traversal are as expected
    assert traversal_ids == expected_ids


@pytest.mark.parametrize("mode", ("bfs", "dfs"))
@pytest.mark.parametrize(
    "key,expected",
    (
        ("test5", ["test5", "test2", "foo1", "root"]),
        ("test3", ["test3", "foo2", "root"]),
    ),
)
def test_default_traversal(fake_nodes, fake_graph, mode, key, expected):
    """ Tests walking towards the root node from a leaf """
    with fake_graph.session_scope():
        leaf = fake_graph.nodes().props(key1=key).first()
        actual = [node.node_id for node in leaf.traverse(mode=mode, edge_pointer="out")]
        assert actual == expected
