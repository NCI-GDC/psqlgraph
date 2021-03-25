import uuid
from parameterized import parameterized

from test import models, PsqlgraphBaseTest


def no_allowed_2_please(edge):
    if not isinstance(edge.src, models.Foo):
        return True

    node = edge.src
    if node.baz == 'allowed_2':
        return False

    return True


class TestPsqlGraphTraversal(PsqlgraphBaseTest):

    def setUp(self):
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
        super(TestPsqlGraphTraversal, self).setUp()

        with self.g.session_scope() as session:
            root_node = models.FooBar(node_id="root", bar='root')

            foo1 = models.Foo(node_id="foo1", bar='foo1', baz='allowed_2')
            foo2 = models.Foo(node_id="foo2", bar='foo2', baz='allowed_1')
            foo3 = models.Foo(node_id="foo3", bar='foo3', baz='allowed_1')

            test1 = models.Test(node_id="", key1='test1')
            test2 = models.Test(node_id="test2", key1='test2')
            test3 = models.Test(node_id="test3", key1='test3')
            test4 = models.Test(node_id=str(uuid.uuid4()), key1='test4')
            test5 = models.Test(node_id="test5", key1='test5')

            root_node.tests.append(test1)

            for foo in [foo1, foo2, foo3]:
                root_node.foos.append(foo)

            for test in [test1, test2]:
                foo1.tests.append(test)

            test1.sub_tests.append(test4)

            foo2.tests.append(test3)

            test2.sub_tests.append(test5)

            session.add(root_node)

        # These nodes should have the sysan_flag set, when predicate active
        self.sysan_flag_nodes = [root_node, foo2, foo3, test1, test3, test4]
        # These nodes shouldn't have sysan_flag set, when predicate active
        self.not_sysan_flag_nodes = [foo1, test2, test5]
        # These are expected nodes for a given depth
        self.depths_results = {
            0: [root_node],
            1: [root_node, foo1, foo2, foo3, test1],
            2: [root_node, foo1, foo2, foo3, test1, test2, test3, test4],
            3: [root_node, foo1, foo2, foo3, test1, test2, test3, test4, test5]
        }

    def tearDown(self):
        print("tear down in progress")
        super(TestPsqlGraphTraversal, self).tearDown()


    def test_default_traversal(self):
        """
        Default traversal should return all nodes
        """
        with self.g.session_scope():
            root = self.g.nodes(models.FooBar).first()
            traversal = {n.node_id for n in root.bfs_children()}

            nodes_all_set = {n.node_id for n in self.g.nodes().all()}

        self.assertEqual(traversal, nodes_all_set)

    def test_traversal_with_predicate(self):
        """
        Traversal with predicate should return only self.sysan_flag_nodes
        """
        with self.g.session_scope():
            root = self.g.nodes(models.FooBar).first()

            gen = root.bfs_children(edge_predicate=no_allowed_2_please)
            traversal = {n.node_id for n in gen}

        expected_ids = {n.node_id for n in self.sysan_flag_nodes}
        self.assertEqual(expected_ids, traversal)

    @parameterized.expand([
        ('zero', 0),
        ('one', 1),
        ('two', 2),
        ('three', 3),
    ])
    def test_traversal_with_max_depth(self, _, depth):
        """
        Traversal should return only self.depths_results[depth] nodes
        """
        with self.g.session_scope():
            root = self.g.nodes(models.FooBar).first()

            gen = root.bfs_children(max_depth=depth)
            traversal = [n for n in gen]

        expected_ids = {n.node_id for n in self.depths_results[depth]}
        traversal_ids = {n.node_id for n in traversal}
        # make sure that traversal size is as expected
        self.assertEqual(len(self.depths_results[depth]), len(traversal))
        # make sure the results of the traversal are as expected
        self.assertEqual(expected_ids, traversal_ids)

    def test_directed_traversal(self):
        """ Tests walking towards the root node from a leaf """
        with self.g.session_scope():
            leaf = self.g.nodes().props(key1="test5").first()
            expected = ['test5', 'test2', 'foo1', 'root']
            actual = [node.node_id for node in leaf.traverse(edge_pointer="out")]
            self.assertListEqual(expected, actual)

            leaf = self.g.nodes().props(key1="test3").first()
            expected = ['test3', 'foo2', 'root']
            actual = [node.node_id for node in leaf.traverse(edge_pointer="out")]
            self.assertListEqual(expected, actual)