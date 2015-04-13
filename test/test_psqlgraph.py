import uuid
import unittest
import logging
import psqlgraph
import random
from psqlgraph import PsqlGraphDriver
from psqlgraph.node import PsqlNode, Node, PolyNode
from multiprocessing import Process
from sqlalchemy.exc import IntegrityError
from psqlgraph.exc import ValidationError, EdgeCreationError

from datetime import datetime
from copy import deepcopy

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


logging.basicConfig(level=logging.DEBUG)

# We have to import models here, even if we don't use them
from models import TestNode


class PsqlEdge(object):

    def __init__(self, *args, **kwargs):
        pass


def sanitize(d):
    return d


class TestPsqlGraphDriver(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger(__name__)
        self.driver = PsqlGraphDriver(host, user, password, database)
        self.REPEAT_COUNT = 200
        self._clear_tables()

    def tearDown(self):
        self.driver.engine.dispose()

    def _clear_tables(self):
        conn = self.driver.engine.connect()
        conn.execute('commit')
        for table in Node.get_subclass_table_names():
            if table != Node.__tablename__:
                conn.execute('delete from {}'.format(table))
        conn.execute('delete from {}'.format(Node.__tablename__))
        conn.close()

    @unittest.skip('not implemented')
    def test_getitem(self):
        """Test that indexing nodes/edges accesses their properties"""
        node = PsqlNode(node_id=str(uuid.uuid4()),label="foo", properties={"bar": 1})
        self.assertEqual(node["bar"], 1)
        edge = PsqlEdge(src_id=None, dst_id=None, label="foo", properties={"bar": 1})
        self.assertEqual(edge["bar"], 1)

    @unittest.skip('not implemented')
    def test_setitem(self):
        """Test that indexing nodes/edges accesses their properties"""
        node = PsqlNode(node_id=str(uuid.uuid4()),label="foo", properties={"bar": 1})
        node["bar"] = 2
        self.assertEqual(node["bar"], 2)
        edge = PsqlEdge(src_id=None, dst_id=None, label="foo", properties={"bar": 1})
        edge["bar"] = 2
        self.assertEqual(edge["bar"], 2)

    def test_long_ints_roundtrip(self):
        """Test that integers that only fit in 26 bits round trip correctly."""
        with self.driver.session_scope():
            node = PolyNode(node_id=str(uuid.uuid4()),label="foo",
                            properties={"bar": 9223372036854775808})
            self.driver.node_insert(node=node)
            loaded = self.driver.node_lookup(node_id=node._id).one()
            self.assertEqual(loaded["bar"], 9223372036854775808)

    def test_node_null_label_merge(self):
        """Test merging of a non-existent node

        Verify the case where a user merges a single non-existent node
        """
        self.assertRaises(
            KeyError,
            self.driver.node_merge,
            node_id=str(uuid.uuid4()))

    def test_node_null_query_one(self):
        """Test querying of a single non-existent node

        Verify the case where a user queries for a single non-existent node
        """

        with self.driver.session_scope():
            node = self.driver.node_lookup_one(str(uuid.uuid4()))
            self.assertTrue(node is None)

    def test_node_null_query(self):
        """Test querying for any non-existent nodes

        Verify the case where a user queries for any non-existent nodes
        """

        with self.driver.session_scope():
            node = list(self.driver.node_lookup(str(uuid.uuid4())))
            self.assertEqual(node, [])
            node = self.driver.node_lookup_one(str(uuid.uuid4()))
            self.assertTrue(node is None)

    def verify_node_count(self, count, node_id=None, matches=None,
                          voided=False):
        """Test querying for the count on a non-existent node

        Verify the case where a user queries a count for non-existent nodes
        """
        with self.driver.session_scope():
            nodes = list(self.driver.node_lookup(
                node_id=node_id,
                property_matches=matches,
                voided=False))
            if voided:
                voided_nodes = list(self.driver.node_lookup(
                    node_id=node_id,
                    property_matches=matches,
                    voided=True))
                nodes = list(nodes) + list(voided_nodes)
            self.assertEqual(len(nodes), count, 'Expected a {n} nodes to '
                             'be found, instead found {count}'.format(
                                 n=count, count=len(nodes)))
            return nodes

    def test_node_merge_and_lookup(self):
        """Test node merge and lookup

        Insert a single node and query, compare that the result of the
        query is correct
        """
        tempid = str(uuid.uuid4())
        properties = {'key1': None, 'key2': 2, 'key3': datetime.now()}
        with self.driver.session_scope() as local:
            self.driver.node_merge(node_id=tempid, label='test',
                                   properties=properties)
        with self.driver.session_scope():
            node = self.driver.node_lookup_one(tempid)
        self.assertEqual(properties, node.properties)

    @unittest.skip('not implemented')
    def test_node_update_updates_acls(self):
        tempid = str(uuid.uuid4())
        with self.driver.session_scope():
            node = self.driver.node_merge(node_id=tempid, label='test')
            self.driver.node_update(node, acl=["somebody"])
        with self.driver.session_scope():
            node = self.driver.nodes().ids([tempid]).one()
            self.assertEqual(node.acl, ["somebody"])
        self.driver.node_merge(node_id=node.node_id, acl=[])
        with self.driver.session_scope():
            node = self.driver.nodes().ids([tempid]).one()
            self.assertEqual(node.acl, [])

    @unittest.skip('not implemented')
    def test_node_update_properties_by_id(self, given_id=None, label=None):
        """Test updating node properties by ID

        Insert a single node, update it, verify that

        (1) The first insertion is successful
        (2) The update is successful
        (3) The transaction of the update is maintained
        (4) There is only a single version of the node
        (5) The voided node is a snapshot of the previous version.
        """

        node_id = str(uuid.uuid4()) if not given_id else given_id

        if not label:
            label = 'test'

        retries = 0 if not given_id else int(1e6)
        # Add first node
        propertiesA = {'key1': None, 'key2': 1, 'key3': datetime.now()}
        self.driver.node_merge(node_id=node_id, properties=propertiesA,
                               label=label, max_retries=retries)

        # Add second node
        propertiesB = {'key1': 2, 'new_key': 'n', 'timestamp': datetime.now()}
        self.driver.node_merge(node_id=node_id, properties=propertiesB,
                               max_retries=retries)

        # Merge properties
        merged = deepcopy(propertiesA)
        for key, val in propertiesB.iteritems():
            merged[key] = val

        if not given_id:
            # Test that there is only 1 non-void node with node_id and property
            # equality
            # if this is not part of another test, check the count
            with self.driver.session_scope():
                node = self.driver.node_lookup_one(node_id)
                self.assertEqual(sanitize(merged), node.properties)
                voided_node = self.driver.node_lookup(
                    node_id, voided=True).one()
                self.assertEqual(sanitize(propertiesA), voided_node.properties)
            self.verify_node_count(2, node_id=node_id, voided=True)

        return merged

    @unittest.skip('not implemented')
    def test_query_by_label(self, node_id=None):
        """Test ability to query for nodes by label"""

        label = 'test_' + str(random.random())
        for i in range(self.REPEAT_COUNT):
            self.driver.node_merge(
                node_id=str(uuid.uuid4()), label=label)
        with self.driver.session_scope():
            nodes = list(self.driver.node_lookup(label=label))
        self.assertEqual(len(nodes), self.REPEAT_COUNT)

    @unittest.skip('not implemented')
    def test_node_update_properties_by_matches(self):
        """Test updating node properties by matching properties

        Insert a single node, update it, verify that

        (1) The first insertion is successful
        (2) The update is successful
        (3) The transaction of the update is maintained
        (4) There is only a single version of the node
        """

        node_id = str(uuid.uuid4())

        a = random.random()
        b = random.random()

        # Add first node
        propertiesA = {'key1': a, 'key2': str(a), 'key3': 12345}
        self.driver.node_merge(node_id=node_id, label='test',
                               properties=propertiesA)

        # Add second node
        propertiesB = {'key1': b, 'key4': str(b)}
        with self.driver.session_scope():
            node = self.driver.node_lookup_one(property_matches=propertiesA)
        self.driver.node_merge(node=node, label='test',
                               properties=propertiesB)

        # Merge properties
        merged = deepcopy(propertiesA)
        for key, val in propertiesB.iteritems():
            merged[key] = val

        with self.driver.session_scope():
            node = self.driver.node_lookup_one(property_matches=propertiesB)
            self.assertEqual(merged, node.properties)
            node = self.driver.node_lookup_one(node_id=node_id)
            self.assertEqual(merged, node.properties)

        nodes = self.verify_node_count(2, node_id=node_id, voided=True)
        self.assertEqual(propertiesA, nodes[1].properties)

        return merged

    @unittest.skip('not implemented')
    def test_node_update_system_annotations_id(self, given_id=None):
        """Test updating node system annotations ID

        Insert a single node, update it, verify that

        (1) The first insertion is successful
        (2) The update is successful
        (3) The transaction of the update is maintained
        (4) There is only a single version of the node
        """

        node_id = str(uuid.uuid4()) if not given_id else given_id

        # Add first node
        system_annotationsA = {
            'key1': None, 'key2': 2, 'key3': datetime.now()
        }
        self.driver.node_merge(node_id=node_id, label='test',
                               system_annotations=system_annotationsA)

        # Add second node
        system_annotationsB = {
            'key1': None, 'new_key': 2, 'timestamp': datetime.now()
        }
        self.driver.node_merge(node_id=node_id, label='test',
                               system_annotations=system_annotationsB)

        # Merge system_annotations
        merged = deepcopy(system_annotationsA)
        for key, val in system_annotationsB.iteritems():
            merged[key] = val

        # if this is not part of another test, check the count
        if not given_id:
            # Test that there is only 1 non-void node with node_id and property
            # equality
            with self.driver.session_scope():
                node = self.driver.node_lookup_one(node_id)
            self.assertEqual(sanitize(merged),
                             node.system_annotations)

            nodes = list(self.verify_node_count(
                2, node_id=node_id, voided=True))
            self.assertEqual(sanitize(system_annotationsA),
                             nodes[1].system_annotations)

        return merged

    @unittest.skip('not implemented')
    def _insert_node(self, node):
        """Test inserting a node"""
        with self.driver.session_scope() as session:
            session.add(node)

    @unittest.skip('not implemented')
    def test_node_unique_id_constraint(self):
        """Test node constraints on unique ID

        Verify that the table constraints prevent the existance two
        non-voided nodes with the same id
        """

        tempid = str(uuid.uuid4())

        # Add first node
        propertiesA = {'key1': None, 'key2': 2, 'key3': datetime.now()}
        self.driver.node_merge(node_id=tempid, label='test',
                               properties=propertiesA)

        propertiesB = {'key1': None, 'key2': 2, 'key3': datetime.now()}
        bad_node = psqlgraph.PsqlNode(
            node_id=tempid,
            system_annotations={},
            acl=[],
            label=None,
            properties=propertiesB
        )

        self.assertRaises(IntegrityError, self._insert_node, bad_node)

    @unittest.skip('not implemented')
    def test_null_node_void(self):
        """Test voiding of a null node

        Verify that the library handles null nodes properly on voiding
        """

        self.assertRaises(
            psqlgraph.ProgrammingError,
            self.driver._node_void,
            None
        )

    @unittest.skip('not implemented')
    def test_null_node_merge(self):
        """Test merging of a null node

        Verify that the library handles null nodes properly on merging
        """
        self.assertRaises(psqlgraph.QueryError, self.driver.node_merge)

    @unittest.skip('not implemented')
    def test_repeated_node_update_properties_by_id(self, given_id=None):
        """Test repeated node updating to properties by ID

        Verify that repeated updates to a single node create
        the correct number of voided transactions and a single valid
        node with the correct properties
        """

        node_id = str(uuid.uuid4()) if not given_id else given_id

        for tally in range(self.REPEAT_COUNT):
            props = self.test_node_update_properties_by_id(node_id)

        if not given_id:
            self.verify_node_count(self.REPEAT_COUNT*2, node_id=node_id,
                                   voided=True)
            with self.driver.session_scope():
                node = self.driver.node_lookup_one(node_id)
            self.assertEqual(sanitize(props), node.properties)

    @unittest.skip('not implemented')
    def test_repeated_node_update_system_annotations_by_id(self,
                                                           given_id=None):
        """Test repeated node updates to system annotations by ID

        Verify that repeated updates to a single node create

        the correct number of voided transactions and a single valid
        node with the correct properties
        """

        REPEAT_COUNT = 200
        node_id = str(uuid.uuid4()) if not given_id else given_id

        for tally in range(REPEAT_COUNT):
            annotations = self.test_node_update_system_annotations_id(node_id)

        if not given_id:
            self.verify_node_count(REPEAT_COUNT*2, node_id=node_id,
                                   voided=True)
            with self.driver.session_scope():
                node = self.driver.node_lookup_one(node_id)
            self.assertEqual(sanitize(annotations), node.system_annotations)

    @unittest.skip('not implemented')
    def test_sessioned_node_update(self):
        """Test repeated update of a sessioned node

        Repeate test_repeated_node_update but passing a single session for
        all interactions to use

        """

        label = str(uuid.uuid4())
        node_ids = [str(uuid.uuid4()) for i in range(self.REPEAT_COUNT)]
        properties = {}
        with self.driver.session_scope() as session:
            for node_id in node_ids:
                properties[node_id] = {
                    'key1': node_id,
                    'key2': 2,
                    'key3': str(datetime.now()),
                    'rand':  random.random()
                }

                self.driver.node_merge(
                    node_id=node_id,
                    label='test',
                    properties=properties[node_id],
                    session=session,
                )
            nodes = list(self.driver.node_lookup(label=label))

        for node in nodes:
            self.assertEqual(
                sanitize(properties[node.node_id]), node.properties,
                'Node properties do not match expected properties'
            )

    @unittest.skip('not implemented')
    def test_concurrent_node_update_by_id(self):
        """Test concurrent node updating by ID

        Test that insertion of nodes is thread-safe and that retries succeed
        eventually
        """

        process_count = 3
        tempid = str(uuid.uuid4())
        processes = []

        for tally in range(process_count):
            processes.append(Process(
                target=self.test_repeated_node_update_properties_by_id,
                kwargs={'given_id': tempid})
            )

        for p in processes:
            p.start()

        for p in processes:
            p.join()

        self.verify_node_count(process_count*self.REPEAT_COUNT*2, tempid,
                               voided=True)

    @unittest.skip('not implemented')
    def test_node_clobber(self):
        """Test that clobbering a node replaces all of its properties"""

        tempid = str(uuid.uuid4())

        propertiesA = {'key1':  None, 'key2':  3, 'key3':  'test'}
        {'key1':  None, 'key2':  2, 'key3':  datetime.now()}
        self.driver.node_merge(node_id=tempid, label='test',
                               properties=propertiesA)

        propertiesB = {'key1':  True, 'key2':  0, 'key3':  'test'}
        self.driver.node_clobber(node_id=tempid, properties=propertiesB)

        propertiesB = sanitizer.sanitize(propertiesB)
        with self.driver.session_scope():
            nodes = list(self.driver.node_lookup(node_id=tempid))
        self.assertEqual(len(nodes), 1,
                         'Expected a single node to be found, instead found '
                         '{count}'.format(count=len(nodes)))
        self.assertEqual(propertiesB, nodes[0].properties,
                         'Node properties do not match expected properties')

    @unittest.skip('not implemented')
    def test_node_delete_property_keys(self):
        """Test the ability to remove property keys from nodes"""

        tempid = str(uuid.uuid4())
        properties = {'key1':  None, 'key2':  2, 'key3':  'test'}
        self.driver.node_merge(node_id=tempid, label='test',
                               properties=properties)

        self.driver.node_delete_property_keys(['key2', 'key3'], node_id=tempid)
        properties.pop('key2')
        properties.pop('key3')

        with self.driver.session_scope():
            nodes = self.driver.node_lookup_one(node_id=tempid)
        self.assertEqual(properties, nodes.properties)

    @unittest.skip('not implemented')
    def test_node_delete_system_annotation_keys(self):
        """Test the ability to remove system annotation keys from nodes"""

        tempid = str(uuid.uuid4())
        annotations = {'key1':  None, 'key2':  2, 'key3':  'test'}
        self.driver.node_merge(node_id=tempid, label='test',
                               system_annotations=annotations)

        self.driver.node_delete_system_annotation_keys(
            ['key2', 'key3'], node_id=tempid)

        annotations.pop('key2')
        annotations.pop('key3')

        with self.driver.session_scope():
            nodes = list(self.driver.node_lookup(node_id=tempid))
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, '
                         'instead found {count}'.format(count=len(nodes)))
        self.assertEqual(annotations, nodes[0].system_annotations)

    @unittest.skip('not implemented')
    def test_node_delete(self):
        """Test node deletion"""

        tempid = str(uuid.uuid4())
        self.driver.node_merge(node_id=tempid, label='test')
        self.driver.node_delete(node_id=tempid)

        with self.driver.session_scope():
            nodes = list(self.driver.node_lookup(tempid))
        self.assertEqual(len(nodes), 0, 'Expected a no non-voided nodes '
                         'to be found, instead found {count}'.format(
                             count=len(nodes)))

    @unittest.skip('not implemented')
    def test_query_then_node_delete(self):
        """Test querying for a node then deleting it."""
        node1 = self.driver.node_merge(node_id=str(uuid.uuid4()), label='test')
        with self.driver.session_scope():
            for node in self.driver.node_lookup(label="test").all():
                self.driver.node_delete(node=node)
        with self.driver.session_scope():
            nodes = self.driver.node_lookup(node1.node_id).all()
        self.assertEqual(len(nodes), 0, 'Expected a no non-voided nodes '
                         'to be found, instead found {count}'.format(
                             count=len(nodes)))

    @unittest.skip('not implemented')
    def test_repeated_node_delete(self):
        """Test repeated node deletion correctness"""

        node_id = str(uuid.uuid4())
        for i in range(self.REPEAT_COUNT):
            self.test_node_update_properties_by_id(node_id)
            self.driver.node_delete(node_id=node_id)
            with self.driver.session_scope():
                self.assertIs(
                    self.driver.node_lookup_one(node_id=node_id), None)

    @unittest.skip('not implemented')
    def test_edge_insert_null_label(self):
        """Test merging of a null edge

        Verify the case where a user merges a single non-existent node
        """

        self.assertRaises(
            EdgeCreationError,
            PsqlEdge,
            str(uuid.uuid4()), str(uuid.uuid4), None,
        )

    @unittest.skip('not implemented')
    def test_edges_have_unique_ids(self):
        """Test that generated edge ids are unique"""
        src_id = str(uuid.uuid4())
        dst_id = str(uuid.uuid4())
        self.driver.node_merge(node_id=src_id, label='test')
        self.driver.node_merge(node_id=dst_id, label='test')

        edge1 = self.driver.edge_insert(PsqlEdge(
            src_id=src_id, dst_id=dst_id, label='test1'))
        edge2 = self.driver.edge_insert(PsqlEdge(
            src_id=src_id, dst_id=dst_id, label='test2'))
        self.assertNotEqual(edge1.edge_id, edge2.edge_id)

    @unittest.skip('not implemented')
    def test_edge_insert_and_lookup(self):
        """Test edge creation and lookup by dst_id, src_id, dst_id and src_id"""
        with self.driver.session_scope():
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            self.driver.node_merge(node_id=src_id, label='test')
            self.driver.node_merge(node_id=dst_id, label='test')

            edge = self.driver.edge_insert(PsqlEdge(
                src_id=src_id, dst_id=dst_id, label='test'))
            self.driver.edge_update(edge, properties={'test': None})
            self.driver.edge_update(edge, properties={'test': 2})

            props = {'test': 2}
            edge = self.driver.edge_lookup_one(dst_id=dst_id)
            self.assertEqual(edge.src_id, src_id)
            self.assertEqual(edge.dst_id, dst_id)
            self.assertEqual(edge.properties, props)

            edge = self.driver.edge_lookup_one(src_id=src_id)
            self.assertEqual(edge.src_id, src_id)
            self.assertEqual(edge.dst_id, dst_id)
            self.assertEqual(edge.properties, props)

            edge = self.driver.edge_lookup_one(src_id=src_id, dst_id=dst_id)
            self.assertEqual(edge.src_id, src_id)
            self.assertEqual(edge.dst_id, dst_id)
            self.assertEqual(edge.properties, props)

    @unittest.skip('not implemented')
    def test_edge_snapshot(self):
        with self.driver.session_scope():
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            self.driver.node_merge(node_id=src_id, label='test')
            self.driver.node_merge(node_id=dst_id, label='test')

            edge = self.driver.edge_insert(PsqlEdge(
                src_id=src_id, dst_id=dst_id, label='test'))
            self.driver.edge_update(edge, properties={'test': 2})
            voided_edge = self.driver.edge_lookup(label='test', voided=True).one()
            self.assertEqual({}, voided_edge.properties)

    @unittest.skip('not implemented')
    def test_edge_insert_and_lookup_properties(self):
        """Test edge property merging"""
        with self.driver.session_scope():
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            props = {'key1': str(random.random()), 'key2': random.random()}
            self.driver.node_merge(node_id=src_id, label='test')
            self.driver.node_merge(node_id=dst_id, label='test')
            self.driver.edge_insert(PsqlEdge(
                src_id=src_id, dst_id=dst_id, properties=props, label='test'
            ))
            edge = self.driver.edge_lookup_one(src_id=src_id, dst_id=dst_id)
            self.assertEqual(edge.src_id, src_id)
            self.assertEqual(edge.dst_id, dst_id)
            self.assertEqual(edge.properties, props)

    @unittest.skip('not implemented')
    def test_edge_lookup_leaves(self):
        """Test looking up the leaves on an edge

        Create a star topology, verify lookup by src_id
        and that all nodes are attached
        """

        with self.driver.session_scope():
            leaf_count = 10
            src_id = str(uuid.uuid4())
            self.driver.node_merge(node_id=src_id, label='test')

            dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
            for dst_id in dst_ids:
                self.driver.node_merge(node_id=dst_id, label='test')
                self.driver.edge_insert(PsqlEdge(
                    src_id=src_id, dst_id=dst_id, label='test'))

            edge_ids = [e.dst_id for e in self.driver.edge_lookup(src_id=src_id)]
            self.assertEqual(len(set(edge_ids)), leaf_count)

            for dst_id in dst_ids:
                self.assertTrue(dst_id in set(edge_ids))

    @unittest.skip('not implemented')
    def test_sessioned_path_insertion(self):
        """Test creation of a sessioned node path

        Test creation of a sample graph with pre-existing nodes
        in a single session
        """

        with self.driver.session_scope():
            leaf_count = 10
            src_id = str(uuid.uuid4())
            dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
            self.driver.node_merge(node_id=src_id, label='test')

            with self.driver.session_scope() as session:
                for dst_id in dst_ids:
                    self.driver.node_merge(
                        node_id=dst_id, label='test', session=session)

            with self.driver.session_scope() as session:
                for dst_id in dst_ids:
                    node = self.driver.node_lookup_one(
                        node_id=dst_id, session=session)
                    self.driver.edge_insert(
                        PsqlEdge(src_id=src_id, dst_id=node.node_id, label='test'),
                        session=session
                    )

            edge_ids = [e.dst_id for e in self.driver.edge_lookup(
                src_id=src_id)]
            self.assertEqual(len(set(edge_ids)), leaf_count)

            for dst_id in dst_ids:
                self.assertTrue(dst_id in set(edge_ids))

    @unittest.skip('not implemented')
    def test_path_deletion(self):
        """Test path deletion

        Test path deletion and verify deletion is cascaded to edges
        """

        with self.driver.session_scope():
            leaf_count = 10
            src_id = str(uuid.uuid4())
            dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
            self.driver.node_merge(node_id=src_id, label='test')

            # Create nodes and link them to source
            for dst_id in dst_ids:
                self.driver.node_merge(node_id=dst_id, label='test')
                self.driver.edge_insert(PsqlEdge(
                    src_id=src_id, dst_id=dst_id, label='test'))

            # Verify that the edges there are correct
            for dst_id in dst_ids:
                edge = self.driver.edge_lookup_one(dst_id=dst_id)
                self.assertEqual(edge.src_id, src_id)

                edges = [e.dst_id for e in self.driver.edge_lookup(src_id=src_id)]
                for dst_id in dst_ids:
                    self.assertTrue(dst_id in set(edges))

            # Delete all dst nodes
            for dst_id in dst_ids:
                self.driver.node_delete(node_id=dst_id)

            # Make sure that there are no hanging edges
            edges = [e.dst_id for e in self.driver.edge_lookup(src_id=src_id)]
            for dst_id in dst_ids:
                self.assertTrue(dst_id not in set(edges))
            for dst_id in dst_ids:
                self.assertIs(self.driver.edge_lookup_one(dst_id=dst_id), None)

    @unittest.skip('not implemented')
    def test_node_validator_error(self):
        """Test node validator error"""

        with self.driver.session_scope():
            node_id = str(uuid.uuid4())
            temp = self.driver.node_validator.validate
            self.driver.node_validator.validate = lambda x: False
            try:
                self.assertRaises(
                    ValidationError,
                    self.driver.node_merge, node_id, label='test',
                )
            except:
                self.driver.node_validator.validate = temp
                raise

    @unittest.skip('not implemented')
    def test_edge_validator_error(self):
        """Test edge validator error"""

        with self.driver.session_scope():
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            temp = self.driver.edge_validator.validate
            self.driver.edge_validator.validate = lambda x: False
            self.driver.node_merge(src_id, label='test')
            self.driver.node_merge(dst_id, label='test')

            try:
                self.assertRaises(
                    ValidationError,
                    self.driver.edge_insert,
                    PsqlEdge(src_id=src_id, dst_id=dst_id, label='test'),
                )
            except:
                self.driver.edge_validator.validate = temp
                raise

    @unittest.skip('not implemented')
    def test_get_nodes(self):
        """Test node get"""

        with self.driver.session_scope():
            self._clear_tables()

            ret_node_ids = []
            node_ids = [str(uuid.uuid4()) for i in range(self.REPEAT_COUNT*10)]

            for node_id in node_ids:
                self.driver.node_merge(node_id, label='test')

            nodes = self.driver.get_nodes()

            for node in nodes:
                self.assertTrue(node.node_id in node_ids)
                ret_node_ids.append(node.node_id)
            for node_id in node_ids:
                self.assertTrue(node_id in ret_node_ids)
            self.assertEqual(len(node_ids), len(ret_node_ids))

    @unittest.skip('not implemented')
    def test_get_edges(self):
        """Test edge get"""

        with self.driver.session_scope():
            self._clear_tables()

            count = 10
            src_ids = [str(uuid.uuid4()) for i in range(count)]
            dst_ids = [str(uuid.uuid4()) for i in range(count)]

            for src_id, dst_id in zip(src_ids, dst_ids):
                self.driver.node_merge(src_id, label='test_src')
                self.driver.node_merge(dst_id, label='test_dst')
                self.driver.edge_insert(PsqlEdge(
                    src_id=src_id,
                    dst_id=dst_id,
                    label='test_edge',
                ))

            edges = self.driver.get_edges()
            ret_src_ids = []
            ret_dst_ids = []
            for edge in edges:
                self.assertTrue(edge.src_id in src_ids)
                self.assertTrue(edge.dst_id in dst_ids)
                ret_src_ids.append(edge.src_id)
                ret_dst_ids.append(edge.dst_id)
            for src_id in src_ids:
                self.assertTrue(src_id in ret_src_ids)
            for dst_id in dst_ids:
                self.assertTrue(dst_id in ret_dst_ids)

    @unittest.skip('not implemented')
    def _create_subtree(self, parent_id, level=0):
        with self.driver.session_scope():
            for i in range(5):
                node_id = str(uuid.uuid4())
                self.driver.node_merge(
                    node_id=node_id, label='test_{}'.format(level))
                self.driver.edge_insert(PsqlEdge(
                    src_id=parent_id, dst_id=node_id, label='test'
                ))
                if level < 2:
                    self._create_subtree(node_id, level+1)

    @unittest.skip('not implemented')
    def _walk_tree(self, node, level=0):
        with self.driver.session_scope():
            for edge in node.edges_out:
                print '+--'*level + '>', edge.dst
                self._walk_tree(edge.dst, level+1)

    @unittest.skip('not implemented')
    def test_tree_walk(self):
        with self.driver.session_scope():
            node_id = str(uuid.uuid4())
            self.driver.node_merge(node_id=node_id, label='test')
            self._create_subtree(node_id)
            with self.driver.session_scope() as session:
                node = self.driver.node_lookup_one(node_id, session=session)
                self._walk_tree(node)

    @unittest.skip('not implemented')
    def test_edge_multiplicity(self):
        with self.driver.session_scope():
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            self.driver.node_merge(node_id=src_id, label='test')
            self.driver.node_merge(node_id=dst_id, label='test')
            self.driver.edge_insert(PsqlEdge(
                src_id=src_id, dst_id=dst_id, label='a'))
            self.driver.edge_insert(PsqlEdge(
                src_id=src_id, dst_id=dst_id, label='b'))
            self.assertEqual(len(list(self.driver.edge_lookup(
                src_id=src_id, dst_id=dst_id))), 2)
            self.assertEqual(len(list(self.driver.edge_lookup(
                src_id=src_id, dst_id=dst_id, label='a'))), 1)
            self.assertEqual(len(list(self.driver.edge_lookup(
                src_id=src_id, dst_id=dst_id, label='b'))), 1)
            self.assertRaises(
                IntegrityError,
                self.driver.edge_insert,
                PsqlEdge(src_id=src_id, dst_id=dst_id, label='a')
            )

    @unittest.skip('not implemented')
    def test_simple_automatic_session(self):
        idA = str(uuid.uuid4())
        with self.driver.session_scope():
            self.driver.node_insert(PsqlNode(idA, 'label'))
        with self.driver.session_scope():
            self.driver.node_lookup(idA).one()

    @unittest.skip('not implemented')
    def test_rollback_automatic_session(self):
        """test_rollback_automatic_session

        Make sure that within a session scope, an error causes the
        entire scope to be rolled back even without an explicit
        session being passed

        """
        nid = str(uuid.uuid4())
        with self.assertRaises(IntegrityError):
            with self.driver.session_scope():
                self.driver.node_insert(PsqlNode(nid, 'label'))
                self.driver.node_insert(PsqlNode(nid, 'label2'))
        with self.driver.session_scope():
            self.assertEqual(len(list(self.driver.node_lookup(nid).all())), 0)

    @unittest.skip('not implemented')
    def test_commit_automatic_session(self):
        """test_commit_automatic_session

        Make sure that when not wrapped in a session scope the
        successful commit of a conflicting node does not rollback
        previously committed nodes. (i.e. the statements don't inherit
        the same session)

        """
        nid = str(uuid.uuid4())
        self.driver.node_insert(PsqlNode(nid, 'label'))
        self.assertRaises(
            IntegrityError,
            self.driver.node_insert,
            PsqlNode(nid, 'label2'))
        with self.driver.session_scope():
            self.assertEqual(self.driver.node_lookup(nid).one().label, 'label')
        self.assertFalse(self.driver.has_session())

    @unittest.skip('not implemented')
    def test_automatic_nested_session(self):
        """test_automatic_nested_session

        Make sure that given a call to explicitly nest session scopes,
        the nested session commits first

        """
        nid = str(uuid.uuid4())
        with self.assertRaises(IntegrityError):
            with self.driver.session_scope():
                self.driver.node_insert(PsqlNode(nid, 'label'))
                with self.driver.session_scope(can_inherit=False):
                    self.driver.node_insert(PsqlNode(nid, 'label2'))
        with self.driver.session_scope():
            self.assertEqual(
                self.driver.node_lookup(nid).one().label, 'label2')
        self.assertFalse(self.driver.has_session())

    @unittest.skip('not implemented')
    def test_automatic_nested_session2(self):
        """test_automatic_nested_session2

        Make sure that given a call to explicitly nest session scopes,
        failure of the nested session scope does not affect the parent
        scope.

        """
        id1 = str(uuid.uuid4())
        id2 = str(uuid.uuid4())
        self.driver.node_insert(PsqlNode(id1, 'label2'))
        with self.driver.session_scope():
            self.driver.node_insert(PsqlNode(id2, 'label'))
            with self.assertRaises(IntegrityError):
                with self.driver.session_scope(can_inherit=False):
                    self.driver.node_insert(PsqlNode(id1, 'label2'))
        with self.driver.session_scope():
            self.assertEqual(self.driver.node_lookup(id2).one().label, 'label')
        self.assertFalse(self.driver.has_session())

    @unittest.skip('not implemented')
    def test_automatic_nested_session3(self):
        """test_automatic_nested_session3

        Also, verify that two statements in a nested session_scope
        inherit the same session (i.e. the session stack is working
        properly).

        """
        id1, id2, id3 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        self.driver.node_insert(PsqlNode(id1, 'label2'))
        with self.driver.session_scope():
            self.driver.node_insert(PsqlNode(id2, 'label'))
            with self.assertRaises(IntegrityError):
                with self.driver.session_scope(can_inherit=False):
                    self.driver.node_insert(PsqlNode(id1, 'label2'))
                    self.driver.node_insert(PsqlNode(id3, 'label2'))
        with self.driver.session_scope():
            self.assertEqual(self.driver.node_lookup(id2).one().label, 'label')
            self.assertEqual(self.driver.node_lookup(id3).count(), 0)
        self.assertFalse(self.driver.has_session())

    @unittest.skip('not implemented')
    def test_automatic_nested_session_inherit_valid(self):
        """test_automatic_nested_session_inherit_valid

        Verify that implicitly nested session scopes correctly inherit
        the parent session for valid node insertion

        """
        id1, id2 = str(uuid.uuid4()), str(uuid.uuid4())
        with self.driver.session_scope():
            self.driver.node_insert(PsqlNode(id1, 'label1'))
            with self.driver.session_scope():
                    self.driver.node_insert(PsqlNode(id2, 'label2'))
            self.assertEqual(
                self.driver.node_lookup(id1).one().label, 'label1')
            self.assertEqual(
                self.driver.node_lookup(id2).one().label, 'label2')
        self.assertFalse(self.driver.has_session())

    @unittest.skip('not implemented')
    def test_automatic_nested_session_inherit_invalid(self):
        """test_automatic_nested_session_inherit_invalid

        Verify that implicitly nested session scopes correctly inherit
        the parent session.

        """
        id1, id2 = str(uuid.uuid4()), str(uuid.uuid4())
        with self.driver.session_scope() as outer:
            self.driver.node_insert(PsqlNode(id1, 'label1'))
            with self.driver.session_scope() as inner:
                self.assertEqual(inner, outer)
                self.driver.node_insert(PsqlNode(id2, 'label2'))
                inner.rollback()
        with self.driver.session_scope():
            self.assertEqual(self.driver.node_lookup(id1).count(), 0)
            self.assertEqual(self.driver.node_lookup(id2).count(), 0)
        self.assertFalse(self.driver.has_session())

    @unittest.skip('not implemented')
    def test_explicit_to_inherit_nested_session(self):
        """test_explicit_to_inherit_nested_session

        Verify that the children of an explicitly passed session scope
        inherit the explicit session and commit all updates.

        """
        id1, id2, id3 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        outer = self.driver._new_session()  # don't do this
        with self.driver.session_scope(outer):
            self.driver.node_insert(PsqlNode(id1, 'label1'))
            with self.driver.session_scope() as inner:
                self.assertEqual(inner, outer)
                self.driver.node_insert(PsqlNode(id2, 'label2'))
                with self.driver.session_scope() as third:
                    self.assertEqual(third, outer)
                    self.driver.node_insert(PsqlNode(id3, 'label2'))
        with self.driver.session_scope():
            self.assertEqual(self.driver.node_lookup(id2).count(), 0)
        outer.commit()
        with self.driver.session_scope():
            self.assertEqual(self.driver.node_lookup(id1).count(), 1)
            self.assertEqual(self.driver.node_lookup(id2).count(), 1)
            self.assertEqual(self.driver.node_lookup(id3).count(), 1)
        self.assertFalse(self.driver.has_session())

    @unittest.skip('not implemented')
    def test_explicit_to_inherit_nested_session_rollback(self):
        """test_explicit_to_inherit_nested_session_rollback

        Verify that the children of an explicitly passed session scope
        inherit the explicit session and rolls back all levels.

        """
        id1, id2, id3 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        outer = self.driver._new_session()  # don't do this
        with self.driver.session_scope(outer):
            self.driver.node_insert(PsqlNode(id1, 'label1'))
            with self.driver.session_scope() as inner:
                self.assertEqual(inner, outer)
                self.driver.node_insert(PsqlNode(id2, 'label2'))
                with self.driver.session_scope() as third:
                    self.assertEqual(third, outer)
                    self.driver.node_insert(PsqlNode(id3, 'label2'))
                    third.rollback()
        with self.driver.session_scope():
            self.assertEqual(self.driver.node_lookup(id1).count(), 0)
            self.assertEqual(self.driver.node_lookup(id2).count(), 0)
            self.assertEqual(self.driver.node_lookup(id3).count(), 0)
        self.assertFalse(self.driver.has_session())

    @unittest.skip('not implemented')
    def test_mixed_session_inheritance(self):
        """test_mixed_session_inheritance

        Verify that an explicit session passed to a middle level in a
        tripple nested session_scope is independent from the outer and
        inner levels.

        """
        id1, id2, id3 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        external = self.driver._new_session()
        with self.driver.session_scope() as outer:
            self.driver.node_insert(PsqlNode(id1, 'rollback'))
            with self.driver.session_scope(external) as inner:
                self.assertEqual(inner, external)
                self.assertNotEqual(inner, outer)
                self.driver.node_insert(PsqlNode(id2, 'inserted'))
                with self.driver.session_scope(outer) as third:
                    self.assertEqual(third, outer)
                    self.driver.node_insert(PsqlNode(id3, 'rollback'))
                    third.rollback()
        with self.driver.session_scope():
            self.assertEqual(self.driver.node_lookup(id2).count(), 0)
        external.commit()
        with self.driver.session_scope():
            self.assertEqual(self.driver.node_lookup(id1).count(), 0)
            self.assertEqual(self.driver.node_lookup(id2).count(), 1)
            self.assertEqual(self.driver.node_lookup(id3).count(), 0)
        self.assertFalse(self.driver.has_session())

    @unittest.skip('not implemented')
    def test_explicit_session(self):
        """test_explicit_session

        Verify that passing a session explicitly functions as expected

        """
        id1, id2 = str(uuid.uuid4()), str(uuid.uuid4())
        with self.driver.session_scope() as session:
            self.driver.node_insert(PsqlNode(id1, 'label2'))
            self.driver.node_insert(PsqlNode(id2, 'label2'), session)
            session.rollback()
        with self.driver.session_scope():
            self.assertEqual(self.driver.node_lookup(id2).count(), 0)
            self.assertEqual(self.driver.node_lookup(id1).count(), 0)
        self.assertFalse(self.driver.has_session())

    @unittest.skip('not implemented')
    def test_library_functions_use_session_implicitly(self):
        """Test that library functions use the session they're scoped in

        """
        id1 = str(uuid.uuid4())
        with self.driver.session_scope():
            self.driver.node_insert(PsqlNode(id1, 'label'))
            self.driver.node_lookup(node_id=id1).one()


if __name__ == '__main__':

    def run_test(test):
        suite = unittest.TestLoader().loadTestsFromTestCase(test)
        unittest.TextTestRunner(verbosity=2).run(suite)

    run_test(TestPsqlGraphDriver)
