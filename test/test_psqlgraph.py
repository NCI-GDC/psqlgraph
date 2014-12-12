import uuid
import unittest
import logging
import psqlgraph
from psqlgraph import PsqlGraphDriver, session_scope, sanitizer
from psqlgraph.sanitizer import sanitize as sanitize
from multiprocessing import Process
import random
from sqlalchemy.exc import IntegrityError
from psqlgraph.exc import ValidationError

from datetime import datetime


host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


logging.basicConfig(level=logging.INFO)


class TestPsqlGraphDriver(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger(__name__)
        self.driver = PsqlGraphDriver(host, user, password, database)
        self.REPEAT_COUNT = 200

    def _clear_tables(self):
        conn = self.driver.engine.connect()
        conn.execute('commit')
        conn.execute('delete from edges')
        conn.execute('delete from nodes')
        conn.close()

    def test_sanitize_int(self):
        """Test sanitization of castable integer type"""
        self.assertEqual(sanitizer.cast(5), 5)

    def test_sanitize_bool(self):
        """Test sanitization of castable integer type"""
        self.assertEqual(sanitizer.cast(True), True)

    def test_sanitize_str(self):
        """Test sanitization of castable string type"""
        self.assertEqual(sanitizer.cast('test'), 'test')

    def test_sanitize_dict(self):
        """Test sanitization of castable dictionary type"""
        test = {'first': 1, 'second': 2, 'third': 'This is a test'}
        self.assertRaises(psqlgraph.ProgrammingError,
                          sanitizer.cast, test)

    def test_sanitize_other(self):
        """Test sanitization of select non-standard types"""
        A = psqlgraph.QueryError
        self.assertRaises(psqlgraph.ProgrammingError,
                          sanitizer.cast, A)
        B = logging
        self.assertRaises(psqlgraph.ProgrammingError,
                          sanitizer.cast, B)

    def test_sanitize(self):
        """ Test sanitization of select non-standard types"""
        self.assertEqual(sanitize({
            'key1': 'First', 'key2': 25, 'key3': 1.2, 'key4': None
        }), {
            'key1': 'First', 'key2': 25, 'key3': 1.2, 'key4': None
        })

    def test_node_null_label_merge(self):
        """Test merging of a non-existent node

        Verify the case where a user merges a single non-existent node
        """

        self.assertRaises(
            IntegrityError,
            self.driver.node_merge,
            node_id=str(uuid.uuid4())
        )

    def test_node_null_query_one(self):
        """Test querying of a single non-existent node

        Verify the case where a user queries for a single non-existent node
        """

        node = self.driver.node_lookup_one(str(uuid.uuid4()))
        self.assertTrue(node is None)

    def test_node_null_query(self):
        """Test querying for any non-existent nodes

        Verify the case where a user queries for any non-existent nodes
        """

        node = self.driver.node_lookup(str(uuid.uuid4()))
        self.assertEqual(node, [])

    def verify_node_count(self, count, node_id=None, matches=None,
                          voided=False):
        """Test querying for the count on a non-existent node

        Verify the case where a user queries a count for non-existent nodes
        """
        nodes = self.driver.node_lookup(
            node_id=node_id,
            property_matches=matches,
            include_voided=voided
        )
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
        self.driver.node_merge(node_id=tempid, label='test',
                               properties=properties)

        node = self.driver.node_lookup_one(tempid)
        self.assertEqual(sanitize(properties), node.properties)

    def test_node_update_properties_by_id(self, given_id=None, label=None):
        """Test updating node properties by ID

        Insert a single node, update it, verify that

        (1) The first insertion is successful
        (2) The update is successful
        (3) The transaction of the update is maintained
        (4) There is only a single version of the node
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
        for key, val in propertiesB.iteritems():
            propertiesA[key] = val

        if not given_id:
            # Test that there is only 1 non-void node with node_id and property
            # equality
            # if this is not part of another test, check the count
            node = self.driver.node_lookup_one(node_id)
            self.assertEqual(sanitize(propertiesA),
                             node.properties)

            self.verify_node_count(2, node_id=node_id, voided=True)

        return propertiesA

    def test_query_by_label(self, node_id=None):
        """Test ability to query for nodes by label"""

        label = 'test_' + str(random.random())
        for i in range(self.REPEAT_COUNT):
            self.test_node_update_properties_by_id(label=label)
        nodes = self.driver.node_lookup(label=label)
        self.assertEqual(len(nodes), self.REPEAT_COUNT)

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
        node = self.driver.node_lookup_one(property_matches=propertiesA)
        self.driver.node_merge(node=node, label='test',
                               properties=propertiesB)

        # Merge properties
        for key, val in propertiesB.iteritems():
            propertiesA[key] = val

        node = self.driver.node_lookup_one(property_matches=propertiesB)
        self.assertEqual(propertiesA, node.properties)

        node = self.driver.node_lookup_one(node_id=node_id)
        self.assertEqual(propertiesA, node.properties)

        nodes = self.verify_node_count(2, node_id=node_id, voided=True)
        self.assertEqual(propertiesA, nodes[1].properties)

        return propertiesA

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
        for key, val in system_annotationsB.iteritems():
            system_annotationsA[key] = val

        # if this is not part of another test, check the count
        if not given_id:
            # Test that there is only 1 non-void node with node_id and property
            # equality
            node = self.driver.node_lookup_one(node_id)
            self.assertEqual(sanitize(system_annotationsA),
                             node.system_annotations)

            nodes = self.verify_node_count(2, node_id=node_id, voided=True)
            self.assertEqual(sanitize(system_annotationsA),
                             nodes[0].system_annotations)

        return system_annotationsA

    def _insert_node(self, node):
        """Test inserting a node"""
        with psqlgraph.session_scope(self.driver.engine) as session:
            session.add(node)

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

    def test_null_node_void(self):
        """Test voiding of a null node

        Verify that the library handles null nodes properly on voiding
        """

        self.assertRaises(
            psqlgraph.ProgrammingError,
            self.driver._node_void,
            None
        )

    def test_null_node_merge(self):
        """Test merging of a null node

        Verify that the library handles null nodes properly on merging
        """
        self.assertRaises(psqlgraph.QueryError, self.driver.node_merge)

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
            node = self.driver.node_lookup_one(node_id)
            self.assertEqual(sanitize(props), node.properties)

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
            node = self.driver.node_lookup_one(node_id)
            self.assertEqual(sanitize(annotations), node.system_annotations)

    def test_sessioned_node_update(self):
        """Test repeated update of a sessioned node

        Repeate test_repeated_node_update but passing a single session for
        all interactions to use

        """

        node_ids = [str(uuid.uuid4()) for i in range(self.REPEAT_COUNT)]
        properties = {}
        with session_scope(self.driver.engine) as session:
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

        for node_id in node_ids:
            node = self.driver.node_lookup_one(node_id=node_id)
            self.assertEqual(
                sanitize(properties[node_id]), node.properties,
                'Node properties' 'do not match expected properties'
            )

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

    def test_node_clobber(self):
        """Test that clobbering a node replaces all of its properties"""

        tempid = str(uuid.uuid4())

        propertiesA = {'key1':  None, 'key2':  3, 'key3':  'test'}

        {'key1':  None, 'key2':  2, 'key3':  datetime.now()}
        self.driver.node_merge(node_id=tempid, label='test',
                               properties=propertiesA)

        propertiesB = {'key1':  True, 'key2':  0, 'key3':  'test'}
        self.driver.node_clobber(node_id=tempid, properties=propertiesB,
                                 label='test')

        propertiesB = sanitizer.sanitize(propertiesB)
        nodes = self.driver.node_lookup(node_id=tempid)
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, '
                         'instead found {count}'.format(count=len(nodes)))
        self.assertEqual(propertiesB, nodes[0].properties, 'Node properties do'
                         ' not match expected properties')

    def test_node_delete_property_keys(self):
        """Test the ability to remove property keys from nodes"""

        tempid = str(uuid.uuid4())
        properties = {'key1':  None, 'key2':  2, 'key3':  'test'}
        self.driver.node_merge(node_id=tempid, label='test',
                               properties=properties)

        self.driver.node_delete_property_keys(['key2', 'key3'], node_id=tempid)
        properties.pop('key2')
        properties.pop('key3')

        nodes = self.driver.node_lookup_one(node_id=tempid)
        self.assertEqual(properties, nodes.properties)

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

        nodes = self.driver.node_lookup(node_id=tempid)
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, '
                         'instead found {count}'.format(count=len(nodes)))
        self.assertEqual(annotations, nodes[0].system_annotations)

    def test_node_delete(self):
        """Test node deletion"""

        tempid = str(uuid.uuid4())
        self.driver.node_merge(node_id=tempid, label='test')
        self.driver.node_delete(node_id=tempid)

        nodes = self.driver.node_lookup(tempid)
        self.assertEqual(len(nodes), 0, 'Expected a no non-voided nodes '
                         'to be found, instead found {count}'.format(
                             count=len(nodes)))

    def test_repeated_node_delete(self):
        """Test repeated node deletion correctness"""

        node_id = str(uuid.uuid4())
        for i in range(self.REPEAT_COUNT):
            self.test_node_update_properties_by_id(node_id)
            self.driver.node_delete(node_id=node_id)
            self.assertIs(self.driver.node_lookup_one(node_id=node_id), None)

    def test_edge_null_label_merge(self):
        """Test merging of a null edge

        Verify the case where a user merges a single non-existent node
        """

        self.assertRaises(
            IntegrityError,
            self.driver.edge_merge,
            src_id=str(uuid.uuid4()),
            dst_id=str(uuid.uuid4()),
        )

    def test_edge_merge_and_lookup(self):
        """Test edge creation and lookup by dst_id, src_id, dst_id and src_id"""

        src_id = str(uuid.uuid4())
        dst_id = str(uuid.uuid4())
        self.driver.node_merge(node_id=src_id, label='test')
        self.driver.node_merge(node_id=dst_id, label='test')

        self.driver.edge_merge(src_id=src_id, dst_id=dst_id, label='test')
        self.driver.edge_merge(src_id=src_id, dst_id=dst_id,
                               properties={'test': None})
        self.driver.edge_merge(src_id=src_id, dst_id=dst_id,
                               properties={'test': 2})

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

    def test_edge_merge_and_lookup_properties(self):
        """Test edge property merging"""
        src_id = str(uuid.uuid4())
        dst_id = str(uuid.uuid4())
        props = {'key1': str(random.random()), 'key2': random.random()}
        self.driver.node_merge(node_id=src_id, label='test')
        self.driver.node_merge(node_id=dst_id, label='test')
        self.driver.edge_merge(
            src_id=src_id, dst_id=dst_id, properties=props, label='test'
        )
        edge = self.driver.edge_lookup_one(src_id=src_id, dst_id=dst_id)
        self.assertEqual(edge.src_id, src_id)
        self.assertEqual(edge.dst_id, dst_id)
        self.assertEqual(edge.properties, props)

    def test_edge_lookup_leaves(self):
        """Test looking up the leaves on an edge

        Create a star topology, verify lookup by src_id
        and that all nodes are attached
        """

        leaf_count = 10
        src_id = str(uuid.uuid4())
        self.driver.node_merge(node_id=src_id, label='test')

        dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
        for dst_id in dst_ids:
            self.driver.node_merge(node_id=dst_id, label='test')
            self.driver.edge_merge(src_id=src_id, dst_id=dst_id, label='test')

        edge_ids = [e.dst_id for e in self.driver.edge_lookup(src_id=src_id)]
        self.assertEqual(len(set(edge_ids)), leaf_count)

        for dst_id in dst_ids:
            self.assertTrue(dst_id in set(edge_ids))

    def test_sessioned_path_insertion(self):
        """Test creation of a sessioned node path

        Test creation of a sample graph with pre-existing nodes
        in a single session
        """

        leaf_count = 10
        src_id = str(uuid.uuid4())
        dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
        self.driver.node_merge(node_id=src_id, label='test')

        with session_scope(self.driver.engine) as session:
            for dst_id in dst_ids:
                self.driver.node_merge(
                    node_id=dst_id, label='test', session=session
                )

        with session_scope(self.driver.engine) as session:
            for dst_id in dst_ids:
                node = self.driver.node_lookup_one(
                    node_id=dst_id, session=session)
                self.driver.edge_merge(src_id=src_id, dst_id=node.node_id,
                                       session=session, label='test')

        edge_ids = [e.dst_id for e in self.driver.edge_lookup(
            src_id=src_id)]
        self.assertEqual(len(set(edge_ids)), leaf_count)

        for dst_id in dst_ids:
            self.assertTrue(dst_id in set(edge_ids))

    def test_path_deletion(self):
        """Test path deletion

        Test path deletion and verify deletion is cascaded to edges
        """

        leaf_count = 10
        src_id = str(uuid.uuid4())
        dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
        self.driver.node_merge(node_id=src_id, label='test')

        # Create nodes and link them to source
        for dst_id in dst_ids:
            self.driver.node_merge(node_id=dst_id, label='test')
            self.driver.edge_merge(src_id=src_id, dst_id=dst_id, label='test')

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

    def test_node_validator_error(self):
        """Test node validator error"""
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

    def test_edge_validator_error(self):
        """Test edge validator error"""
        src_id = str(uuid.uuid4())
        dst_id = str(uuid.uuid4())
        temp = self.driver.edge_validator.validate
        self.driver.edge_validator.validate = lambda x: False
        self.driver.node_merge(src_id, label='test')
        self.driver.node_merge(dst_id, label='test')

        try:
            self.assertRaises(
                ValidationError,
                self.driver.edge_merge, src_id, dst_id, label='test',
            )
        except:
            self.driver.edge_validator.validate = temp
            raise

    def test_get_nodes(self):
        """Test node get"""
        self._clear_tables()

        ret_node_ids = []
        node_ids = [str(uuid.uuid4()) for i in range(self.REPEAT_COUNT*10)]

        for node_id in node_ids:
            print node_id
            self.driver.node_merge(node_id, label='test')

        nodes = self.driver.get_nodes()

        for node in nodes:
            self.assertTrue(node.node_id in node_ids)
            ret_node_ids.append(node.node_id)
        for node_id in node_ids:
            self.assertTrue(node_id in ret_node_ids)
        self.assertEqual(len(node_ids), len(ret_node_ids))

    def test_get_edges(self):
        """Test edge get"""
        self._clear_tables()

        # count = self.REPEAT_COUNT*10
        count = 10
        src_ids = [str(uuid.uuid4()) for i in range(count)]
        dst_ids = [str(uuid.uuid4()) for i in range(count)]

        for src_id, dst_id in zip(src_ids, dst_ids):
            self.driver.node_merge(src_id, label='test_src')
            self.driver.node_merge(dst_id, label='test_dst')
            self.driver.edge_merge(
                src_id=src_id,
                dst_id=dst_id,
                label='test_edge',
            )

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
        self.assertEqual(len(ret_src_ids), len(src_ids))
        self.assertEqual(len(ret_dst_ids), len(dst_ids))

if __name__ == '__main__':

    def run_test(test):
        suite = unittest.TestLoader().loadTestsFromTestCase(test)
        unittest.TextTestRunner(verbosity=2).run(suite)

    run_test(TestPsqlGraphDriver)
