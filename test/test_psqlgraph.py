import uuid
import unittest
import time
import logging
import psqlgraph
from psqlgraph import PsqlGraphDriver, session_scope
from multiprocessing import Process
import random
from sqlalchemy.exc import IntegrityError

from psqlgraph.exc import NodeCreationError, EdgeCreationError

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

    def test_sanitize_int(self):
        """ Test sanitization of castable integer type"""
        self.assertEqual(psqlgraph.sanitizer.cast(5), 5)

    def test_sanitize_str(self):
        """ Test sanitization of castable string type"""
        self.assertEqual(psqlgraph.sanitizer.cast('test'), 'test')

    def test_sanitize_dict(self):
        """ Test sanitization of castable dictionary type"""
        test = {'first': 1, 'second': 2, 'third': 'This is a test'}
        self.assertRaises(psqlgraph.ProgrammingError,
                          psqlgraph.sanitizer.cast, test)

    def test_sanitize_other(self):
        """ Test sanitization of select non-standard types"""
        A = psqlgraph.QueryError
        self.assertRaises(psqlgraph.ProgrammingError,
                          psqlgraph.sanitizer.cast, A)
        B = logging
        self.assertRaises(psqlgraph.ProgrammingError,
                          psqlgraph.sanitizer.cast, B)

    def test_sanitize(self):
        """ Test sanitization of select non-standard types"""
        self.assertEqual(psqlgraph.sanitizer.sanitize({
            'key1': 'First', 'key2': 25, 'key3': 1.2, 'key4': None
        }), {
            'key1': 'First', 'key2': 25, 'key3': 1.2, 'key4': None
        })

    def test_node_null_query_one(self):
        """Verify that the library handles the case where a user queries for a
        a single non-existant node

        """

        node = self.driver.node_lookup_one(str(uuid.uuid4()))
        self.assertTrue(node is None)

    def test_node_null_query(self):
        """Verify that the library handles the case where a user queries for
        non-existant nodes

        """

        node = self.driver.node_lookup(str(uuid.uuid4()))
        self.assertEqual(node, [])

    def verify_node_count(self, count, node_id=None, matches=None,
                          voided=False):
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
        """Insert a single node and query, compare that the result of the
        query is correct

        """
        tempid = str(uuid.uuid4())
        properties = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.node_merge(node_id=tempid, properties=properties)

        node = self.driver.node_lookup_one(tempid)
        self.assertEqual(properties, node.properties)

    def test_node_update_properties_by_id(self, node_id=None, label=None):
        """
        Insert a single node, update it, verify that

        (1) The fisrt insertion is successful
        (2) The update is successful
        (3) The transaction of the update is maintained
        (4) There is only a single version of the node

        """

        if not node_id:
            node_id = str(uuid.uuid4())

        # Add first node
        propertiesA = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.node_merge(node_id=node_id, properties=propertiesA,
                               label=label, max_retries=int(1e6))

        # Add second node
        propertiesB = {'key1': None, 'new_key': 2, 'timestamp': time.time()}
        self.driver.node_merge(node_id=node_id, properties=propertiesB,
                               label=label, max_retries=int(1e6))

        # Merge properties
        for key, val in propertiesA.iteritems():
            propertiesB[key] = val

        # Test that there is only 1 non-void node with node_id and property
        # equality
        # if this is not part of another test, check the count
        if not node_id:
            node = self.driver.node_lookup_one(node_id)
            self.assertEqual(propertiesB, node.properties)

            nodes = self.verify_node_count(2, node_id=node_id, voided=True)
            self.assertEqual(propertiesB, nodes[1].properties)

        return propertiesB

    def test_query_by_label(self, node_id=None):
        """
        Test ability to query for nodes by label
        """

        label = 'test_' + str(random.random())
        for i in range(self.REPEAT_COUNT):
            self.test_node_update_properties_by_id(label=label)
        nodes = self.driver.node_lookup(label=label)
        self.assertEqual(len(nodes), self.REPEAT_COUNT)

    def test_node_update_properties_by_matches(self):
        """
        Insert a single node, update it, verify that

        (1) The fisrt insertion is successful
        (2) The update is successful
        (3) The transaction of the update is maintained
        (4) There is only a single version of the node

        """

        node_id = str(uuid.uuid4())

        a = random.random()
        b = random.random()

        # Add first node
        propertiesA = {'key1': a, 'key2': str(a), 'key3': 12345}
        self.driver.node_merge(node_id=node_id, properties=propertiesA)

        # Add second node
        propertiesB = {'key1': b, 'key4': str(b)}
        node = self.driver.node_lookup_one(property_matches=propertiesA)
        self.driver.node_merge(node=node, properties=propertiesB)

        # Merge properties
        for key, val in propertiesB.iteritems():
            propertiesA[key] = val

        node = self.driver.node_lookup_one(property_matches=propertiesB)
        self.assertEqual(propertiesA, node.properties)

        node = self.driver.node_lookup_one(node_id=node_id)
        self.assertEqual(propertiesA, node.properties)

        nodes = self.verify_node_count(2, node_id=node_id, voided=True)
        self.assertEqual(propertiesA, nodes[1].properties)

        return propertiesB

    def test_node_update_system_annotations_id(self, node_id=None):
        """
        Insert a single node, update it, verify that

        (1) The fisrt insertion is successful
        (2) The update is successful
        (3) The transaction of the update is maintained
        (4) There is only a single version of the node

        """

        if not node_id:
            node_id = str(uuid.uuid4())

        # Add first node
        system_annotationsA = {
            'key1': None, 'key2': 2, 'key3': time.time()
        }
        self.driver.node_merge(node_id=node_id,
                               system_annotations=system_annotationsA)

        # Add second node
        system_annotationsB = {
            'key1': None, 'new_key': 2, 'timestamp': time.time()
        }
        self.driver.node_merge(node_id=node_id,
                               system_annotations=system_annotationsB)

        # Merge system_annotations
        for key, val in system_annotationsA.iteritems():
            system_annotationsB[key] = val

        # if this is not part of another test, check the count
        if not node_id:
            # Test that there is only 1 non-void node with node_id and property
            # equality
            node = self.driver.node_lookup_one(node_id)
            self.assertEqual(system_annotationsB, node.system_annotations)

            nodes = self.verify_node_count(2, node_id=node_id, voided=True)
            self.assertEqual(system_annotationsB, nodes[1].system_annotations)

        return system_annotationsB

    def _insert_node(self, node):
        with psqlgraph.session_scope(self.driver.engine) as session:
            session.add(node)

    def test_node_unique_id_constraint(self):
        """
        Verify that the table constraints prevent the existance two
        non-voided nodes with the same id
        """

        tempid = str(uuid.uuid4())

        # Add first node
        propertiesA = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.node_merge(node_id=tempid, properties=propertiesA)

        propertiesB = {'key1': None, 'key2': 2, 'key3': time.time()}
        bad_node = psqlgraph.PsqlNode(
            node_id=tempid,
            system_annotations={},
            acl=[],
            label=None,
            properties=propertiesB
        )

        self.assertRaises(IntegrityError, self._insert_node, bad_node)

    def test_null_node_void(self):
        """
        Verify that the library handles null nodes properly on voiding
        """

        self.assertRaises(
            psqlgraph.ProgrammingError,
            self.driver.node_void,
            None
        )

    def test_null_node_merge(self):
        """
        Verify that the library handles null nodes properly on merging
        """
        self.assertRaises(psqlgraph.QueryError, self.driver.node_merge)

    def test_repeated_node_update_properties_by_id(self, node_id=None):
        """
        Verify that updates repeated updates to a single node create
        the correct number of voided transactions and a single valid
        node with the correct properties
        """

        if not node_id:
            node_id = str(uuid.uuid4())
        for tally in range(self.REPEAT_COUNT):
            props = self.test_node_update_properties_by_id(node_id)

        if not node_id:
            self.verify_node_count(self.REPEAT_COUNT*2, node_id=node_id,
                                   voided=True)
            node = self.driver.node_lookup_one(node_id)
            self.assertEqual(props, node.properties)

    def test_repeated_node_update_system_annotations_by_id(self, node_id=None):
        """
        Verify that updates repeated updates to a single node create
        the correct number of voided transactions and a single valid
        node with the correct properties
        """

        REPEAT_COUNT = 200
        if not node_id:
            node_id = str(uuid.uuid4())
        for tally in range(REPEAT_COUNT):
            annotations = self.test_node_update_system_annotations_id(node_id)

        if not node_id:
            self.verify_node_count(REPEAT_COUNT*2, node_id=node_id,
                                   voided=True)
            node = self.driver.node_lookup_one(node_id)
            self.assertEqual(annotations, node.system_annotations)

    def test_sessioned_node_update(self, tempid=None):
        """
        Repeate test_repeated_node_update but passing a single session for
        all interactions to use
        """

        if not tempid:
            tempid = str(uuid.uuid4())

        with session_scope(self.driver.engine) as session:
            for tally in range(self.REPEAT_COUNT):
                properties = {'key1': None,
                              'key2': 2,
                              'key3': time.time(),
                              'rand':  random.random()}

                self.driver.node_merge(
                    node_id=tempid,
                    properties=properties,
                    session=session,
                    max_retries=int(1e6),
                    backoff=lambda x, y: time.sleep(random.randint(5, 30))
                )

            node = self.driver.node_lookup_one(node_id=tempid, session=session)
            self.assertEqual(properties, node.properties, 'Node properties'
                             'do not match expected properties')

        if not tempid:
            """if this is not part of another test, check the count"""
            self.verify_node_count(self.REPEAT_COUNT, tempid, voided=True)

    def test_concurrent_node_update_by_id(self):
        """
        Test that insertion of nodes is thread-safe and that retries
        succeed eventually
        """

        process_count = 3
        tempid = str(uuid.uuid4())
        processes = []

        for tally in range(process_count):
            processes.append(Process(
                target=self.test_repeated_node_update_properties_by_id,
                kwargs={'node_id': tempid})
            )

        for p in processes:
            p.start()

        for p in processes:
            p.join()

        self.verify_node_count(process_count*self.REPEAT_COUNT*2, tempid,
                               voided=True)

    def test_node_clobber(self):
        """
        Test that clobbering a node replaces all of it's properties
        """

        tempid = str(uuid.uuid4())

        propertiesA = {'key1':  None, 'key2':  3, 'key3':  time.time()}

        {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.node_merge(node_id=tempid, properties=propertiesA)

        propertiesB = {'key1':  True, 'key2':  0, 'key3':  time.time()}
        self.driver.node_clobber(node_id=tempid, properties=propertiesB)

        nodes = self.driver.node_lookup(node_id=tempid)
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, '
                         'instead found {count}'.format(count=len(nodes)))
        self.assertEqual(propertiesB, nodes[0].properties, 'Node properties do'
                         ' not match expected properties')

    def test_node_delete_property_keys(self):
        """
        Test the ability to remove property keys from nodes
        """

        tempid = str(uuid.uuid4())
        properties = {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.node_merge(node_id=tempid, properties=properties)

        self.driver.node_delete_property_keys(['key2', 'key3'], node_id=tempid)
        properties.pop('key2')
        properties.pop('key3')

        nodes = self.driver.node_lookup(node_id=tempid)
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, '
                         'instead found {count}'.format(count=len(nodes)))
        self.assertEqual(properties, nodes[0].properties)

    def test_node_delete_system_annotation_keys(self):
        """
        Test the ability to remove property keys from nodes
        """

        tempid = str(uuid.uuid4())
        annotations = {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.node_merge(node_id=tempid, system_annotations=annotations)

        self.driver.node_delete_system_annotation_keys(
            ['key2', 'key3'], node_id=tempid)

        annotations.pop('key2')
        annotations.pop('key3')

        nodes = self.driver.node_lookup(node_id=tempid)
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, '
                         'instead found {count}'.format(count=len(nodes)))
        self.assertEqual(annotations, nodes[0].system_annotations)

    def test_node_delete(self):
        """
        Test node deletion functionality
        """

        tempid = str(uuid.uuid4())
        self.driver.node_merge(node_id=tempid)
        self.driver.node_delete(node_id=tempid)

        nodes = self.driver.node_lookup(tempid)
        self.assertEqual(len(nodes), 0, 'Expected a single no non-voided nodes'
                         'to be found, instead found {count}'.format(
                             count=len(nodes)))

    def test_repeated_node_delete(self):
        """
        Test repeated node deletion correctness
        """

        node_id = str(uuid.uuid4())
        for i in range(self.REPEAT_COUNT):
            self.test_node_update_properties_by_id(node_id=node_id)
            self.driver.node_delete(node_id=node_id)
            self.assertIs(self.driver.node_lookup_one(node_id=node_id), None)

    def test_edge_merge_and_lookup(self):
        """
        Test edge creation and lookup by dst_id, src_id, dst_id and src_id
        """

        src_id = str(uuid.uuid4())
        dst_id = str(uuid.uuid4())
        self.driver.node_merge(node_id=src_id)
        self.driver.node_merge(node_id=dst_id)
        self.driver.edge_merge(src_id=src_id, dst_id=dst_id)

        edge = self.driver.edge_lookup_one(dst_id=dst_id)
        self.assertEqual(edge.src_id, src_id)
        self.assertEqual(edge.dst_id, dst_id)

        edge = self.driver.edge_lookup_one(src_id=src_id)
        self.assertEqual(edge.src_id, src_id)
        self.assertEqual(edge.dst_id, dst_id)

        edge = self.driver.edge_lookup_one(src_id=src_id, dst_id=dst_id)
        self.assertEqual(edge.src_id, src_id)
        self.assertEqual(edge.dst_id, dst_id)

    def test_edge_merge_and_lookup_properties(self):
        """
        Test edge property merging
        """
        src_id = str(uuid.uuid4())
        dst_id = str(uuid.uuid4())
        props = {'key1': str(random.random()), 'key2': random.random()}
        self.driver.node_merge(node_id=src_id)
        self.driver.node_merge(node_id=dst_id)
        self.driver.edge_merge(src_id=src_id, dst_id=dst_id, properties=props)
        edge = self.driver.edge_lookup_one(src_id=src_id, dst_id=dst_id)
        self.assertEqual(edge.src_id, src_id)
        self.assertEqual(edge.dst_id, dst_id)
        self.assertEqual(edge.properties, props)

    def test_edge_lookup_leaves(self):
        """
        Create a star topology, verify lookup by src_id and that all nodes
        are attached
        """

        leaf_count = 10
        src_id = str(uuid.uuid4())
        self.driver.node_merge(node_id=src_id)

        dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
        for dst_id in dst_ids:
            self.driver.node_merge(node_id=dst_id)
            self.driver.edge_merge(src_id=src_id, dst_id=dst_id)

        # edges = self.driver.edge_lookup(src_id=src_id)
        edge_ids = [e.dst_id for e in self.driver.edge_lookup(src_id=src_id)]
        self.assertEqual(len(set(edge_ids)), leaf_count)

        for dst_id in dst_ids:
            self.assertTrue(dst_id in set(edge_ids))

    def test_sessioned_path_insertion(self):
        """
        Test creation of a sample graph with pre-existing nodes in a
        single session
        """

        leaf_count = 10
        src_id = str(uuid.uuid4())
        dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
        self.driver.node_merge(node_id=src_id)

        with session_scope(self.driver.engine) as session:
            for dst_id in dst_ids:
                self.driver.node_merge(node_id=dst_id, session=session)

        with session_scope(self.driver.engine) as session:
            for dst_id in dst_ids:
                node = self.driver.node_lookup_one(
                    node_id=dst_id, session=session)
                self.driver.edge_merge(src_id=src_id, dst_id=node.node_id,
                                       session=session)

        edge_ids = [e.dst_id for e in self.driver.edge_lookup(
            src_id=src_id)]
        self.assertEqual(len(set(edge_ids)), leaf_count)

        for dst_id in dst_ids:
            self.assertTrue(dst_id in set(edge_ids))

    def test_path_deletion(self):
        """
        Test path deletion. Verify that nodes deletion is cascaded to edges
        """

        leaf_count = 10
        src_id = str(uuid.uuid4())
        dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
        self.driver.node_merge(node_id=src_id)

        # Create nodes and link them to source
        for dst_id in dst_ids:
            self.driver.node_merge(node_id=dst_id)
            self.driver.edge_merge(src_id=src_id, dst_id=dst_id)

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
        node_id = str(uuid.uuid4())
        self.driver.node_validator.validate = lambda x: False
        prop = {'key1': None, 'key2': 2, 'key3': time.time()}

        try:
            self.assertRaises(
                NodeCreationError,
                self.driver.node_merge, node_id, properties=prop
            )
        except:
            self.driver.node_validator.validate = lambda x: True
            raise

    def test_edge_validator_error(self):
        src_id = str(uuid.uuid4())
        dst_id = str(uuid.uuid4())
        self.driver.edge_validator.validate = lambda x: False
        self.driver.node_merge(src_id)
        self.driver.node_merge(dst_id)

        try:
            self.assertRaises(
                EdgeCreationError,
                self.driver.edge_merge, src_id, dst_id
            )
        except:
            self.driver.edge_validator.validate = lambda x: True
            raise


if __name__ == '__main__':

    def run_test(test):
        suite = unittest.TestLoader().loadTestsFromTestCase(test)
        unittest.TextTestRunner(verbosity=2).run(suite)

    run_test(TestPsqlGraphDriver)
