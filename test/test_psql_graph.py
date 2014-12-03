import uuid
import unittest
import time
from cdisutils import log
import psqlgraph
from psqlgraph import PsqlGraphDriver, session_scope
from psqlgraph.setup_psql_graph import setup_database, create_tables
from multiprocessing import Process
import logging
import random
from sqlalchemy.exc import IntegrityError
import json

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


logging.basicConfig(level=logging.WARN)


class TestPsqlGraphSetup(unittest.TestCase):

    def setUp(self):
        self.logger = log.get_logger(__name__)

    @unittest.skip('Will interfere with other tests')
    def test_setup(self):
        setup_database(user, password, database)
        create_tables(host, user, password, database)


class TestPsqlGraphDriver(unittest.TestCase):

    def setUp(self):
        self.logger = log.get_logger(__name__)
        self.driver = PsqlGraphDriver(host, user, password, database)
        self.REPEAT_COUNT = 200

    def test_sanitize_int(self):
        """ Test sanitization of castable integer type"""
        self.assertEqual(psqlgraph.Sanitizer.cast(5), 5)

    def test_sanitize_str(self):
        """ Test sanitization of castable string type"""
        self.assertEqual(psqlgraph.Sanitizer.cast('test'), 'test')

    def test_sanitize_dict(self):
        """ Test sanitization of castable dictionary type"""
        test = {'first': 1, 'second': 2, 'third': 'This is a test'}
        self.assertEqual(psqlgraph.Sanitizer.cast(test), json.dumps(test))

    def test_sanitize_other(self):
        """ Test sanitization of select non-standard types"""
        A = psqlgraph.QueryError
        self.assertEqual(psqlgraph.Sanitizer.cast(A), str(A))
        B = logging
        self.assertEqual(psqlgraph.Sanitizer.cast(B), str(B))

    def test_sanitize(self):
        """ Test sanitization of select non-standard types"""
        self.assertEqual(psqlgraph.Sanitizer.sanitize({
            'key1': 'First', 'key2': 25,
            'nested': {'nestedkey1': "First's", 'nestedkey2': 25},
            'other': psqlgraph.Sanitizer,
        }), {
            'key1': 'First', 'key2': 25,
            'nested': '{"nestedkey1": "First\'s", "nestedkey2": 25}',
            'other': str(psqlgraph.Sanitizer),
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

    def test_node_update_properties_by_id(self, node_id=None):
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
        self.driver.node_merge(node_id=node_id, properties=propertiesA)

        # Add second node
        propertiesB = {'key1': None, 'new_key': 2, 'timestamp': time.time()}
        self.driver.node_merge(node_id=node_id, properties=propertiesB)

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
        self.driver.node_merge(
            property_matches=propertiesA, properties=propertiesB
        )

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
        self.assertRaises(
            psqlgraph.ProgrammingError,
            self.driver.node_void,
            None
        )

    def test_null_node_merge(self):
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

    @unittest.skip('Not implemented')
    def test_node_clobber(self):

        tempid = str(uuid.uuid4())

        propertiesA = {'key1':  None, 'key2':  3, 'key3':  time.time()}

        {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.node_merge(node_id=tempid, properties=propertiesA)

        propertiesB = {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.node_clobber(tempid, properties=propertiesB)

        nodes = self.driver.node_lookup(tempid)
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, '
                         'instead found {count}'.format(count=len(nodes)))
        self.assertEqual(propertiesB, nodes[0].properties, 'Node properties do'
                         ' not match expected properties')

    @unittest.skip('Not implemented')
    def test_node_delete_property_keys(self):

        tempid = str(uuid.uuid4())
        properties = {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.node_merge(node_id=tempid, properties=properties)

        self.driver.node_delete_property_keys(tempid, ['key2', 'key3'])
        properties.pop('key2')
        properties.pop('key3')

        nodes = self.driver.node_lookup(tempid)
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, '
                         'instead found {count}'.format(count=len(nodes)))
        self.assertEqual(properties, nodes[0].properties)

    @unittest.skip('Not implemented')
    def test_node_delete_system_annotation_keys(self):

        tempid = str(uuid.uuid4())
        annotations = {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.node_merge(node_id=tempid, system_annotations=annotations)

        self.driver.node_delete_system_annotations_keys(
            tempid, ['key2', 'key3'])

        annotations.pop('key2')
        annotations.pop('key3')

        nodes = self.driver.node_lookup(tempid)
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, '
                         'instead found {count}'.format(count=len(nodes)))
        self.assertEqual(annotations, nodes[0].properties)

    @unittest.skip('Not implemented')
    def test_node_delete(self):

        tempid = str(uuid.uuid4())

        self.driver.node_merge(node_id=tempid)
        self.driver.node_delete(tempid)

        nodes = self.driver.node_lookup_one(tempid)
        self.assertEqual(len(nodes), 0, 'Expected a single no non-voided nodes'
                         'to be found, instead found {count}'.format(
                             count=len(nodes)))

    @unittest.skip('Not implemented')
    def test_repeated_node_delete(self):

        tempid = str(uuid.uuid4())
        void_count = 5

        for i in range(void_count):
            self.driver.node_merge(node_id=tempid)
            self.driver.node_delete(tempid)

        nodes = self.driver.node_lookup_one(tempid, include_voided=False)
        self.assertEqual(len(nodes), 0, 'Expected a no non-voided nodes to be'
                         ' found, instead found {count}'.format(
                             count=len(nodes)))

        nodes = self.driver.node_lookup(tempid, include_voided=True)
        self.assertEqual(len(nodes), void_count - 1, 'Expected a single {exp}'
                         'non-voided nodes to be found, instead found {real}'
                         ''.format(exp=void_count, real=len(nodes)))

    @unittest.skip('Not implemented')
    def test_edge_merge_and_lookup(self):
        self.driver.connect_to_table('edges')

        tempid = str(uuid.uuid4())
        properties = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.edge_merge(node_id=tempid, properties=properties)

        edges = self.driver.edge_lookup(tempid)
        self.assertEqual(len(edges), 1, 'Expected a single edge to be found, '
                         'instead found {count}'.format(count=len(edges)))
        self.assertTrue(cmp(properties, edges[0].properties) == 0)

    @unittest.skip('Not implemented')
    def test_edge_update(self):
        self.driver.connect_to_table('edges')

        tempid = str(uuid.uuid4())

        propertiesA = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.edge_merge(node_id=tempid, properties=propertiesA)

        propertiesB = {'key1': None, 'new_key': 2, 'timestamp': time.time()}
        self.driver.edge_merge(node_id=tempid, properties=propertiesB)

        edges = self.driver.edge_lookup_one(tempid)
        self.assertEqual(propertiesA, edges[0].properties, 'Edge properties do'
                         ' not match expected properties')

        edges = self.driver.edge_lookup(tempid, include_voided=True)
        self.assertEqual(len(edges), 1, 'Expected a single voided edge to be '
                         'found, instead found {count}'.format(
                             count=len(edges)))
        self.assertEqual(propertiesB, edges[0].properties, 'Edge properties do'
                         ' not match expected properties')

    @unittest.skip('Not implemented')
    def test_repeated_edge_update(self):
        self.driver.connect_to_table('edges')

        update_count = 5000

        tempid = str(uuid.uuid4())

        for tally in range(update_count):
            properties = {'key1': None, 'key2': 2, 'key3': time.time(),
                          'tally':  tally}
            self.driver.edge_merge(node_id=tempid, properties=properties)

        edges = self.driver.edge_lookup(tempid)
        self.assertEqual(len(edges), 1, 'Expected a single non-voided edge to '
                         'be found, instead found {count}'.format(
                             count=len(edges)))
        self.assertEqual(properties, edges[0].properties, 'Edge properties do '
                         'not match expected properties')

        edges = self.driver.edge_lookup(tempid, voided=True)
        self.assertEqual(len(edges), update_count - 1)

    @unittest.skip('Not implemented')
    def test_edge_clobber(self):

        tempid = str(uuid.uuid4())

        propertiesA = {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.edge_merge(node_id=tempid, properties=propertiesA)

        propertiesB = {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.edge_clobber(tempid, properties=propertiesB)

        edges = self.driver.edge_lookup(tempid)
        self.assertEqual(len(edges), 1, 'Expected a single edge to be found, instead found {count}'.format(count=len(edges)))
        self.assertEqual(propertiesB, edges[0].properties, 'Edge properties do not match expected properties')

    @unittest.skip('Not implemented')
    def test_edge_delete_property_keys(self):

        tempid = str(uuid.uuid4())
        properties = {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.edge_merge(node_id=tempid, properties=properties)

        self.driver.edge_delete_property_keys(tempid, ['key2', 'key3'])
        properties.pop('key2')
        properties.pop('key3')

        edges = self.driver.edge_lookup(tempid)
        self.assertEqual(len(edges), 1, 'Expected a single edge to be found, instead found {count}'.format(count=len(edges)))
        self.assertEqual(properties, edges[0].properties)

    @unittest.skip('Not implemented')
    def test_edge_delete_system_annotation_keys(self):

        tempid = str(uuid.uuid4())
        annotations = {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.edge_merge(node_id=tempid, system_annotations=annotations)

        self.driver.edge_delete_system_annotations_keys(
            tempid, ['key2', 'key3'])

        annotations.pop('key2')
        annotation.pop('key3')

        edges = self.driver.edge_lookup(tempid)
        self.assertEqual(len(edges), 1, 'Expected a single edge to be found, instead found {count}'.format(count=len(edges)))
        self.assertEqual(properties, edges[0].properties)

    @unittest.skip('Not implemented')
    def test_edge_delete(self):

        tempid = str(uuid.uuid4())

        self.driver.edge_merge(node_id=tempid)
        self.driver.edge_delete(tempid)

        edges = self.driver.edge_lookup(tempid)
        self.assertEqual(len(edges), 0, 'Expected a no non-voided edges to be found, instead found {count}'.format(count=len(edges)))

        edges = self.driver.edge_lookup(tempid)
        self.assertEqual(len(edges), 0, 'Expected a single no non-voided edges to be found, instead found {count}'.format(count=len(edges)))

    @unittest.skip('Not implemented')
    def test_repeated_edge_delete(self):

        tempid = str(uuid.uuid4())
        void_count = 5

        for i in range(void_count):
            self.driver.edge_merge(node_id=tempid)
            self.driver.edge_delete(tempid)

        edges = self.driver.edge_lookup(tempid, include_voided=False)
        self.assertEqual(len(edges), 0, 'Expected a no non-voided edges to be found, instead found {count}'.format(count=len(edges)))

        edges = self.driver.edge_lookup(tempid, include_voided=True)
        self.assertEqual(len(edges), void_count - 1, 'Expected a single {exp} non-voided edges to be found, instead found {real}'.format(
                        exp=void_count, real=len(edges)))


if __name__ == '__main__':

    def run_test(test):
        suite = unittest.TestLoader().loadTestsFromTestCase(test)
        unittest.TextTestRunner(verbosity=2).run(suite)

    run_test(TestPsqlGraphSetup)
    run_test(TestPsqlGraphDriver)
