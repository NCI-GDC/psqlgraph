import uuid
import unittest
import time
from cdisutils import log
from PsqlGraph import PsqlGraphDriver, session_scope
from PsqlGraph.setup_psql_graph import setup_database, create_tables, \
    try_drop_test_data
from multiprocessing import Process
import logging
import random

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'
table = 'test'


logging.basicConfig(level=logging.WARN)


class TestPsqlGraphSetup(unittest.TestCase):

    def setUp(self):
        self.logger = log.get_logger(__name__)

    def test_setup(self):
        setup_database(user, password, database)
        create_tables(host, user, password, database)


class TestPsqlGraphTeardown(unittest.TestCase):

    def setUp(self):
        self.logger = log.get_logger(__name__)

    def test_teardown(self):
        time.sleep(1)
        try_drop_test_data(user, database)


class TestPsqlGraphDriver(unittest.TestCase):

    def setUp(self):
        self.logger = log.get_logger(__name__)
        self.driver = PsqlGraphDriver(host, user, password, database)

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

    def test_connect_to_node_table(self):
        self.driver.connect_to_table('nodes')
        self.assertTrue(self.driver.is_connected())

    def test_node_merge_and_lookup(self):
        tempid = str(uuid.uuid4())
        properties = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.node_merge(node_id=tempid, properties=properties)

        node = self.driver.node_lookup_one(tempid)
        self.assertEqual(properties, node.properties)

    def test_node_update(self):

        tempid = str(uuid.uuid4())

        # Add first node
        propertiesA = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.node_merge(node_id=tempid, properties=propertiesA)

        # Add second node
        propertiesB = {'key1': None, 'new_key': 2, 'timestamp': time.time()}
        self.driver.node_merge(node_id=tempid, properties=propertiesB)

        # Merge properties
        for key, val in propertiesA.iteritems():
            propertiesB[key] = val

        # Test that there is only 1 non-void node with tempid and property
        # equality
        node = self.driver.node_lookup_one(tempid)
        self.assertEqual(propertiesB, node.properties)

        # Test to make sure that there are 2 voided nodes with tempid
        nodes = self.verify_node_count(2, node_id=tempid, voided=True)
        self.assertEqual(propertiesB, nodes[1].properties)

    def test_repeated_node_update(self, tempid=None):
        """-

        Verify that updates repeated updates to a single node create
        the correct number of voided transactions and a single valid
        node with the correct properties

        """

        update_count = 200

        if not tempid:
            tempid = str(uuid.uuid4())

        for tally in range(update_count):
            properties = {'key1': None,
                          'key2': 2,
                          'key3': time.time(),
                          'rand':  random.random()}
            self.driver.node_merge(node_id=tempid, properties=properties)

        node = self.driver.node_lookup_one(tempid)
        self.assertEqual(properties, node.properties, 'Node properties'
                         'do not match expected properties')
        self.verify_node_count(update_count, node_id=tempid, voided=True)

    def test_sessioned_node_update(self, tempid=None):
        """

        Repeate test_repeated_node_update but passing a single session for
        all interactions to use

        """

        update_count = 200

        if not tempid:
            tempid = str(uuid.uuid4())

        with session_scope(self.driver.engine) as session:
            for tally in range(update_count):
                properties = {'key1': None,
                              'key2': 2,
                              'key3': time.time(),
                              'rand':  random.random()}
                self.driver.node_merge(node_id=tempid, properties=properties,
                                       session=session)

            node = self.driver.node_lookup_one(node_id=tempid, session=session)
            self.assertEqual(properties, node.properties, 'Node properties'
                             'do not match expected properties')

        self.verify_node_count(update_count, tempid, voided=True)

    def test_concurrent_node_update(self):

        process_count = 3
        tempid = str(uuid.uuid4())

        processes = []

        for tally in range(process_count):
            processes.append(Process(target=self.test_repeated_node_update,
                                     kwargs={'tempid': tempid}))

        for p in processes:
            p.start()

        for p in processes:
            p.join()

        self.verify_node_count(process_count*200, tempid, voided=True)

    def test_sessioned_concurrent_node_update(self):

        process_count = 10
        tempid = str(uuid.uuid4())

        processes = []

        for tally in range(process_count):
            p = Process(target=self.test_sessioned_node_update,
                        kwargs={'tempid': tempid})
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        self.verify_node_count(process_count*200, tempid, voided=True)

    def test_node_clobber(self):

        tempid = str(uuid.uuid4())

        propertiesA =

        {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.node_merge(node_id=tempid, properties=propertiesA)

        propertiesB = {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.node_clobber(tempid, properties=propertiesB)

        nodes = self.driver.node_lookup(tempid)
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, '
                         'instead found {count}'.format(count=len(nodes)))
        self.assertEqual(propertiesB, nodes[0].properties, 'Node properties do'
                         ' not match expected properties')

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

    def test_node_delete(self):

        tempid = str(uuid.uuid4())

        self.driver.node_merge(node_id=tempid)
        self.driver.node_delete(tempid)

        nodes = self.driver.node_lookup_one(tempid)
        self.assertEqual(len(nodes), 0, 'Expected a single no non-voided nodes'
                         'to be found, instead found {count}'.format(
                             count=len(nodes)))

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

    def test_edge_merge_and_lookup(self):
        self.driver.connect_to_table('edges')

        tempid = str(uuid.uuid4())
        properties = {'key1': None, 'key2': 2, 'key3': time.time()}
        self.driver.edge_merge(node_id=tempid, properties=properties)

        edges = self.driver.edge_lookup(tempid)
        self.assertEqual(len(edges), 1, 'Expected a single edge to be found, '
                         'instead found {count}'.format(count=len(edges)))
        self.assertTrue(cmp(properties, edges[0].properties) == 0)

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

    def test_edge_clobber(self):

        tempid = str(uuid.uuid4())

        propertiesA = {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.edge_merge(node_id=tempid, properties=propertiesA)

        propertiesB = {'key1':  None, 'key2':  2, 'key3':  time.time()}
        self.driver.edge_clobber(tempid, properties=propertiesB)

        edges = self.driver.edge_lookup(tempid)
        self.assertEqual(len(edges), 1, 'Expected a single edge to be found, instead found {count}'.format(count=len(edges)))
        self.assertEqual(propertiesB, edges[0].properties, 'Edge properties do not match expected properties')

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

    def test_edge_delete(self):

        tempid = str(uuid.uuid4())

        self.driver.edge_merge(node_id=tempid)
        self.driver.edge_delete(tempid)

        edges = self.driver.edge_lookup(tempid)
        self.assertEqual(len(edges), 0, 'Expected a no non-voided edges to be found, instead found {count}'.format(count=len(edges)))

        edges = self.driver.edge_lookup(tempid)
        self.assertEqual(len(edges), 0, 'Expected a single no non-voided edges to be found, instead found {count}'.format(count=len(edges)))

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