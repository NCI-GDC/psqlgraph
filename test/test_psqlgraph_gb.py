import uuid
import unittest
import logging
import psqlgraph
from psqlgraph import PsqlGraphDriver, session_scope, sanitizer
from multiprocessing import Process
import random
from sqlalchemy.exc import IntegrityError
from psqlgraph.exc import ValidationError, NodeCreationError
from psqlgraph.edge import PsqlEdge
from psqlgraph.node import PsqlNode

from datetime import datetime
import time
import yaml
import string
import os, sys

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'

logging.basicConfig(level=logging.INFO)

class TestPsqlGraphDriverGB(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger(__name__)
        self.driver = PsqlGraphDriver(host, user, password, database)
        self.driver_list = []
        for i in range(10):
            self.driver_list.append(PsqlGraphDriver(host, user, password, database))
        self.REPEAT_COUNT = 200
        with open(os.path.join(os.path.dirname(__file__), "quickbrown.yaml"), 'r') as unicode_file:
            self.yaml_conf = yaml.load(unicode_file)
        self.big_node_id_size = 10000
        self.big_node_label_size = 10000
        self.many_edge_count = 1000
        self.spam_driver_count = 1000

    def tearDown(self):
        self.driver.engine.dispose()
        for driver in self.driver_list:
            driver.engine.dispose()

    def _clear_tables(self):
        conn = self.driver.engine.connect()
        conn.execute('commit')
        conn.execute('delete from edges')
        conn.execute('delete from nodes')
        conn.close()

    #
    # Glass Box tests - joe sislow/mad
    #

    def test_create_node_with_unicode_name(self):
        """Test unicode as node name"""

        temp_id = str(uuid.uuid4())
        self.driver.node_merge(node_id=temp_id, label=self.yaml_conf['Greek'])

        node_by_id = self.driver.node_lookup_one(temp_id)
        node_by_label = self.driver.node_lookup_one(label=self.yaml_conf['Greek'])
        self.assertEqual(node_by_id.node_id, temp_id)
        self.assertEqual(node_by_label.node_id, temp_id)
        self.driver.node_delete(node_id=temp_id)

    def test_create_node_with_unicode_properties(self):
        """Test unicode as node properties"""

        temp_id = str(uuid.uuid4())
        properties = {'key1': self.yaml_conf['Hungarian'], 'key2': 2, 'key3': datetime.now()}
        self.driver.node_merge(node_id=temp_id, label='test_unicode', properties=properties)

        node_by_id = self.driver.node_lookup_one(temp_id)
        node_by_label = self.driver.node_lookup_one(label='test_unicode')
        self.assertEqual(node_by_id.node_id, temp_id)
        self.assertEqual(node_by_label.node_id, temp_id)
        self.driver.node_delete(node_id=temp_id)

    def test_create_edge_with_unicode_name(self):
        """Test unicode as node name"""

        temp_id = str(uuid.uuid4())
        self.driver.node_merge(node_id=temp_id, label='test_unicode_edge')
        self.driver.edge_insert(PsqlEdge(src_id=temp_id, dst_id=temp_id, label=self.yaml_conf['Japanese']))

        edge_by_id = self.driver.edge_lookup_one(dst_id=temp_id)
        self.assertEqual(edge_by_id.dst_id, temp_id)
        self.driver.node_delete(node_id=temp_id)

    def test_create_edge_with_unicode_properties(self):
        """Test unicode as node properties"""

        temp_id = str(uuid.uuid4())
        properties = {'key1': self.yaml_conf['Polish'], 'key2': 2, 'key3': datetime.now()}
        self.driver.node_merge(node_id=temp_id, label='test_unicode_edge')
        self.driver.edge_insert(PsqlEdge(src_id=temp_id, dst_id=temp_id, label='test_unicode_edge', properties=properties))

        edge_by_id = self.driver.edge_lookup_one(dst_id=temp_id)
        self.assertEqual(edge_by_id.dst_id, temp_id)
        self.assertEqual(edge_by_id.properties['key1'], properties['key1'])
        self.driver.node_delete(node_id=temp_id)

    def test_create_node_with_large_label(self):
        """ Test creating a node with a very large label"""
        temp_id = str(uuid.uuid4())
        rand_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(self.big_node_id_size))
        self.driver.node_merge(node_id=temp_id, label=rand_string)

        node_by_id = self.driver.node_lookup_one(temp_id)
        node_by_label = self.driver.node_lookup_one(label=rand_string)
        self.assertEqual(node_by_id.node_id, temp_id)
        self.assertEqual(node_by_label.node_id, temp_id)
        self.driver.node_delete(node_id=temp_id)

    def test_create_node_with_large_id(self):
        """Test creating a node with a very large ID"""
        temp_id = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(self.big_node_label_size))
        self.driver.node_merge(node_id=temp_id, label='test_large_id')

        node_by_id = self.driver.node_lookup_one(temp_id)
        node_by_label = self.driver.node_lookup_one(label='test_large_id')
        self.assertEqual(node_by_id.node_id, temp_id)
        self.assertEqual(node_by_label.node_id, temp_id)
        self.driver.node_delete(node_id=temp_id)

    def test_create_node_with_many_edges(self):
        """Test creating a node with a whole bunch of edges"""
        src_id = str(uuid.uuid4())
        dst_id = str(uuid.uuid4())
        with self.driver.session_scope() as session:
            self.driver.node_merge(node_id=src_id, label='test', session=session)
            self.driver.node_merge(node_id=dst_id, label='test', session=session)
            for n in range(self.many_edge_count):
                edge_name = "test%s" % n
                self.driver.edge_insert(PsqlEdge(src_id=src_id, dst_id=dst_id, label=edge_name), session=session)       

            edge_labels = [e.label for e in self.driver.edge_lookup(src_id=src_id, session=session)]
        self.assertEqual(len(set(edge_labels)), self.many_edge_count)

    def test_check_node_dual_parameters(self):
        """Test checking add with both node and id"""
        src_id = str(uuid.uuid4())
        src_id2 = str(uuid.uuid4())
        self.assertRaises(NodeCreationError, self.driver.node_merge, node_id=src_id, node=PsqlNode(node_id=src_id2, label='test2'), label='test')

    def test_spam_drivers(self):
        """Test spamming various driver connections"""
        check_node_id = ""
        for i in range(self.spam_driver_count):
            cur_driver = self.driver_list[random.randint(0, len(self.driver_list) - 1)]
            node_id = str(uuid.uuid4())
            if i == len(self.driver_list) - 2:
                check_node_id = node_id
            node_name = "test_%d" % i
            cur_driver.node_merge(node_id=node_id, label=node_name)

        new_node = self.driver.node_lookup_one(node_id=check_node_id)
        test_name = "test_%d" % (len(self.driver_list) - 2)
        self.assertEquals(new_node.label, test_name)

    def test_sneaky_node_delete(self):
        node_id = str(uuid.uuid4())
        cur_node = PsqlNode(node_id=node_id, label='test_node')
        cur_node2 = cur_node.copy()
        #cur_node2 = PsqlNode(cur_node)
        self.driver.node_merge(cur_node)
        self.driver.node_delete(cur_node)
        self.driver.node_delete(cur_node2)

        self.assertEqual(cur_node, cur_node2)

    def test_multi_driver_session_add(self):
        """Test adding to multiple drivers in the same session"""
        with self.driver.session_scope() as session:
            node_id = str(uuid.uuid4())
            node_name = "test_node" 
            for i in range(len(self.driver_list)):
                self.driver_list[i].node_merge(node_id=node_id, label=node_name)

        new_node = self.driver.node_lookup(node_id=node_id)
        self.assertEquals(len(new_node.all()), 1)

if __name__ == '__main__':

    def run_test(test):
        suite = unittest.TestLoader().loadTestsFromTestCase(test)
        unittest.TextTestRunner(verbosity=2).run(suite)

    run_test(TestPsqlGraphDriverGB)
