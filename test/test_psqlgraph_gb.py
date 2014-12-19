import uuid
import unittest
import logging
import psqlgraph
from psqlgraph import PsqlGraphDriver, session_scope, sanitizer
from multiprocessing import Process
import random
from sqlalchemy.exc import IntegrityError
from psqlgraph.exc import ValidationError

from datetime import datetime
import time
import yaml
import string

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


logging.basicConfig(level=logging.INFO)


class TestPsqlGraphDriverGB(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger(__name__)
        self.driver = PsqlGraphDriver(host, user, password, database)
        self.REPEAT_COUNT = 200
        with open("quickbrown.yaml", 'r') as unicode_file:
            self.yaml_conf = yaml.load(unicode_file)

    def _clear_tables(self):
        conn = self.driver.engine.connect()
        conn.execute('commit')
        conn.execute('delete from edges')
        conn.execute('delete from nodes')
        conn.close()

    #
    # Glass Box tests - joe sislow
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
        self.driver.edge_merge(src_id=temp_id, dst_id=temp_id, label=self.yaml_conf['Hebrew'])

        edge_by_id = self.driver.edge_lookup_one(dst_id=temp_id)
        self.assertEqual(edge_by_id.dst_id, temp_id)
        self.driver.node_delete(node_id=temp_id)


    def test_create_edge_with_unicode_properties(self):
        """Test unicode as node properties"""

        temp_id = str(uuid.uuid4())
        properties = {'key1': self.yaml_conf['Polish'], 'key2': 2, 'key3': datetime.now()}
        self.driver.node_merge(node_id=temp_id, label='test_unicode_edge')
        self.driver.edge_merge(src_id=temp_id, dst_id=temp_id, label='test_unicode_edge', properties=properties)

        edge_by_id = self.driver.edge_lookup_one(dst_id=temp_id)
        self.assertEqual(edge_by_id.dst_id, temp_id)
        self.assertEqual(edge_by_id.properties['key1'], properties['key1'])
        self.driver.node_delete(node_id=temp_id)

    def test_create_node_with_large_label(self):
        temp_id = str(uuid.uuid4())
        rand_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(1000000))
        self.driver.node_merge(node_id=temp_id, label=rand_string)

        node_by_id = self.driver.node_lookup_one(temp_id)
        node_by_label = self.driver.node_lookup_one(label=rand_string)
        self.assertEqual(node_by_id.node_id, temp_id)
        self.assertEqual(node_by_label.node_id, temp_id)
        self.driver.node_delete(node_id=temp_id)


    def test_create_node_with_large_id(self):
        temp_id = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(1000))
        self.driver.node_merge(node_id=temp_id, label='test_large_id')

        node_by_id = self.driver.node_lookup_one(temp_id)
        node_by_label = self.driver.node_lookup_one(label='test_large_id')
        self.assertEqual(node_by_id.node_id, temp_id)
        self.assertEqual(node_by_label.node_id, temp_id)
        self.driver.node_delete(node_id=temp_id)

    def test_create_node_with_many_edges(self):
        src_id = str(uuid.uuid4())
        dst_id = str(uuid.uuid4())
        props = {'key1': str(random.random()), 'key2': random.random()}
        self.driver.node_merge(node_id=src_id, label='test')
        self.driver.node_merge(node_id=dst_id, label='test')
        count = 100000000
        for n in range(count):
            edge_name = "test%s" % n
            self.driver.edge_merge(src_id=src_id, dst_id=dst_id, label=edge_name)

        edge_ids = [e.dst_id for e in self.driver.edge_lookup(
            src_id=src_id)]
        self.assertEqual(len(set(edge_ids)), count)


if __name__ == '__main__':

    def run_test(test):
        suite = unittest.TestLoader().loadTestsFromTestCase(test)
        unittest.TextTestRunner(verbosity=2).run(suite)

    run_test(TestPsqlGraphDriverGB)
