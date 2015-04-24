import unittest
import logging
from psqlgraph import psqlgraph2neo4j
import py2neo
import os
from models import Test, Edge1
from psqlgraph import Edge, Node, PolyNode
from py2neo.packages.httpstream import http
http.socket_timeout = 9999
from psqlgraph import PolyEdge,Edge, Node

import uuid
import shutil
import subprocess
import time
from models import Test, Edge1

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'

driver = psqlgraph2neo4j.PsqlGraph2Neo4j()
driver.connect_to_psql(host, user, password, database)

logging.basicConfig(level=logging.INFO)

class Test_psql2neo(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        dirname = os.path.dirname
        script = os.path.join(
            dirname(dirname(__file__)),
            'bin/csv_to_neo.sh')
        subprocess.call('bash %s setup' % script, shell=True)

    @classmethod
    def tearDownClass(cls):
        dirname = os.path.dirname
        try:
            shutil.rmtree(
                os.path.join(dirname(dirname(__file__)), 'batch_importer'))

    def setUp(self):
        self.logger = logging.getLogger(__name__)
        self.driver = driver
        self.data_dir = None
        self.root_dir = None
        self.csv_dir = None
        self.neo4j_script = None
        self.batch_script = None
        self.psqlDriver = self.driver.psqlgraphDriver
        self._clear_tables()

    def tearDown(self):
        self.psqlDriver.engine.dispose()
        shutil.rmtree(self.get_data_dir())

    def _clear_tables(self):
        conn = self.psqlDriver.engine.connect()
        conn.execute('commit')
        for table in Node().get_subclass_table_names():
            if table != Node.__tablename__:
                conn.execute('delete from {}'.format(table))
        for table in Edge().get_subclass_table_names():
            if table != Edge.__tablename__:
                conn.execute('delete from {}'.format(table))

        conn.execute('delete from _voided_nodes')
        conn.execute('delete from _voided_edges')
        conn.close()

    def get_data_dir(self):
        if not self.data_dir:
            dirname = os.path.dirname
            self.data_dir = os.path.join(
                dirname(dirname(__file__)),
                'neo4j-community-2.1.6/data/graph.db')
            if not os.path.exists(self.data_dir):
                os.makedirs(self.data_dir)
        return self.data_dir

    def get_batch_script(self):
        if not self.batch_script:
            self.batch_script = os.path.join(
                self.get_root_dir(), 'bin/csv_to_neo.sh')
        return self.batch_script

    def get_root_dir(self):
        if not self.root_dir:
            dirname = os.path.dirname
            self.root_dir = dirname(dirname(__file__))
        return self.root_dir

    def get_csv_dir(self):
        if not self.csv_dir:
            self.csv_dir = os.path.join(self.get_root_dir(), 'csv')
            if not os.path.exists(self.csv_dir):
                os.makedirs(self.csv_dir)
        return self.csv_dir

    def get_neo4j_script(self):
        if not self.neo4j_script:
            self.neo4j_script = os.path.join(self.get_root_dir(),
                                             'neo4j-community-2.1.6/bin/neo4j')
        return self.neo4j_script

    def batch_import(self):
        data_dir = self.get_data_dir()
        csv_dir = self.get_csv_dir()
        script = self.get_batch_script()
        print script, csv_dir, data_dir
        subprocess.call('%s -s %s -d %s -b batch_importer convert' % (
            script, csv_dir, data_dir), shell=True)
        subprocess.call([self.get_neo4j_script(), 'stop'])
        r = subprocess.call([self.get_neo4j_script(), 'start'])
        if r == 2:
            subprocess.call([self.get_neo4j_script(), 'start-no-wait'])
            time.sleep(20)

    def test_neo_single_node(self):
        self._clear_tables()
        node_id = str(uuid.uuid4())
        timestamp = long(time.time())
        props = {'timestamp': timestamp}
        test_props = {
            'id': node_id, 'timestamp': (timestamp)
        }
        with self.psqlDriver.session_scope():
            self.psqlDriver.node_merge(node_id, label='test', properties=props)
            node_properties = [ {'name':'test_properties',
                'fields':[{'name':'timestamp','type':'long'}]} ]

            self.driver.export(self.get_csv_dir(),node_properties)
        self.batch_import()

        self.neo4jDriver = py2neo.Graph()
        nodes = self.neo4jDriver.cypher.execute('match (n:test) return n')
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].n.properties, test_props)

    def test_neo_many_node(self):
        self._clear_tables()
        count = 200
        test_props = []
        with self.psqlDriver.session_scope():
            for i in range(count):
                node_id = str(uuid.uuid4())
                timestamp = long(time.time())
                props = {'timestamp': timestamp}
                test_props.append({
                    'id': node_id, 'timestamp': (timestamp)
                })
                self.psqlDriver.node_merge(node_id, label='test', properties=props)

            node_properties = [ {'name':'test_properties',
                'fields':[{'name':'timestamp','type':'long'}]} ]
            self.driver.export(self.get_csv_dir(),node_properties)
        self.batch_import()

        self.neo4jDriver = py2neo.Graph()
        nodes = self.neo4jDriver.cypher.execute('match (n:test) return n')
        self.assertEqual(len(nodes), count)
        node_props = [n.n.properties for n in nodes]
        for prop in test_props:
            self.assertTrue(prop in node_props)
        for node_prop in node_props:
            self.assertTrue(node_prop in test_props)

    def test_neo_single_path(self):
        self._clear_tables()
        src_id = str(uuid.uuid4())
        dst_id = str(uuid.uuid4())

        with self.psqlDriver.session_scope():
            self.psqlDriver.node_merge(node_id=src_id, label='test')
            self.psqlDriver.node_merge(node_id=dst_id, label='test')
            self.psqlDriver.edge_insert(
                PolyEdge(src_id=src_id, dst_id=dst_id, label='edge1'))
            node_properties = [ {'name':'test_properties',
                'fields':[]} ]

            self.driver.export(self.get_csv_dir(),node_properties)
        self.batch_import()


        self.neo4jDriver = py2neo.Graph()
        self.neo4jDriver = py2neo.Graph()
        nodes = self.neo4jDriver.cypher.execute('match (n:test) return n')
        self.assertEqual(len(nodes), 2)

        nodes = self.neo4jDriver.cypher.execute("""
        match (n)-[r]-(m)
        where n.id = "{src_id}" and m.id = "{dst_id}"
        return n
        """.format(src_id=src_id, dst_id=dst_id))

        self.assertEqual(len(nodes), 1)
        nodes = self.neo4jDriver.cypher.execute("""
        match (n)-[r]-(m)
        where n.id = "{dst_id}" and m.id = "{src_id}"
        return n
        """.format(src_id=src_id, dst_id=dst_id))

        self.assertEqual(len(nodes), 1)
        edges = self.neo4jDriver.cypher.execute("""
        match (n)-[r]-(m)
        where n.id = "{dst_id}" and m.id = "{src_id}"
        return r
        """.format(src_id=src_id, dst_id=dst_id))

        self.assertEqual(len(edges), 1)

    def test_neo_star_topology(self):
        """
        Create a star topology, verify lookup by src_id and that all nodes
        are attached
        """

        self._clear_tables()
        leaf_count = 10
        src_id = str(uuid.uuid4())
        with self.psqlDriver.session_scope():
            self.psqlDriver.node_merge(node_id=src_id, label='test')

            dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
            for dst_id in dst_ids:
                self.psqlDriver.node_merge(node_id=dst_id, label='test')
                self.psqlDriver.edge_insert(PolyEdge(
                    src_id=src_id, dst_id=dst_id, label='edge1'))
            node_properties = [ {'name':'test_properties',
                'fields':[]} ]

            self.driver.export(self.get_csv_dir(),node_properties)
        self.batch_import()

        self.neo4jDriver = py2neo.Graph()
        nodes = self.neo4jDriver.cypher.execute(
            'match (n)-[r]-(m) where n.id = "{src_id}" return n'.format(
                src_id=src_id)
        )
        self.assertEqual(len(nodes), leaf_count)

    def _create_subtree(self, parent_id, level):
        for i in range(5):
            node_id = str(uuid.uuid4())
            self.psqlDriver.node_merge(node_id=node_id, label='test')
            self.psqlDriver.edge_insert(PolyEdge(
                src_id=parent_id, dst_id=node_id, label='edge1'
            ))
            if level < 2:
                self._create_subtree(node_id, level + 1)

    def test_neo_tree_topology(self):
        """
        Create a tree topology, verify lookup by src_id and that all nodes
        are attached
        """

        self._clear_tables()
        node_id = str(uuid.uuid4())
        with self.psqlDriver.session_scope():
            self.psqlDriver.node_merge(node_id=node_id, label='test')
            self._create_subtree(node_id, 0)
            node_properties = [ {'name':'test_properties',
                'fields':[]} ]

            self.driver.export(self.get_csv_dir(),node_properties)
            self.batch_import()


if __name__ == '__main__':

    def run_test(test):
        suite = unittest.TestLoader().loadTestsFromTestCase(test)
        unittest.TextTestRunner(verbosity=2).run(suite)

    run_test(Test_psql2neo)
