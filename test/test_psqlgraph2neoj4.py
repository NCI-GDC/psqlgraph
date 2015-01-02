from datetime import datetime
import unittest
import logging
from psqlgraph import psqlgraph2neo4j
import uuid

host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


logging.basicConfig(level=logging.INFO)


class Test_psql2neo(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger(__name__)
        self.driver = psqlgraph2neo4j.PsqlGraph2Neo4j()
        self.driver.connect_to_psql(host, user, password, database)
        self.driver.connect_to_neo4j(host)
        self.psqlDriver = self.driver.psqlgraphDriver
        self.neo4jDriver = self.driver.neo4jDriver

    def _clear_tables(self):
        # clear psqltables
        conn = self.psqlDriver.engine.connect()
        conn.execute('commit')
        conn.execute('delete from edges')
        conn.execute('delete from nodes')
        conn.close()

        # clear neo4j
        self.neo4jDriver.cypher.execute(
            """MATCH (n:test)
            OPTIONAL MATCH (n:test)-[r]-()
            DELETE n,r
            """
        )
        self.neo4jDriver.cypher.execute(
            """MATCH (n:test)
            OPTIONAL MATCH (n:test2)-[r]-()
            DELETE n,r
            """
        )

    def test_neo_single_node(self):
        self._clear_tables()
        node_id = str(uuid.uuid4())
        timestamp = datetime.now()
        props = {'time': timestamp}
        test_props = {
            'id': node_id, 'time': self.driver.datetime2ms_epoch(timestamp)
        }
        self.psqlDriver.node_merge(node_id, label='test', properties=props)
        self.driver.export()
        nodes = self.neo4jDriver.cypher.execute('match (n:test) return n')
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].n.properties, test_props)

    def test_neo_many_node(self):
        self._clear_tables()
        count = 200
        test_props = []
        for i in range(count):
            node_id = str(uuid.uuid4())
            timestamp = datetime.now()
            props = {'time': timestamp}
            test_props.append({
                'id': node_id, 'time': self.driver.datetime2ms_epoch(timestamp)
            })
            self.psqlDriver.node_merge(node_id, label='test', properties=props)

        self.driver.export()
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
        self.psqlDriver.node_merge(node_id=src_id, label='test')
        self.psqlDriver.node_merge(node_id=dst_id, label='test')
        self.psqlDriver.edge_merge(src_id=src_id, dst_id=dst_id, label='test')
        self.driver.export()

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
        self.psqlDriver.node_merge(node_id=src_id, label='test')

        dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
        for dst_id in dst_ids:
            self.psqlDriver.node_merge(node_id=dst_id, label='test')
            self.psqlDriver.edge_merge(src_id=src_id, dst_id=dst_id,
                                       label='test')
        self.driver.export()
        nodes = self.neo4jDriver.cypher.execute(
            'match (n)-[r]-(m) where n.id = "{src_id}" return n'.format(
                src_id=src_id)
        )
        self.assertEqual(len(nodes), leaf_count)

    def _create_subtree(self, parent_id, level):
        for i in range(5):
            node_id = str(uuid.uuid4())
            self.psqlDriver.node_merge(node_id=node_id, label='test')
            self.psqlDriver.edge_merge(
                src_id=parent_id, dst_id=node_id, label='test'
            )
            if level < 2:
                self._create_subtree(node_id, level+1)

    def test_neo_tree_topology(self):
        """
        Create a tree topology, verify lookup by src_id and that all nodes
        are attached
        """

        self._clear_tables()
        node_id = str(uuid.uuid4())
        self.psqlDriver.node_merge(node_id=node_id, label='test')
        self._create_subtree(node_id, 0)
        self.driver.export()


if __name__ == '__main__':

    def run_test(test):
        suite = unittest.TestLoader().loadTestsFromTestCase(test)
        unittest.TextTestRunner(verbosity=2).run(suite)

    run_test(Test_psql2neo)
