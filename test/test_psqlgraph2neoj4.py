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
        self.neo4jDriver.cypher.execute('match (n:test) delete n')
        self.neo4jDriver.cypher.execute('match (n:test2) delete n')

    def test_neo_single_node(self):
        self._clear_tables()
        node_id = str(uuid.uuid4())
        props = {'time': datetime.now()}
        self.psqlDriver.node_merge(node_id, label='test', properties=props)
        self.driver.export()
        nodes = self.neo4jDriver.cypher.execute('match (n:test) return n')
        for node in nodes:
            print node
        # self.assertTrue(False)

    # def test_neo_many_node(self):
    #     self._clear_tables()
    #     node_ids = [str(uuid.uuid4()) for i in range(200)]
    #     for node_id in node_ids:
    #         self.psqlDriver.node_merge(node_id, label='test2')
    #     self.driver.export()
    #     self.assertTrue(False)

if __name__ == '__main__':

    def run_test(test):
        suite = unittest.TestLoader().loadTestsFromTestCase(test)
        unittest.TextTestRunner(verbosity=2).run(suite)

    run_test(Test_psql2neo)
