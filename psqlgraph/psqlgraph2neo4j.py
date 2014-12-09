import py2neo
import psqlgraph


class PsqlGraph2Neo4j(object):

    def __init__(self):
        self.psqlgraphDriver = None
        self.neo4jDriver = None

    def connect_to_psql(self, host, user, password, database):
        self.psqlgraphDriver = psqlgraph.PsqlGraphDriver(
            host, user, password, database
        )

    def connect_to_neo4j(self, host, port=7474, user=None, password=None):
        self.neo4jDriver = py2neo.Graph(
            'http://{host}:{port}/db/data'.format(host=host, port=port)
        )

    def export(self):
        for node in self.psqlgraphDriver.get_nodes():
            node.properties['id'] = node.node_id
            self.neo4jDriver.create(py2neo.Node(node.label, **node.properties))
