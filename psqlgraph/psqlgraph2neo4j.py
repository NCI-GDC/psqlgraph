import py2neo
import psqlgraph
from datetime import datetime


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

    def datetime2ms_epoch(self, dt):
        epoch = datetime.utcfromtimestamp(0)
        delta = dt - epoch
        return int(delta.total_seconds() * 1000)

    def try_parse_doc(self, doc):
        for key, val in doc.iteritems():
            try:
                ts = datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
                doc[key] = self.datetime2ms_epoch(ts)
            except:
                pass

    def convert_node(self, node):
            node.properties['id'] = node.node_id
            self.try_parse_doc(node.properties)

    def export(self):
        for node in self.psqlgraphDriver.get_nodes():
            self.convert_node(node)
            self.neo4jDriver.create(py2neo.Node(node.label, **node.properties))
