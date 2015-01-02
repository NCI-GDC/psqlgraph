import py2neo
import psqlgraph
from datetime import datetime
import progressbar


class ExportError(Exception):
    pass


class PsqlGraph2Neo4j(object):

    def __init__(self):
        self.psqlgraphDriver = None
        self.neo4jDriver = None
        self.indexed_keys = [
            'id'
        ]

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

    def get_distinct_labels(self):
        conn = self.psqlgraphDriver.engine.connect()
        conn.execute('commit')
        labels = conn.execute('select distinct label from nodes').fetchall()
        conn.close()
        return [l[0] for l in labels]

    def create_indexes(self):
        print('Creating indexes ...')
        labels = self.get_distinct_labels()
        for label in labels:
            for key in self.indexed_keys:
                try:
                    self.neo4jDriver.schema.create_index(label, key)
                except Exception, msg:
                    print('Unable to create index: ' + str(msg))

    def export_nodes(self, silent=False):
        if not silent:
            i = 0
            node_count = self.psqlgraphDriver.get_node_count()
            print("Exporting {n} nodes:".format(n=node_count))
            pbar = self.start_pbar(node_count)

        nodes = self.psqlgraphDriver.get_nodes()
        for node in nodes:
            self.convert_node(node)
            self.neo4jDriver.create(py2neo.Node(node.label, **node.properties))
            if not silent:
                i = self.update_pbar(pbar, i)

    def start_pbar(self, maxval):
        pbar = progressbar.ProgressBar(
            widgets=[
                progressbar.Percentage(), ' ',
                progressbar.Bar(marker='#'), ' ',
                progressbar.ETA(), ' ',
            ], maxval=maxval).start()
        return pbar

    def update_pbar(self, pbar, i):
        try:
            pbar.update(i)
        except:
            pass
        return i+1

    def export_edges(self, silent=False, batch_size=1000):

        i = 0
        edge_count = self.psqlgraphDriver.get_edge_count()

        if not edge_count:
            return

        batch_size = min(edge_count, batch_size)
        if not silent:
            print("\nExporting {n} edges:".format(n=edge_count))
            pbar = self.start_pbar(edge_count)

        transaction = self.neo4jDriver.cypher.begin()
        batch_count = 0
        edges = self.psqlgraphDriver.get_edges()
        for edge in edges:
            batch_count += 1

            if not (edge.src and edge.dst):
                i += self.update_pbar(pbar, i)
                continue

            cypher = """
            MATCH (s:{src_label}), (d:{dst_label})
            WHERE s.id = {{src_id}} and d.id = {{dst_id}}
            CREATE (s)-[:{edge_label}]->(d)
            """.format(
                src_label=edge.src.label,
                dst_label=edge.dst.label,
                edge_label=edge.label
            )

            parameters = dict(
                src_id=edge.src_id,
                dst_id=edge.dst_id,
            )

            transaction.append(
                cypher,
                parameters=parameters,
            )

            if batch_count >= batch_size:
                transaction.commit()
                batch_count = 0
                transaction = self.neo4jDriver.cypher.begin()

            if not silent:
                i = self.update_pbar(pbar, i)

        transaction.commit()

    def export(self, silent=False):

        if not self.psqlgraphDriver:
            raise ExportError(
                'No psqlgraph driver.  Please call .connect_to_psql()'
            )

        if not self.neo4jDriver:
            raise ExportError(
                'No neo4j driver.  Please call .connect_to_neo4j()'
            )

        self.export_nodes(silent)
        self.export_edges(silent)
