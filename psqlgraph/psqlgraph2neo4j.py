import py2neo
import psqlgraph
import time
import sys
import os.path
import pickle
from datetime import datetime
import progressbar
#from pdb import set_trace

MAX_RETRIES = 3
TIMEOUT = 90

class ExportError(Exception):
    pass

class PsqlGraph2Neo4j(object):

    def __init__(self):
        self.psqlgraphDriver = None
        self.neo4jDriver = None
        self.node_ids = {}
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

    def export_nodes(self, silent=False,batch_size=1000):
        node_count = self.psqlgraphDriver.get_node_count()
        i = 0
        if not silent:
            print("Exporting {n} nodes:".format(n=node_count))
            pbar = self.start_pbar(node_count)
        batch_size = min(batch_size,node_count)
        batch_ct = 0
        nodes = self.psqlgraphDriver.get_nodes()
        node_batch = []
        for node in nodes:
            self.convert_node(node)
            batch_ct += 1
            cypher="""
            CREATE (n:{label} {{props}}) RETURN n.id, ID(n)
            """.format(
                label=node.label
            )
            props = dict( [ (j, node.properties[j]) for j in node.properties if node.properties[j] != None ] )
            parameters = {"props":props}
            node_batch.append( (cypher,parameters) )
            if batch_ct >= batch_size:
                ids = self._transact(node_batch)
                for r in ids:
                    self.node_ids[r[0][0]] = r[0][1]
                node_batch = []
                if not silent:
                    i += self.update_pbar(pbar, i+batch_ct)
                batch_ct=0
        if len(node_batch):
            ids = self._transact(node_batch)
            for r in ids:
                self.node_ids[r[0][0]] = r[0][1]
        if not silent:
            self.update_pbar(pbar, node_count)
        fh = open("nodeids.o","w")
        pickle.dump(self.node_ids,fh)
        fh.close()
        

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

        if not self.node_ids:
            if os.path.exists("nodeids.o"):
                fh = open("nodeids.o")
                self.node_ids = pickle.load(fh)
            else:
                rows = self.neo4jDriver.cypher.stream("""
                MATCH (a) RETURN a.id, ID(a)
                """)
                for r in rows:
                    self.node_ids[r[0]] = r[1]
                fh = open("nodeids.o","w")
                pickle.dump(self.node_ids,fh)
        
        batch_size = min(edge_count, batch_size)
        if not silent:
            print("\nExporting {n} edges:".format(n=edge_count))
            pbar = self.start_pbar(edge_count)
        batch_count=0
        edges = self.psqlgraphDriver.get_edges()
        edge_batch = []
        for edge in edges:
            batch_count += 1
            if not (edge.src and edge.dst):
                i += self.update_pbar(pbar, i)
                continue
            src_n = self.neo4jDriver.node( self.node_ids[edge.src_id] )
            dst_n = self.neo4jDriver.node( self.node_ids[edge.dst_id] )
            s2d = py2neo.Relationship(src_n,edge.label,dst_n)
            props = dict( [ (j, edge.properties[j]) for j in edge.properties if edge.properties[j] != None ] )
            edge_batch.append( [(src_n,s2d,dst_n),props] )
            if batch_count >= batch_size:
                self._batch_edges(edge_batch)
                edge_batch = []
                if not silent:
                    i += self.update_pbar(pbar, i+batch_count)
                batch_count=0

        if len(edge_batch):
            self._batch_edges(edge_batch)
        if not silent:
            self.update_pbar(pbar, edge_count)

    def _transact(self, cypher_list):
        retries = 0
        ret = None
        while retries < MAX_RETRIES:
            transaction = self.neo4jDriver.cypher.begin()
            try:
                for c in cypher_list:
                    transaction.append(c[0],parameters=c[1])
                ret = transaction.commit()
                j = 0
                while (j < TIMEOUT and not transaction.finished):
                    time.sleep(1.0)
                    j += 1
                if (not transaction.finished):
                    raise RuntimeError('Timeout reached before txn finished')
                break
            
            except Exception as e: #py2neo.packages.httpstream.http.SocketError:
                print >> sys.stderr, type(e), e
                transaction.rollback()
                retries += 1
                if not transaction.finished or retries >= MAX_RETRIES:
                    raise RuntimeError("transaction failed after %d retries" % retries-1)
        return ret
    def _batch_edges(self, edge_list):
        batch = py2neo.batch.Batch(self.neo4jDriver)
        for e in edge_list:
            j = py2neo.batch.CreateRelationshipJob(*e[0],**e[1])
            batch.append(j)
        self.neo4jDriver.batch.run(batch)
        pass

        
        
    def export(self, silent=False, batch_size=1000, create_indexes=True):
        if create_indexes:
            self.create_indexes()

        if not self.psqlgraphDriver:
            raise ExportError(
                'No psqlgraph driver.  Please call .connect_to_psql()')

        if not self.neo4jDriver:
            raise ExportError(
                'No neo4j driver.  Please call .connect_to_neo4j()')

        self.export_nodes(silent, batch_size=batch_size)
        self.export_edges(silent, batch_size=batch_size)
