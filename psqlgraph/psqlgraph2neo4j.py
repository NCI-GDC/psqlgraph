from __future__ import print_function
from datetime import datetime
import psqlgraph
from psqlgraph import Node, Edge
import progressbar
import os
import py2neo

def create_index():
    graph = py2neo.Graph()
    for node_class in psqlgraph.Node.get_subclasses():
        label = node_class.get_label()
        graph.cypher.execute("create index on :{}(id)".format(label))


class PsqlGraph2Neo4j(object):
    def __init__(self):
        self.psqlgraphDriver = None
        self.files = dict()

    def connect_to_psql(self, host, user, password, database):
        self.psqlgraphDriver = psqlgraph.PsqlGraphDriver(
            host, user, password, database
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
            self.try_parse_doc(node.properties)

    def create_node_files(self, data_dir):
        type_conversion = {str: 'String', bool: 'boolean', int: 'int',
                           float: 'float', long: 'long'}
        with self.psqlgraphDriver.session_scope():
            count = 0
            for node_class in psqlgraph.Node.get_subclasses():
                properties = node_class.get_pg_properties()
                label = node_class.get_label()
                f = open(
                    os.path.join(data_dir, 'nodes'+str(count)+'.csv'), 'w')
                count += 1
                self.files[label] = [f]
                keys = []
                title = 'i:id\tid\tl:label\t'
                for key, value in properties.iteritems():
                    keys.append(key)
                    if not value:
                        typ = 'String'
                    else:
                        typ = type_conversion[value[0]]
                    title += (key + ':' + typ + '\t')
                print(title, file=f)
                self.files[label].append(keys)

    def close_files(self):
        for f in self.files.values():
            f[0].close()

    def start_pbar(self, maxval):
        pbar = progressbar.ProgressBar(
            widgets=[
                progressbar.Percentage(), ' ',
                progressbar.Bar(marker='#'), ' ',
                progressbar.ETA(), ' ',
            ], maxval=maxval).start()
        return pbar

    def node_to_csv(self, id, node):
        result = ''
        node_file = self.files[node.label]
        f = node_file[0]
        result = id+'\t'+node.node_id + '\t' + node.label + '\t'
        for key in node_file[1]:
            value = unicode(node.properties.get(key, ''))\
                .replace('\r', '\\\\r').replace('\n', '\\\\n')\
                .replace('"', '\\"')
            if value == 'None':
                value = ''
            result += value+'\t'
        print(result.encode('utf-8'), file=f)

    def export_to_csv(self, data_dir, silent=False):
        node_ids = dict()
        if not silent:
            i = 0
            node_count = self.psqlgraphDriver.nodes().not_sysan({'to_delete': True}).count()
            print("Exporting {n} nodes:".format(n=node_count))
            if node_count != 0:
                pbar = self.start_pbar(node_count)

        edge_file = open(os.path.join(data_dir, 'rels.csv'), 'w')
        print('start\tend\ttype\t', file=edge_file)
        self.create_node_files(data_dir)
        batch_size = 1000
        id_count = 0
        for node_type in Node.get_subclasses():
            nodes = self.psqlgraphDriver.nodes(node_type).not_sysan({'to_delete': True}).yield_per(batch_size)
            for node in nodes:
                self.convert_node(node)
                self.node_to_csv(str(id_count), node)
                node_ids[node.node_id] = id_count
                id_count += 1

                if not silent and node_count != 0:
                    i = self.update_pbar(pbar, i)

            if not silent and node_count != 0:
                self.update_pbar(pbar, node_count)

        self.close_files()
        if not silent:
            i = 0
            edge_count = self.psqlgraphDriver.get_edge_count()
            print("Exporting {n} edges:".format(n=edge_count))
            if edge_count != 0:
                pbar = self.start_pbar(node_count)

        for edge_type in Edge.get_subclasses():
            edges = self.psqlgraphDriver.edges(edge_type).yield_per(batch_size)
            for edge in edges:
                src = node_ids.get(edge.src_id, '')
                dst = node_ids.get(edge.dst_id, '')
                if src != '' and dst != '':
                    edge_file.write(str(src)+'\t'+str(dst)+'\t'+edge.label+'\n')
                if not silent and edge_count != 0:
                    i = self.update_pbar(pbar, i)

        edge_file.close()
        if not silent and edge_count != 0:
            self.update_pbar(pbar, edge_count)

    def update_pbar(self, pbar, i):
        try:
            pbar.update(i)
        except:
            pass
        return i+1

    def export(self, data_dir, silent=False):
        '''
        create csv files that will later be parsed by batch
        importer from psqlgraph.

        data_dir:         directory to store csv
        '''

        if not self.psqlgraphDriver:
            raise Exception(
                'No psqlgraph driver.  Please call .connect_to_psql()')

        self.export_to_csv(data_dir, silent=silent)
