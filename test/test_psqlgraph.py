import uuid
import unittest
import logging
import random
from psqlgraph import Node, Edge, PolyNode, sanitize, VoidedEdge
from psqlgraph import PolyNode as PsqlNode
from psqlgraph import PolyEdge as PsqlEdge

from parameterized import parameterized
from multiprocessing import Process
from sqlalchemy.exc import IntegrityError
from psqlgraph.exc import ValidationError

from datetime import datetime
from copy import deepcopy


# We have to import models here, even if we don't use them
from test import models, PsqlgraphBaseTest

logging.basicConfig(level=logging.DEBUG)


def timestamp():
    return str(datetime.now())


class TestPsqlGraphDriver(PsqlgraphBaseTest):

    def tearDown(self):
        self._clear_tables()

    def test_getitem(self):
        """Test that indexing nodes/edges accesses their properties"""
        node = PolyNode(
            node_id=str(uuid.uuid4()), label="foo", properties={"bar": 1})
        self.assertEqual(node["bar"], 1)

    def test_setitem(self):
        """Test that indexing nodes/edges accesses their properties"""
        node = PolyNode(
            node_id=str(uuid.uuid4()), label="foo", properties={"bar": 1})
        node["bar"] = 2
        self.assertEqual(node["bar"], 2)
        edge = models.Edge1(src_id=None, dst_id=None)
        edge["bar"] = 2
        self.assertEqual(edge["bar"], 2)

    def test_long_ints_roundtrip(self):
        """Test that integers that only fit in 26 bits round trip correctly."""
        with self.g.session_scope():
            node = PolyNode(node_id=str(uuid.uuid4()), label="foo",
                            properties={"bar": 9223372036854775808})
            self.g.node_insert(node=node)
            loaded = self.g.node_lookup(node_id=node.node_id).one()
            self.assertEqual(loaded["bar"], 9223372036854775808)

    def test_node_null_label_merge(self):
        """Test merging of a non-existent node

        Verify the case where a user merges a single non-existent node
        """
        with self.g.session_scope():
            self.assertRaises(
                AssertionError,
                self.g.node_merge,
                node_id=str(uuid.uuid4()))

    def test_node_null_query_one(self):
        """Test querying of a single non-existent node

        Verify the case where a user queries for a single non-existent node
        """

        with self.g.session_scope():
            node = self.g.node_lookup_one(str(uuid.uuid4()))
            self.assertTrue(node is None)

    def test_node_null_query(self):
        """Test querying for any non-existent nodes

        Verify the case where a user queries for any non-existent nodes
        """

        with self.g.session_scope():
            node = list(self.g.node_lookup(str(uuid.uuid4())))
            self.assertEqual(node, [])
            node = self.g.node_lookup_one(str(uuid.uuid4()))
            self.assertTrue(node is None)

    def verify_node_count(self, count, node_id=None, matches=None,
                          voided=False):
        """Test querying for the count on a non-existent node

        Verify the case where a user queries a count for non-existent nodes
        """
        with self.g.session_scope():
            nodes = list(self.g.node_lookup(
                node_id=node_id,
                property_matches=matches,
                voided=False))
            if voided:
                voided_nodes = list(self.g.node_lookup(
                    node_id=node_id,
                    property_matches=matches,
                    voided=True))
                nodes = list(nodes) + list(voided_nodes)
            self.assertEqual(len(nodes), count, 'Expected a {n} nodes to '
                             'be found, instead found {count}'.format(
                                 n=count, count=len(nodes)))
            return nodes

    def test_node_merge_and_lookup(self):
        """Test node merge and lookup

        Insert a single node and query, compare that the result of the
        query is correct
        """
        tempid = str(uuid.uuid4())
        properties = {'key1': None, 'key2': 2, 'key3': timestamp(),
                      'timestamp': None, 'new_key': None}
        with self.g.session_scope():
            self.g.node_merge(node_id=tempid, label='test', properties=properties)
        with self.g.session_scope():
            node = self.g.node_lookup_one(tempid)
        self.assertEqual(properties, node.properties)

    def test_node_update_acls(self):
        tempid = str(uuid.uuid4())
        with self.g.session_scope():
            node = self.g.node_merge(node_id=tempid, label='test')
            self.g.node_update(node, acl=["somebody"])
        with self.g.session_scope():
            node = self.g.nodes().ids([tempid]).one()
            self.assertEqual(node.acl, ["somebody"])
        with self.g.session_scope():
            self.g.node_merge(node_id=node.node_id, acl=[])
        with self.g.session_scope():
            node = self.g.nodes().ids([tempid]).one()
            self.assertEqual(node.acl, [])

    def test_node_update_properties_by_id(self, given_id=None, label=None):
        """Test updating node properties by ID

        Insert a single node, update it, verify that

        (1) The first insertion is successful
        (2) The update is successful
        (3) The transaction of the update is maintained
        (4) There is only a single version of the node
        (5) The voided node is a snapshot of the previous version.
        """

        node_id = str(uuid.uuid4()) if not given_id else given_id

        if not label:
            label = 'test'

        retries = 0 if not given_id else int(1e6)
        # Add first node
        propertiesA = {'key1': None, 'key2': 1, 'key3': timestamp(),
                       'timestamp': None, 'new_key': None}
        self.g.node_merge(node_id=node_id, properties=propertiesA,
                     label=label, max_retries=retries)
        print('-- commited A')

        # Add second node
        propertiesB = {'key1': '2', 'new_key': 'n',
                       'timestamp': timestamp()}
        self.g.node_merge(node_id=node_id, properties=propertiesB,
                     max_retries=retries)
        print('-- commited B')

        # Merge properties
        merged = deepcopy(propertiesA)
        merged.update(propertiesB)

        if not given_id:
            # Test that there is only 1 non-void node with node_id and property
            # equality
            # if this is not part of another test, check the count
            with self.g.session_scope():
                node = self.g.node_lookup_one(node_id)
                self.assertEqual(merged, node.properties)
                voided_node = self.g.node_lookup(node_id, voided=True).one()
                voided_props = sanitize(propertiesA)
                self.assertEqual(voided_props, voided_node.properties)
            self.verify_node_count(2, node_id=node_id, voided=True)

        return merged

    def test_query_by_label(self, node_id=None):
        """Test ability to query for nodes by label"""

        label = 'test'
        repeat = 10
        for i in range(repeat):
            self.g.node_merge(
                node_id=str(uuid.uuid4()), label=label)
        with self.g.session_scope():
            nodes = list(self.g.node_lookup(label=label))
        self.assertEqual(len(nodes), repeat)

    def test_node_update_properties_by_matches(self):
        """Test updating node properties by matching properties

        Insert a single node, update it, verify that

        (1) The first insertion is successful
        (2) The update is successful
        (3) The transaction of the update is maintained
        (4) There is only a single version of the node
        """

        node_id = str(uuid.uuid4())

        # Add first node
        propertiesA = {'key1': 'first', 'key2': 5}
        node = self.g.node_merge(node_id=node_id, label='test',
                            properties=propertiesA)
        merged = {k: v for k, v in node.properties.items()}

        # Add second node
        propertiesB = {'key1': 'second', 'key2': 6}
        with self.g.session_scope():
            node = self.g.node_lookup_one(property_matches=propertiesA)
        self.g.node_merge(node=node, label='test', properties=propertiesB)

        # Merge properties
        merged.update(propertiesB)

        with self.g.session_scope():
            nodes = self.g.nodes().props(propertiesB).all()
            node = self.g.node_lookup_one(property_matches=propertiesB)
            self.assertEqual(merged, node.properties)
            node = self.g.nodes().ids(node_id).one()
            self.assertEqual(merged, node.properties)

        nodes = self.verify_node_count(2, node_id=node_id, voided=True)
        self.assertEqual(merged, nodes[0].properties)
        return merged

    def test_node_update_sysan_items(self):
        """Test updating node system annotations ID
        """

        node_id = str(uuid.uuid4())

        system_annotationsA = sanitize({
            'key1': None, 'key2': 2, 'key3': timestamp()
        })
        node = self.g.node_merge(node_id=node_id, label='test',
                            system_annotations=system_annotationsA)
        test_string = 'This is a test'
        node.system_annotations['key1'] = test_string
        with self.g.session_scope() as session:
            session.merge(node)
        with self.g.session_scope():
            node = self.g.nodes().ids(node_id).one()
            self.assertTrue(node.system_annotations['key1'], test_string)

    def test_node_update_property_items(self):
        """Test updating node system annotations ID
        """

        node_id = str(uuid.uuid4())

        props = sanitize({'key1': None, 'key2': 2})
        with self.g.session_scope() as session:
            node = PolyNode(node_id, 'test', properties=props)
        test_string = 'This is a test'
        node.properties['key1'] = test_string
        with self.g.session_scope() as session:
            session.merge(node)
        with self.g.session_scope():
            node = self.g.nodes().ids(node_id).one()
            self.assertTrue(node.properties['key1'], test_string)

    def test_node_update_system_annotations_id(self, given_id=None):
        """Test updating node system annotations ID

        Insert a single node, update it, verify that

        (1) The first insertion is successful
        (2) The update is successful
        (3) The transaction of the update is maintained
        (4) There is only a single version of the node
        """

        node_id = str(uuid.uuid4()) if not given_id else given_id

        # Add first node
        system_annotationsA = sanitize({
            'key1': None, 'key2': 2, 'key3': timestamp()
        })
        self.g.node_merge(node_id=node_id, label='test',
                     system_annotations=system_annotationsA)

        # Add second node
        system_annotationsB = sanitize({
            'key1': None, 'new_key': 2, 'timestamp': timestamp()
        })
        self.g.node_merge(node_id=node_id, label='test',
                     system_annotations=system_annotationsB)

        # Merge system_annotations
        merged = deepcopy(system_annotationsA)
        merged.update(system_annotationsB)

        # if this is not part of another test, check the count
        if not given_id:
            with self.g.session_scope():
                node = self.g.node_lookup_one(node_id)
            print("merged:", sanitize(merged))
            print('node:', sanitize(node.system_annotations))
            self.assertEqual(
                sanitize(merged), sanitize(node.system_annotations))

            nodes = list(self.verify_node_count(
                2, node_id=node_id, voided=True))
            self.assertEqual(sanitize(system_annotationsA),
                             nodes[1].system_annotations)

        return merged

    def test_node_to_json(self):
        """test node serialization
        """
        node_id = str(uuid.uuid4())

        # add first node
        propertiesA = sanitize({
            'key1': None, 'key2': 1, 'key3': timestamp(),
                       'timestamp': None, 'new_key': None
        })
        system_annotationsA = sanitize({
            'key1': None, 'key2': 2, 'key3': timestamp()
        })
        self.g.node_merge(node_id=node_id, label='test', properties=propertiesA,
                     system_annotations=system_annotationsA)

        dst = self.g.node_merge(node_id=str(uuid.uuid4()), label='test')
        edge1 = self.g.edge_insert(PsqlEdge(
            src_id=node_id, dst_id=dst.node_id, label='edge1'))

        with self.g.session_scope():
            node = self.g.node_lookup_one(node_id)
            expected_json = {
                                'node_id': node_id,
                                'label': 'test',
                                'acl': [],
                                'properties': propertiesA,
                                'system_annotations': system_annotationsA,
                            }

            self.assertDictEqual(node.to_json(), expected_json)

    def test_node_from_json(self):
        """Test node creation from json 
        """
        node_id = str(uuid.uuid4())

        propertiesA = sanitize({
            'key1': None, 'key2': 1, 'key3': timestamp(),
                       'timestamp': None, 'new_key': None
        })

        system_annotationsA = sanitize({
            'key1': None, 'key2': 2, 'key3': timestamp()
        })

        node_json = {
                        'node_id': node_id,
                        'label': 'test',
                        'acl': [],
                        'properties': propertiesA,
                        'system_annotations': system_annotationsA,
                        'edges_out': [],
                        'edges_in': []
                    }

        node = Node.from_json(node_json)

        with self.g.session_scope() as s:
            s.merge(node)
            node = self.g.node_lookup_one(node_id)
            self.assertDictEqual(node.props, propertiesA)
            self.assertDictEqual(node.sysan, system_annotationsA)
            self.assertEqual(node.acl, [])
            self.assertEqual(node.node_id, node_id)

    def test_node_json_roundtrip(self):
        """test node serialization
        """
        node_id = str(uuid.uuid4())

        # add first node
        propertiesA = sanitize({
            'key1': None, 'key2': 1, 'key3': timestamp(),
                       'timestamp': None, 'new_key': None
        })
        system_annotationsA = sanitize({
            'key1': None, 'key2': 2, 'key3': timestamp()
        })
        self.g.node_merge(node_id=node_id, label='test', properties=propertiesA,
                     system_annotations=system_annotationsA)

        dst = self.g.node_merge(node_id=str(uuid.uuid4()), label='test')
        edge1 = self.g.edge_insert(PsqlEdge(
            src_id=node_id, dst_id=dst.node_id, label='edge1'))

        with self.g.session_scope():
            node = self.g.node_lookup_one(node_id)

            node_json = node.to_json()

            new_node = Node.from_json(node_json)
            self.assertDictEqual(new_node.props, node.props)
            self.assertDictEqual(new_node.sysan, node.sysan)
            self.assertEqual(new_node.node_id, node_id)


    def _insert_node(self, node):
        """Test inserting a node"""
        with self.g.session_scope() as session:
            session.add(node)

    def test_node_unique_id_constraint(self):
        """Test node constraints on unique ID

        Verify that the table constraints prevent the existance two
        non-voided nodes with the same id
        """

        tempid = str(uuid.uuid4())

        # Add first node
        propertiesA = {'key1': None, 'key2': 2, 'key3': timestamp()}
        self.g.node_merge(node_id=tempid, label='test', properties=propertiesA)

        propertiesB = {'key1': None, 'key2': 2, 'key3': timestamp()}
        with self.assertRaises(IntegrityError):
            bad_node = PolyNode(
                node_id=tempid,
                system_annotations={},
                acl=[],
                label='test',
                properties=propertiesB
            )
            self.g.node_insert(bad_node)

    def test_null_node_merge(self):
        """Test merging of a null node

        Verify that the library handles null nodes properly on merging
        """
        self.assertRaises(AssertionError, self.g.node_merge)

    def test_repeated_node_update_properties_by_id(self, given_id=None):
        """Test repeated node updating to properties by ID

        Verify that repeated updates to a single node create
        the correct number of voided transactions and a single valid
        node with the correct properties
        """

        node_id = str(uuid.uuid4()) if not given_id else given_id

        for tally in range(self.REPEAT_COUNT):
            props = self.test_node_update_properties_by_id(node_id)

        if not given_id:
            self.verify_node_count(self.REPEAT_COUNT*2, node_id=node_id,
                                   voided=True)
            with self.g.session_scope():
                node = self.g.node_lookup_one(node_id)
            self.assertEqual(props, node.properties)

    def test_repeated_node_update_system_annotations_by_id(
            self, given_id=None):
        """Test repeated node updates to system annotations by ID

        Verify that repeated updates to a single node create

        the correct number of voided transactions and a single valid
        node with the correct properties
        """

        REPEAT_COUNT = 20
        node_id = str(uuid.uuid4()) if not given_id else given_id

        for tally in range(REPEAT_COUNT):
            annotations = self.test_node_update_system_annotations_id(node_id)

        if not given_id:
            self.verify_node_count(
                REPEAT_COUNT*2, node_id=node_id, voided=True)
            with self.g.session_scope():
                node = self.g.node_lookup_one(node_id)
            self.assertEqual(
                sanitize(annotations), sanitize(node.system_annotations))

    def test_sessioned_node_update(self):
        """Test repeated update of a sessioned node

        Repeate test_repeated_node_update but passing a single session for
        all interactions to use

        """

        label = 'test'
        node_ids = [str(uuid.uuid4()) for i in range(self.REPEAT_COUNT)]
        properties = {}
        with self.g.session_scope() as session:
            for node_id in node_ids:
                properties[node_id] = {
                    'key1': node_id,
                    'key2': 2,
                    'key3': timestamp(),
                    'new_key': None,
                    'timestamp': None
                }

                self.g.node_merge(
                    node_id=node_id,
                    label='test',
                    properties=properties[node_id],
                    session=session,
                )
            nodes = list(self.g.node_lookup(label=label))

        for node in nodes:
            self.assertEqual(
                properties[node.node_id], node.properties,
            )

    @unittest.skip('not implemented')
    def test_concurrent_node_update_by_id(self):
        """Test concurrent node updating by ID

        Test that insertion of nodes is thread-safe and that retries succeed
        eventually
        """

        process_count = 2
        tempid = str(uuid.uuid4())
        processes = []

        for tally in range(process_count):
            processes.append(Process(
                target=self.test_repeated_node_update_properties_by_id,
                kwargs={'given_id': tempid})
            )

        for p in processes:
            p.start()

        for p in processes:
            p.join()

        self.verify_node_count(process_count*self.REPEAT_COUNT*2, tempid,
                               voided=True)

    def test_node_clobber(self):
        """Test that clobbering a node replaces all of its properties"""

        tempid = str(uuid.uuid4())

        propertiesA = {'key1':  None, 'key2':  2, 'key3':  timestamp()}
        self.g.node_merge(node_id=tempid, label='test',
                     properties=propertiesA)

        propertiesB = {'key1':  'second', 'key2':  0, 'key3':  timestamp(),
                       'new_key': None, 'timestamp': None}
        self.g.node_clobber(node_id=tempid, properties=propertiesB)

        with self.g.session_scope():
            node = self.g.node_lookup(node_id=tempid).one()
        self.assertEqual(propertiesB, node.properties)

    @unittest.skip('not implemented')
    def test_node_delete_system_annotation_keys(self):
        """Test the ability to remove system annotation keys from nodes"""

        tempid = str(uuid.uuid4())
        annotations = {'key1':  None, 'key2':  2, 'key3':  'test'}
        self.g.node_merge(node_id=tempid, label='test',
                     system_annotations=annotations)

        self.g.node_delete_system_annotation_keys(
            ['key2', 'key3'], node_id=tempid)

        annotations.pop('key2')
        annotations.pop('key3')

        with self.g.session_scope():
            nodes = list(self.g.node_lookup(node_id=tempid))
        self.assertEqual(len(nodes), 1, 'Expected a single node to be found, '
                         'instead found {count}'.format(count=len(nodes)))
        self.assertEqual(annotations, nodes[0].system_annotations)

    def test_node_delete(self):
        """Test node deletion"""

        tempid = str(uuid.uuid4())
        self.g.node_merge(node_id=tempid, label='test')
        self.g.node_delete(node_id=tempid)

        with self.g.session_scope():
            nodes = list(self.g.node_lookup(tempid))
        self.assertEqual(len(nodes), 0, 'Expected a no non-voided nodes '
                         'to be found, instead found {count}'.format(
                             count=len(nodes)))

    def test_query_then_node_delete(self):
        """Test querying for a node then deleting it."""
        node1 = self.g.node_merge(node_id=str(uuid.uuid4()), label='test')
        with self.g.session_scope():
            for node in self.g.node_lookup(label="test").all():
                self.g.node_delete(node=node)
        with self.g.session_scope():
            nodes = self.g.node_lookup(node1.node_id).all()
        self.assertEqual(len(nodes), 0, 'Expected a no non-voided nodes '
                         'to be found, instead found {count}'.format(
                             count=len(nodes)))

    def test_repeated_node_delete(self):
        """Test repeated node deletion correctness"""

        node_id = str(uuid.uuid4())
        for i in range(self.REPEAT_COUNT):
            self.test_node_update_properties_by_id(node_id)
            self.g.node_delete(node_id=node_id)
            with self.g.session_scope():
                self.assertIs(
                    self.g.node_lookup_one(node_id=node_id), None)

    def test_edge_insert_null_label(self):
        """Test merging of a null edge

        Verify the case where a user merges a single non-existent node
        """

        self.assertRaises(
            AttributeError,
            PsqlEdge,
            str(uuid.uuid4()), str(uuid.uuid4), None,
        )

    @unittest.skip('not implemented')
    def test_edges_have_unique_ids(self):
        """Test that generated edge ids are unique"""
        src_id = str(uuid.uuid4())
        dst_id = str(uuid.uuid4())
        self.g.node_merge(node_id=src_id, label='test')
        self.g.node_merge(node_id=dst_id, label='test')

        edge1 = self.g.edge_insert(PsqlEdge(
            src_id=src_id, dst_id=dst_id, label='edge1'))
        edge2 = self.g.edge_insert(PsqlEdge(
            src_id=src_id, dst_id=dst_id, label='edge2'))
        self.assertNotEqual(edge1.edge_id, edge2.edge_id)

    def test_edge_insert_and_lookup(self):
        """Test edge creation and lookup by dst_id, src_id, dst_id and src_id"""
        with self.g.session_scope():
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            self.g.node_merge(node_id=src_id, label='test')
            self.g.node_merge(node_id=dst_id, label='test')

            edge = self.g.edge_insert(PsqlEdge(
                src_id=src_id, dst_id=dst_id, label='edge1'))
            self.g.edge_update(edge, properties={'test': None})
            self.g.edge_update(edge, properties={'test': 2})

            props = edge.property_template({'test': 2})
            edge = self.g.edge_lookup_one(dst_id=dst_id)
            self.assertEqual(edge.src_id, src_id)
            self.assertEqual(edge.dst_id, dst_id)
            self.assertEqual(edge.properties, props)

            edge = self.g.edge_lookup_one(src_id=src_id)
            self.assertEqual(edge.src_id, src_id)
            self.assertEqual(edge.dst_id, dst_id)
            self.assertEqual(edge.properties, props)

            edge = self.g.edge_lookup_one(src_id=src_id, dst_id=dst_id)
            self.assertEqual(edge.src_id, src_id)
            self.assertEqual(edge.dst_id, dst_id)
            self.assertEqual(edge.properties, props)

    def test_edge_snapshot(self):
        with self.g.session_scope():
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            self.g.node_merge(node_id=src_id, label='test')
            self.g.node_merge(node_id=dst_id, label='test')
            edge = self.g.edge_insert(PsqlEdge(
                src_id=src_id, dst_id=dst_id, label='edge1'))
        with self.g.session_scope():
            self.g.edge_update(edge, properties={'test': 3})
            voided_edge = self.g.edges(VoidedEdge).one()
            self.assertEqual(edge.property_template(), voided_edge.properties)

    def test_edge_insert_and_lookup_properties(self):
        """Test edge property merging"""
        with self.g.session_scope():
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            props = {'key1': str(random.random()), 'key2': random.random()}
            self.g.node_merge(node_id=src_id, label='test')
            self.g.node_merge(node_id=dst_id, label='test')
            self.g.edge_insert(PsqlEdge(
                src_id=src_id, dst_id=dst_id, properties=props, label='edge1'))
            edge = self.g.edge_lookup_one(src_id=src_id, dst_id=dst_id)
            self.assertEqual(edge.src_id, src_id)
            self.assertEqual(edge.dst_id, dst_id)
            self.assertEqual(edge.properties, edge.property_template(props))

    def test_edge_lookup_leaves(self):
        """Test looking up the leaves on an edge

        Create a star topology, verify lookup by src_id
        and that all nodes are attached
        """

        with self.g.session_scope():
            leaf_count = 10
            src_id = str(uuid.uuid4())
            self.g.node_merge(node_id=src_id, label='test')

            dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
            for dst_id in dst_ids:
                self.g.node_merge(node_id=dst_id, label='test')
                self.g.edge_insert(PsqlEdge(
                    src_id=src_id, dst_id=dst_id, label='edge1'))

            edge_ids = [e.dst_id for e in self.g.edge_lookup(src_id=src_id)]
            self.assertEqual(len(set(edge_ids)), leaf_count)

            for dst_id in dst_ids:
                self.assertTrue(dst_id in set(edge_ids))

    def test_edge_lookup_with_label(self):
        """
        Regression test that edge_lookup works if label is specified.
        """
        with self.g.session_scope():
            nid1 = self.g.node_merge(node_id=str(uuid.uuid4()), label='test').node_id
            nid2 = self.g.node_merge(node_id=str(uuid.uuid4()), label='test').node_id
            self.g.edge_insert(PsqlEdge(src_id=nid1, dst_id=nid2, label='edge1'))
            self.g.edge_lookup(label="edge1", src_id=nid1, dst_id=nid2).one()

    def test_edge_to_json(self):
        """Test edge serialization to json
        """
        with self.g.session_scope() as session:
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            src = self.g.node_merge(node_id=src_id, label='test')
            dst = self.g.node_merge(node_id=dst_id, label='test')

            edge = self.g.edge_insert(PsqlEdge(
                src_id=src_id, dst_id=dst_id, label='edge1'), session=session)

            expected_json = {
                              'src_id': src_id,
                              'dst_id': dst_id,
                              'src_label': src.label,
                              'dst_label': dst.label,
                              'label': 'edge1',
                              'acl': [],
                              'properties': edge.property_template(),
                              'system_annotations': {}
                            }
            self.assertDictEqual(edge.to_json(), expected_json)

    def test_edge_get_unique_subclass(self):
        """Test
        """
        src_id = str(uuid.uuid4())
        dst_id = str(uuid.uuid4())
        src = self.g.node_merge(node_id=src_id, label='test')
        dst = self.g.node_merge(node_id=dst_id, label='test')

        self.assertIs(Edge.get_unique_subclass('test','edge1','test'), models.Edge1)

    def test_edge_from_json(self):
        """Test edge creation from json
        """
        with self.g.session_scope() as s:
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            src = self.g.node_merge(node_id=src_id, label='test')
            dst = self.g.node_merge(node_id=dst_id, label='test')

            edge_json = {
                          'src_id': src_id,
                          'src_label': 'test',
                          'dst_label': 'test',
                          'dst_id': dst_id,
                          'label': 'edge1',
                          'acl': [],
                          'properties': {},
                          'system_annotations': {}
                        }

            edge = Edge.from_json(edge_json)

            self.assertEqual(edge.acl, [])
            self.assertEqual(edge.label, 'edge1')
            self.assertEqual(edge.properties, edge.property_template())
            self.assertEqual(edge.src_id, src_id)
            self.assertEqual(edge.dst_id, dst_id)

    def test_json_roundtrip(self):
        """Test edge from_json after a to_json
        """
        with self.g.session_scope():
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            src = self.g.node_merge(node_id=src_id, label='test')
            dst = self.g.node_merge(node_id=dst_id, label='test')

            edge = self.g.edge_insert(PsqlEdge(
                src_id=src_id, dst_id=dst_id, label='edge1'))

            edge_json = edge.to_json()

            new_edge = Edge.from_json(edge_json)

            self.assertEqual(new_edge.src_id, edge.src_id)
            self.assertEqual(new_edge.dst_id, edge.dst_id)
            self.assertDictEqual(new_edge.props, edge.props)
            self.assertDictEqual(new_edge.sysan, edge.sysan)


    def test_sessioned_path_insertion(self):
        """Test creation of a sessioned node path

        Test creation of a sample graph with pre-existing nodes
        in a single session
        """

        with self.g.session_scope():
            leaf_count = 10
            src_id = str(uuid.uuid4())
            dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
            self.g.node_merge(node_id=src_id, label='test')

            with self.g.session_scope() as session:
                for dst_id in dst_ids:
                    self.g.node_merge(
                        node_id=dst_id, label='test', session=session)

            with self.g.session_scope() as session:
                for dst_id in dst_ids:
                    node = self.g.node_lookup_one(
                        node_id=dst_id, session=session)
                    self.g.edge_insert(
                        PsqlEdge(src_id=src_id, dst_id=node.node_id, label='edge1'),
                        session=session
                    )

            edge_ids = [e.dst_id for e in self.g.edge_lookup(
                src_id=src_id)]
            self.assertEqual(len(set(edge_ids)), leaf_count)

            for dst_id in dst_ids:
                self.assertTrue(dst_id in set(edge_ids))

    def test_path_deletion(self):
        """Test path deletion

        Test path deletion and verify deletion is cascaded to edges
        """

        with self.g.session_scope():
            leaf_count = 10
            src_id = str(uuid.uuid4())
            dst_ids = [str(uuid.uuid4()) for i in range(leaf_count)]
            self.g.node_merge(node_id=src_id, label='test')

            # Create nodes and link them to source
            for dst_id in dst_ids:
                self.g.node_merge(node_id=dst_id, label='test')
                self.g.edge_insert(PsqlEdge(
                    src_id=src_id, dst_id=dst_id, label='edge1'))

            # Verify that the edges there are correct
            for dst_id in dst_ids:
                edge = self.g.edge_lookup_one(dst_id=dst_id)
                self.assertEqual(edge.src_id, src_id)

                edges = [e.dst_id for e in self.g.edge_lookup(src_id=src_id)]
                for dst_id in dst_ids:
                    self.assertTrue(dst_id in set(edges))

            # Delete all dst nodes
            for dst_id in dst_ids:
                self.g.node_delete(node_id=dst_id)

            # Make sure that there are no hanging edges
            edges = [e.dst_id for e in self.g.edge_lookup(src_id=src_id)]
            for dst_id in dst_ids:
                self.assertTrue(dst_id not in set(edges))
            for dst_id in dst_ids:
                self.assertIs(self.g.edge_lookup_one(dst_id=dst_id), None)

    @unittest.skip('deprecated')
    def test_node_validator_error(self):
        """Test node validator error"""

        with self.g.session_scope():
            node_id = str(uuid.uuid4())
            temp = self.g.node_validator.validate
            self.g.node_validator.validate = lambda x: False
            try:
                self.assertRaises(
                    ValidationError,
                    self.g.node_merge, node_id, label='test',
                )
            except:
                self.g.node_validator.validate = temp
                raise

    @unittest.skip('deprecated')
    def test_edge_validator_error(self):
        """Test edge validator error"""

        with self.g.session_scope():
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            temp = self.g.edge_validator.validate
            self.g.edge_validator.validate = lambda x: False
            self.g.node_merge(src_id, label='test')
            self.g.node_merge(dst_id, label='test')

            try:
                self.assertRaises(
                    ValidationError,
                    self.g.edge_insert,
                    PsqlEdge(src_id=src_id, dst_id=dst_id, label='test'),
                )
            except:
                self.g.edge_validator.validate = temp
                raise

    def test_get_nodes(self):
        """Test node get"""

        with self.g.session_scope():
            self._clear_tables()

            ret_node_ids = []
            node_ids = [str(uuid.uuid4()) for i in range(self.REPEAT_COUNT*10)]

            for node_id in node_ids:
                self.g.node_merge(node_id, label='test')

            nodes = self.g.get_nodes()

            for node in nodes:
                self.assertTrue(node.node_id in node_ids)
                ret_node_ids.append(node.node_id)
            for node_id in node_ids:
                self.assertTrue(node_id in ret_node_ids)
            self.assertEqual(len(node_ids), len(ret_node_ids))

    def test_get_edges(self):
        """Test edge get"""

        with self.g.session_scope():
            self._clear_tables()

            count = 10
            src_ids = [str(uuid.uuid4()) for i in range(count)]
            dst_ids = [str(uuid.uuid4()) for i in range(count)]

            for src_id, dst_id in zip(src_ids, dst_ids):
                self.g.node_merge(src_id, label='test')
                self.g.node_merge(dst_id, label='test')
                self.g.edge_insert(PsqlEdge(
                    src_id=src_id,
                    dst_id=dst_id,
                    label='edge1',
                ))

            edges = self.g.get_edges()
            ret_src_ids = []
            ret_dst_ids = []
            for edge in edges:
                self.assertTrue(edge.src_id in src_ids)
                self.assertTrue(edge.dst_id in dst_ids)
                ret_src_ids.append(edge.src_id)
                ret_dst_ids.append(edge.dst_id)
            for src_id in src_ids:
                self.assertTrue(src_id in ret_src_ids)
            for dst_id in dst_ids:
                self.assertTrue(dst_id in ret_dst_ids)

    def _create_subtree(self, parent_id, level=0):
        with self.g.session_scope():
            for i in range(5):
                node_id = str(uuid.uuid4())
                self.g.node_merge(
                    node_id=node_id, label='test'.format(level))
                self.g.edge_insert(PsqlEdge(
                    src_id=parent_id, dst_id=node_id, label='edge1'
                ))
                if level < 2:
                    self._create_subtree(node_id, level+1)

    def _walk_tree(self, node, level=0):
        with self.g.session_scope():
            for edge in node.edges_out:
                print('+--'*level + '>', edge.dst)
                self._walk_tree(edge.dst, level+1)

    def test_tree_walk(self):
        with self.g.session_scope():
            node_id = str(uuid.uuid4())
            self.g.node_merge(node_id=node_id, label='test')
            self._create_subtree(node_id)
            with self.g.session_scope() as session:
                node = self.g.node_lookup_one(node_id, session=session)
                self._walk_tree(node)

    def test_edge_multiplicity(self):
        with self.g.session_scope() as s:
            src_id = str(uuid.uuid4())
            dst_id = str(uuid.uuid4())
            foo_id = str(uuid.uuid4())
            self.g.node_merge(node_id=src_id, label='test')
            self.g.node_merge(node_id=dst_id, label='test')
            self.g.node_merge(node_id=foo_id, label='foo')
            self.g.edge_insert(PsqlEdge(
                src_id=src_id, dst_id=dst_id, label='edge1'))
            self.g.edge_insert(models.Edge2(src_id, foo_id))
            s.commit()
            self.assertEqual(len(list(self.g.edge_lookup(
                src_id=src_id, dst_id=dst_id))), 1)
            self.assertEqual(len(list(self.g.edge_lookup(
                src_id=src_id, dst_id=foo_id))), 1)
            self.assertRaises(
                Exception,
                self.g.edge_insert,
                models.Edge1(src_id=src_id, dst_id=dst_id)
            )

    def test_simple_automatic_session(self):
        idA = str(uuid.uuid4())
        with self.g.session_scope():
            self.g.node_insert(PsqlNode(idA, 'test'))
        with self.g.session_scope():
            self.g.node_lookup(idA).one()

    def test_rollback_automatic_session(self):
        """test_rollback_automatic_session

        Make sure that within a session scope, an error causes the
        entire scope to be rolled back even without an explicit
        session being passed

        """
        nid = str(uuid.uuid4())
        with self.assertRaises(IntegrityError):
            with self.g.session_scope():
                self.g.node_insert(PsqlNode(nid, 'test'))
                self.g.node_insert(PsqlNode(nid, 'test'))
        with self.g.session_scope():
            self.assertEqual(len(list(self.g.node_lookup(nid).all())), 0)

    def test_commit_automatic_session(self):
        """test_commit_automatic_session

        Make sure that when not wrapped in a session scope the
        successful commit of a conflicting node does not rollback
        previously committed nodes. (i.e. the statements don't inherit
        the same session)

        """
        nid = str(uuid.uuid4())
        self.g.node_insert(PsqlNode(nid, 'test'))
        self.assertRaises(
            IntegrityError,
            self.g.node_insert,
            PsqlNode(nid, 'test'))
        with self.g.session_scope():
            self.assertEqual(self.g.node_lookup(nid).one().label, 'test')
        self.assertFalse(self.g.has_session())

    def test_automatic_nested_session(self):
        """test_automatic_nested_session

        Make sure that given a call to explicitly nest session scopes,
        the nested session commits first

        """
        nid = str(uuid.uuid4())
        with self.assertRaises(IntegrityError):
            with self.g.session_scope():
                self.g.node_insert(PsqlNode(nid, 'test'))
                with self.g.session_scope(can_inherit=False):
                    self.g.node_insert(PsqlNode(nid, 'test'))
        with self.g.session_scope():
            self.assertEqual(
                self.g.node_lookup(nid).one().label, 'test')
        self.assertFalse(self.g.has_session())

    def test_automatic_nested_session2(self):
        """test_automatic_nested_session2

        Make sure that given a call to explicitly nest session scopes,
        failure of the nested session scope does not affect the parent
        scope.

        """
        id1 = str(uuid.uuid4())
        id2 = str(uuid.uuid4())
        self.g.node_insert(PsqlNode(id1, 'foo'))
        with self.g.session_scope():
            self.g.node_insert(PsqlNode(id2, 'test'))
            with self.assertRaises(IntegrityError):
                with self.g.session_scope(can_inherit=False):
                    self.g.node_insert(PsqlNode(id1, 'foo'))
        with self.g.session_scope():
            self.assertEqual(self.g.node_lookup(id2).one().label, 'test')
        self.assertFalse(self.g.has_session())

    def test_automatic_nested_session3(self):
        """test_automatic_nested_session3

        Also, verify that two statements in a nested session_scope
        inherit the same session (i.e. the session stack is working
        properly).

        """
        id1, id2, id3 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        self.g.node_insert(PsqlNode(id1, 'foo'))
        with self.g.session_scope():
            self.g.node_insert(PsqlNode(id2, 'test'))
            with self.assertRaises(IntegrityError):
                with self.g.session_scope(can_inherit=False):
                    self.g.node_insert(PsqlNode(id1, 'foo'))
                    self.g.node_insert(PsqlNode(id3, 'foo'))
        with self.g.session_scope():
            self.assertEqual(self.g.node_lookup(id2).one().label, 'test')
            self.assertEqual(self.g.node_lookup(id3).count(), 0)
        self.assertFalse(self.g.has_session())

    def test_automatic_nested_session_inherit_valid(self):
        """test_automatic_nested_session_inherit_valid

        Verify that implicitly nested session scopes correctly inherit
        the parent session for valid node insertion

        """
        id1, id2 = str(uuid.uuid4()), str(uuid.uuid4())
        with self.g.session_scope():
            self.g.node_insert(PsqlNode(id1, 'test'))
            with self.g.session_scope():
                    self.g.node_insert(PsqlNode(id2, 'foo'))
            self.assertEqual(
                self.g.node_lookup(id1).one().label, 'test')
            self.assertEqual(
                self.g.node_lookup(id2).one().label, 'foo')
        self.assertFalse(self.g.has_session())

    def test_automatic_nested_session_inherit_invalid(self):
        """test_automatic_nested_session_inherit_invalid

        Verify that implicitly nested session scopes correctly inherit
        the parent session.

        """
        id1, id2 = str(uuid.uuid4()), str(uuid.uuid4())
        with self.g.session_scope() as outer:
            self.g.node_insert(PsqlNode(id1, 'test'))
            with self.g.session_scope() as inner:
                self.assertEqual(inner, outer)
                self.g.node_insert(PsqlNode(id2, 'foo'))
                inner.rollback()
        with self.g.session_scope():
            self.assertEqual(self.g.node_lookup(id1).count(), 0)
            self.assertEqual(self.g.node_lookup(id2).count(), 0)
        self.assertFalse(self.g.has_session())

    def test_explicit_to_inherit_nested_session(self):
        """test_explicit_to_inherit_nested_session

        Verify that the children of an explicitly passed session scope
        inherit the explicit session and commit all updates.

        """
        id1, id2, id3 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        outer = self.g._new_session()  # don't do this
        with self.g.session_scope(outer):
            self.g.node_insert(PsqlNode(id1, 'test'))
            with self.g.session_scope() as inner:
                self.assertEqual(inner, outer)
                self.g.node_insert(PsqlNode(id2, 'foo'))
                with self.g.session_scope() as third:
                    self.assertEqual(third, outer)
                    self.g.node_insert(PsqlNode(id3, 'foo'))
        with self.g.session_scope():
            self.assertEqual(self.g.node_lookup(id2).count(), 0)
        outer.commit()
        with self.g.session_scope():
            self.assertEqual(self.g.node_lookup(id1).count(), 1)
            self.assertEqual(self.g.node_lookup(id2).count(), 1)
            self.assertEqual(self.g.node_lookup(id3).count(), 1)
        self.assertFalse(self.g.has_session())

    def test_explicit_to_inherit_nested_session_rollback(self):
        """test_explicit_to_inherit_nested_session_rollback

        Verify that the children of an explicitly passed session scope
        inherit the explicit session and rolls back all levels.

        """
        id1, id2, id3 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        outer = self.g._new_session()  # don't do this
        with self.g.session_scope(outer):
            self.g.node_insert(PsqlNode(id1, 'test'))
            with self.g.session_scope() as inner:
                self.assertEqual(inner, outer)
                self.g.node_insert(PsqlNode(id2, 'foo'))
                with self.g.session_scope() as third:
                    self.assertEqual(third, outer)
                    self.g.node_insert(PsqlNode(id3, 'foo'))
                    third.rollback()
        with self.g.session_scope():
            self.assertEqual(self.g.node_lookup(id1).count(), 0)
            self.assertEqual(self.g.node_lookup(id2).count(), 0)
            self.assertEqual(self.g.node_lookup(id3).count(), 0)
        self.assertFalse(self.g.has_session())

    def test_mixed_session_inheritance(self):
        """test_mixed_session_inheritance

        Verify that an explicit session passed to a middle level in a
        tripple nested session_scope is independent from the outer and
        inner levels.

        """
        id1, id2, id3 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        external = self.g._new_session()
        with self.g.session_scope() as outer:
            self.g.node_insert(PsqlNode(id1, 'foo'))
            with self.g.session_scope(external) as inner:
                self.assertEqual(inner, external)
                self.assertNotEqual(inner, outer)
                self.g.node_insert(PsqlNode(id2, 'test'))
                with self.g.session_scope(outer) as third:
                    self.assertEqual(third, outer)
                    self.g.node_insert(PsqlNode(id3, 'foo'))
                    third.rollback()
        with self.g.session_scope():
            self.assertEqual(self.g.node_lookup(id2).count(), 0)
        external.commit()
        with self.g.session_scope():
            self.assertEqual(self.g.node_lookup(id1).count(), 0)
            self.assertEqual(self.g.node_lookup(id2).count(), 1)
            self.assertEqual(self.g.node_lookup(id3).count(), 0)
        self.assertFalse(self.g.has_session())

    def test_explicit_session(self):
        """test_explicit_session

        Verify that passing a session explicitly functions as expected

        """
        id1, id2 = str(uuid.uuid4()), str(uuid.uuid4())
        with self.g.session_scope() as session:
            self.g.node_insert(PsqlNode(id1, 'foo'))
            self.g.node_insert(PsqlNode(id2, 'foo'), session)
            session.rollback()
        with self.g.session_scope():
            self.assertEqual(self.g.node_lookup(id2).count(), 0)
            self.assertEqual(self.g.node_lookup(id1).count(), 0)
        self.assertFalse(self.g.has_session())

    def test_library_functions_use_session_implicitly(self):
        """Test that library functions use the session they're scoped in

        """
        id1 = str(uuid.uuid4())
        with self.g.session_scope():
            self.g.node_insert(PsqlNode(id1, 'test'))
            self.g.node_lookup(node_id=id1).one()


def no_allowed_2_please(edge):
    if not isinstance(edge.src, models.Foo):
        return True

    node = edge.src
    if node.baz == 'allowed_2':
        return False

    return True


class TestPsqlGraphTraversal(PsqlgraphBaseTest):

    def setUp(self):
        """
        Setting up a subgraph that we can later traverse.
        NOTE: Edge directions are as follows: Test->Test->Foo->FooBar and
        Test->FooBar, i.e. Foo/Test will be in FooBar's edges_in, Test will be
        in Foo's edges_in and Test will be in Test's edges_in

        Edges look like this:
        root_node <- foo1
        root_node <- foo2
        root_node <- foo3
        root_node <- test1
        foo1 <- test1
        foo1 <- test2
        foo2 <- test3
        test1 <- test4
        test2 <- test5

        """
        super(TestPsqlGraphTraversal, self).setUp()

        with self.g.session_scope() as session:
            root_node = models.FooBar(node_id="root", bar='root')

            foo1 = models.Foo(node_id="foo1", bar='foo1', baz='allowed_2')
            foo2 = models.Foo(node_id="foo2", bar='foo2', baz='allowed_1')
            foo3 = models.Foo(node_id="foo3", bar='foo3', baz='allowed_1')

            test1 = models.Test(node_id="", key1='test1')
            test2 = models.Test(node_id="test2", key1='test2')
            test3 = models.Test(node_id="test3", key1='test3')
            test4 = models.Test(node_id=str(uuid.uuid4()), key1='test4')
            test5 = models.Test(node_id="test5", key1='test5')

            root_node.tests.append(test1)

            for foo in [foo1, foo2, foo3]:
                root_node.foos.append(foo)

            for test in [test1, test2]:
                foo1.tests.append(test)

            test1.sub_tests.append(test4)

            foo2.tests.append(test3)

            test2.sub_tests.append(test5)

            session.add(root_node)

        # These nodes should have the sysan_flag set, when predicate active
        self.sysan_flag_nodes = [root_node, foo2, foo3, test1, test3, test4]
        # These nodes shouldn't have sysan_flag set, when predicate active
        self.not_sysan_flag_nodes = [foo1, test2, test5]
        # These are expected nodes for a given depth
        self.depths_results = {
            0: [root_node],
            1: [root_node, foo1, foo2, foo3, test1],
            2: [root_node, foo1, foo2, foo3, test1, test2, test3, test4],
            3: [root_node, foo1, foo2, foo3, test1, test2, test3, test4, test5]
        }

    def tearDown(self):
        print("tear down in progress")
        super(TestPsqlGraphTraversal, self).tearDown()

    def test_default_traversal(self):
        """
        Default traversal should return all nodes
        """
        with self.g.session_scope():
            root = self.g.nodes(models.FooBar).first()
            traversal = {n.node_id for n in root.bfs_children()}

            nodes_all_set = {n.node_id for n in self.g.nodes().all()}

        self.assertEqual(traversal, nodes_all_set)

    def test_traversal_with_predicate(self):
        """
        Traversal with predicate should return only self.sysan_flag_nodes
        """
        with self.g.session_scope():
            root = self.g.nodes(models.FooBar).first()

            gen = root.bfs_children(edge_predicate=no_allowed_2_please)
            traversal = {n.node_id for n in gen}

        expected_ids = {n.node_id for n in self.sysan_flag_nodes}
        self.assertEqual(expected_ids, traversal)

    @parameterized.expand([
        ('zero', 0),
        ('one', 1),
        ('two', 2),
        ('three', 3),
    ])
    def test_traversal_with_max_depth(self, _, depth):
        """
        Traversal should return only self.depths_results[depth] nodes
        """
        with self.g.session_scope():
            root = self.g.nodes(models.FooBar).first()

            gen = root.bfs_children(max_depth=depth)
            traversal = [n for n in gen]

        expected_ids = {n.node_id for n in self.depths_results[depth]}
        traversal_ids = {n.node_id for n in traversal}
        # make sure that traversal size is as expected
        self.assertEqual(len(self.depths_results[depth]), len(traversal))
        # make sure the results of the traversal are as expected
        self.assertEqual(expected_ids, traversal_ids)

    def test_directed_traversal(self):
        """ Tests walking towards the root node from a leaf """
        with self.g.session_scope():
            leaf = self.g.nodes().props(key1="test5").first()
            expected = ['test5', 'test2', 'foo1', 'root']
            actual = [node.node_id for node in leaf.traverse(edge_pointer="out")]
            self.assertListEqual(expected, actual)

            leaf = self.g.nodes().props(key1="test3").first()
            expected = ['test3', 'foo2', 'root']
            actual = [node.node_id for node in leaf.traverse(edge_pointer="out")]
            self.assertListEqual(expected, actual)


if __name__ == '__main__':

    def run_test(test):
        suite = unittest.TestLoader().loadTestsFromTestCase(test)
        unittest.TextTestRunner(verbosity=2).run(suite)

    run_test(TestPsqlGraphDriver)

    run_test(TestPsqlGraphTraversal)
