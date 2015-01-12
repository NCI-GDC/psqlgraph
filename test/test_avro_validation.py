import uuid
import unittest
from psqlgraph import PsqlGraphDriver, PsqlNode, PsqlEdge
from psqlgraph.exc import ValidationError
from psqlgraph.validate import AvroNodeValidator, AvroEdgeValidator

from avro.schema import make_avsc_object


host = 'localhost'
user = 'test'
password = 'test'
database = 'automated_test'


class TestAvroValidation(unittest.TestCase):

    def setUp(self):
        self.driver = PsqlGraphDriver(host, user, password, database)

    def tearDown(self):
        conn = self.driver.engine.connect()
        conn.execute('commit')
        conn.execute('delete from edges')
        conn.execute('delete from nodes')
        conn.close()

    def avro_node_schema_dict(self, label, props):
        return {
            "name": label,
            "type": "record",
            "fields": [
                {"name": "id", "type": "string"},
                {
                    "name": "label",
                    "type": [{"symbols": [label],
                              "type": "enum",
                              "name": label + "_enum"}],
                },
                {
                    "name": "properties",
                    "type": [{"type": "record", "name": label+"properties",
                              "fields": [{"name": k, "type": v}
                                         for k, v in props.iteritems()]}],
                },
            ]
        }

    def make_avro_node_schema(self, label_to_drops_dict):
        return make_avsc_object([self.avro_node_schema_dict(label, props)
                                 for label, props in label_to_drops_dict.iteritems()])

    def test_basic_avro_node_validation_succeeds(self):
        self.driver.node_validator = AvroNodeValidator(
            self.make_avro_node_schema({"file": {"file_name": "string"}}))
        node = PsqlNode(str(uuid.uuid4()), "file",
                        properties={"file_name": "foobar.txt"})
        self.driver.node_insert(node)

    def test_optional_props_must_be_set_explicitly(self):
        self.driver.node_validator = AvroNodeValidator(
            self.make_avro_node_schema({"baz": {"foo": "null"}}))
        node = PsqlNode(str(uuid.uuid4()), "baz", properties={})
        with self.assertRaises(ValidationError):
            self.driver.node_insert(node)

    def test_avro_validation_catches_errors_on_update(self):
        self.driver.node_validator = AvroNodeValidator(
            self.make_avro_node_schema({"file": {"file_name": "string"}}))
        node = PsqlNode(str(uuid.uuid4()), "file",
                        properties={"file_name": "foobar.txt"})
        self.driver.node_insert(node)
        with self.assertRaises(ValidationError):
            self.driver.node_merge(node=node, properties={"file_name": 3})

    def test_nonstring_id_causes_avro_validtion_failure(self):
        self.driver.node_validator = AvroNodeValidator(
            self.make_avro_node_schema({"file": {"file_name": "string"}}))
        node = PsqlNode(345, "file",
                        properties={"file_name": "foobar.txt"})
        with self.assertRaises(ValidationError):
            self.driver.node_insert(node)

    def test_prop_type_mismatch_causes_avro_validation_failure(self):
        self.driver.node_validator = AvroNodeValidator(
            self.make_avro_node_schema({"file": {"file_name": "string"}}))
        node = PsqlNode(str(uuid.uuid4()), "file",
                        properties={"file_name": 3})
        with self.assertRaises(ValidationError):
            self.driver.node_insert(node)

    def test_missing_prop_types_causes_avro_validation_failure(self):
        self.driver.node_validator = AvroNodeValidator(
            self.make_avro_node_schema({"file": {"file_name": "string",
                                                 "md5": "string"}}))
        node = PsqlNode(str(uuid.uuid4()), "file",
                        properties={"file_name": "foobar.txt"})
        with self.assertRaises(ValidationError):
            self.driver.node_insert(node)

    def test_avro_validation_succeeds_with_multiple_node_types(self):
        schema = self.make_avro_node_schema({"file": {"file_name": "string"},
                                             "participant": {"name": "string",
                                                             "age": "int"}})
        self.driver.node_validator = AvroNodeValidator(schema)
        file_node = PsqlNode(str(uuid.uuid4()), "file",
                             properties={"file_name": "foobar.txt"})
        participant_node = PsqlNode(str(uuid.uuid4()), "participant",
                                    properties={"name": "josh", "age": 23})
        self.driver.node_insert(file_node)
        self.driver.node_insert(participant_node)

    def test_avro_validation_failure_with_multiple_node_types(self):
        schema = self.make_avro_node_schema({"file": {"file_name": "string"},
                                             "participant": {"name": "string",
                                                             "age": "int"}})
        self.driver.node_validator = AvroNodeValidator(schema)
        file_node = PsqlNode(str(uuid.uuid4()), "file",
                             properties={"file_name": "foobar.txt"})
        participant_node = PsqlNode(str(uuid.uuid4()), "participant",
                                    properties={"name": "josh", "age": "23"})
        self.driver.node_insert(file_node)
        with self.assertRaises(ValidationError):
            self.driver.node_insert(participant_node)

    def test_avro_validation_fails_if_unexpected_properties_are_present(self):
        self.driver.node_validator = AvroNodeValidator(
            self.make_avro_node_schema({"file": {"file_name": "string"}}))
        node = PsqlNode(str(uuid.uuid4()), "file",
                        properties={"file_name": "foobar.txt",
                                    "someting_else": "bazquux"})
        with self.assertRaises(ValidationError):
            self.driver.node_insert(node)

    def test_avro_node_validation_checks_on_update(self):
        self.driver.node_validator = AvroNodeValidator(
            self.make_avro_node_schema({"file": {"file_name": "string"}}))
        node = PsqlNode(str(uuid.uuid4()), "file",
                        properties={"file_name": "foobar.txt",
                                    "someting_else": "bazquux"})
        with self.assertRaises(ValidationError):
            self.driver.node_insert(node)

    def avro_edge_node_labels_dict(self, label_pairs):
        return [
            {"type": "record",
             "name": src + ":" + dst,
             "fields": [
                 {
                     "type": {
                         "symbols": [src],
                         "type": "enum",
                         "name": str(uuid.uuid4()) + src + "_enum",
                     },
                     "name": "src_node_label"
                 },
                 {
                     "type": {
                         "symbols": [dst],
                         "type": "enum",
                         "name": str(uuid.uuid4()) + dst + "_enum"
                     },
                     "name": "dst_node_label"
                 }
                 ]
             } for src, dst in label_pairs.items()]

    def make_avro_edge_schema(self, label, node_label_pairs, props={}):
        return {
            "name": label,
            "type": "record",
            "fields": [
                {"type": "string", "name": "id"},
                {"type": [{"symbols": [label],
                           "type": "enum",
                           "name": label + "_enum"}],
                 "name": "label"},
                {
                    "name": "node_labels",
                    "type": self.avro_edge_node_labels_dict(node_label_pairs)
                },
                {
                    "type": {
                        "type": "record",
                        "name": str(uuid.uuid4()) + "src_dst_nodes",
                        "fields": [
                            {"type": "string", "name": "src_node_id"},
                            {"type": "string", "name": "dst_node_id"}
                        ]
                    },
                    "name": "node_ids"
                },
                {
                    "name": "properties",
                    "type": [{"type": "record", "name": label+"properties",
                              "fields": [{"name": k, "type": v}
                                         for k, v in props.iteritems()]}],
                },
            ]
        }

    def insert_edge(self, label, from_label, to_label, props={}):
        node1 = self.driver.node_insert(PsqlNode(node_id=str(uuid.uuid4()),
                                                 label=from_label))
        node2 = self.driver.node_insert(PsqlNode(node_id=str(uuid.uuid4()),
                                                 label=to_label))
        edge = PsqlEdge(src_id=node1.node_id,
                        dst_id=node2.node_id,
                        label=label,
                        properties=props)
        return self.driver.edge_insert(edge)

    def avsc_from_dict(self, *args, **kwargs):
        return make_avsc_object(self.make_avro_edge_schema(*args, **kwargs))

    def test_simple_edge_validation_succeeds(self):
        schema = self.avsc_from_dict("derived_from",
                                     {"file": "aliquot"},
                                     props={"count": "int"})
        self.driver.edge_validator = AvroEdgeValidator(schema)
        self.insert_edge("derived_from", "file", "aliquot", props={"count": 1})

    def test_edge_validation_happens_on_updates(self):
        schema = self.avsc_from_dict("derived_from",
                                     {"file": "aliquot"},
                                     props={"count": "int"})
        self.driver.edge_validator = AvroEdgeValidator(schema)
        edge = self.insert_edge("derived_from", "file", "aliquot", props={"count": 1})
        with self.assertRaises(ValidationError):
            self.driver.edge_update(edge, properties={"count": "foo"})

    def test_edge_validation_fails_on_missing_properites(self):
        schema = self.avsc_from_dict("derived_from",
                                     {"file": "aliquot"},
                                     props={"count": "int"})
        self.driver.edge_validator = AvroEdgeValidator(schema)
        with self.assertRaises(ValidationError):
            self.insert_edge("derived_from", "file", "aliquot")

    def test_edge_validation_fails_on_unkown_node_label(self):
        schema = self.avsc_from_dict("derived_from",
                                     {"file": "aliquot"})
        self.driver.edge_validator = AvroEdgeValidator(schema)
        with self.assertRaises(ValidationError):
            self.insert_edge("derived_from", "file", "something_else")

    def test_edge_validation_fails_on_ill_typed_properties(self):
        schema = self.avsc_from_dict("derived_from",
                                     {"file": "aliquot"},
                                     props={"count": "int"})
        self.driver.edge_validator = AvroEdgeValidator(schema)
        with self.assertRaises(ValidationError):
            self.insert_edge("derived_from", "file", "aliquot",
                             props={"count": "1"})

    def test_edge_validation_fails_on_unexpected_properites(self):
        schema = self.avsc_from_dict("derived_from",
                                     {"file": "aliquot"})
        self.driver.edge_validator = AvroEdgeValidator(schema)
        with self.assertRaises(ValidationError):
            self.insert_edge("derived_from", "file", "aliquot",
                             props={"unknown": "foo"})

    def test_edge_validation_fails_on_unkown_edge_label(self):
        schema = self.avsc_from_dict("derived_from",
                                     {"file": "aliquot"})
        self.driver.edge_validator = AvroEdgeValidator(schema)
        with self.assertRaises(ValidationError):
            self.insert_edge("random_label", "file", "something_else")

    def test_edge_validation_with_multiple_node_labels_succeeds(self):
        schema = self.avsc_from_dict("derived_from",
                                     {"file": "aliquot",
                                      "aliquot": "analyte"})
        self.driver.edge_validator = AvroEdgeValidator(schema)
        self.insert_edge("derived_from", "file", "aliquot")
        self.insert_edge("derived_from", "aliquot", "analyte")

    def test_edge_validation_with_multiple_node_labels_fails(self):
        schema = self.avsc_from_dict("derived_from",
                                     {"file": "aliquot",
                                      "aliquot": "analyte"})
        self.driver.edge_validator = AvroEdgeValidator(schema)
        self.insert_edge("derived_from", "file", "aliquot")
        with self.assertRaises(ValidationError):
            self.insert_edge("derived_from", "something_else", "analyte")

    def test_edge_validation_with_union_schema_succeeds(self):
        schema_dicts = [self.make_avro_edge_schema("derived_from",
                                                   {"aliquot": "analyte"}),
                        self.make_avro_edge_schema("member_of",
                                                   {"file": "archive"})]
        schema = make_avsc_object(schema_dicts)
        self.driver.edge_validator = AvroEdgeValidator(schema)
        self.insert_edge("derived_from", "aliquot", "analyte")
        self.insert_edge("member_of", "file", "archive")

    def test_edge_validation_with_union_schema_fails(self):
        schema_dicts = [self.make_avro_edge_schema("derived_from",
                                                   {"aliquot": "analyte"}),
                        self.make_avro_edge_schema("member_of",
                                                   {"file": "archive"})]
        schema = make_avsc_object(schema_dicts)
        self.driver.edge_validator = AvroEdgeValidator(schema)
        self.insert_edge("derived_from", "aliquot", "analyte")
        self.insert_edge("member_of", "file", "archive")
        with self.assertRaises(ValidationError):
            self.insert_edge("random_edge", "file", "archive")
        with self.assertRaises(ValidationError):
            self.insert_edge("member_of", "file", "analyte")
