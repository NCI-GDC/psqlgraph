from avro.io import validate as avro_validate


class PsqlEdgeValidator(object):
    def __init__(self, driver, *args, **kwargs):
        self.driver = driver

    def __call__(self, edge, *args, **kwargs):
        return self.validate(edge, *args, **kwargs)

    def validate(self, edge, *args, **kwargs):
        return True


class PsqlNodeValidator(object):
    def __init__(self, driver, *args, **kwargs):
        self.driver = driver

    def __call__(self, node, *args, **kwargs):
        return self.validate(node, *args, **kwargs)

    def validate(self, node, *args, **kwargs):
        return True


class AvroNodeValidator(PsqlNodeValidator):
    def __init__(self, schema):
        # TODO sanity checks on schema here, it needs to have three fields:
        # id, label, and properties
        self.schema = schema

    def munge_node_into_dict(self, node):
        res = {}
        res["type"] = node.label
        res["properties"] = node.properties
        res["id"] = node.node_id
        return res

    def validate(self, node, *args, **kwargs):
        node_as_dict = self.munge_node_into_dict(node)
        return avro_validate(self.schema, node_as_dict)
