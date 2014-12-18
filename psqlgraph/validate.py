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


def validate_no_unexpected_props(schema, datum):
    """The default avro validator does not validate the absence of
    unexpected props, so we do this as a separate step."""
    if schema.type in ['union', 'error_union']:
        return any((validate_no_unexpected_props(s, datum) for s in schema.schemas))
    if schema.type in ['record', 'error', 'request']:
        if not isinstance(datum, dict):
            return False
        field_names = [f.name for f in schema.fields]
        if any((k not in field_names for k in datum.keys())):
            return False
        else:
            return all((validate_no_unexpected_props(f.type, datum[f.name])
                        for f in schema.fields))
    else:
        return True


class AvroNodeValidator(PsqlNodeValidator):
    def __init__(self, schema):
        self.schema = schema

    def munge_node_into_dict(self, node):
        res = {}
        res["label"] = node.label
        res["properties"] = node.properties
        res["id"] = node.node_id
        return res

    def validate(self, node, *args, **kwargs):
        node_as_dict = self.munge_node_into_dict(node)
        return (avro_validate(self.schema, node_as_dict)
                and validate_no_unexpected_props(self.schema, node_as_dict))
