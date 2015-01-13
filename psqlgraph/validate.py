import logging
from avro.io import validate as avro_validate

logger = logging.getLogger('psqlgraph.validate')


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
        return any((validate_no_unexpected_props(s, datum)
                    for s in schema.schemas))
    if schema.type in ['record', 'error', 'request']:
        if not isinstance(datum, dict):
            return False
        field_names = [f.name for f in schema.fields]
        if any((k not in field_names for k in datum.keys())):
            return False
        if any((name not in datum.keys() for name in field_names)):
            # this is a bit of an overloading of the name of this
            # function.  it verifies that all field names in a record
            # schema exist in the datum. avro does not do this by
            # default for optional properties
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
        return (avro_validate(self.schema, node_as_dict) and
                validate_no_unexpected_props(self.schema, node_as_dict))


class AvroEdgeValidator(PsqlEdgeValidator):
    def __init__(self, schema):
        self.schema = schema

    def munge_edge_into_dict(self, edge):
        res = {}
        res["id"] = str(edge.key)
        res["label"] = edge.label
        res["node_labels"] = {"src_node_label": edge.src.label,
                              "dst_node_label": edge.dst.label}
        res["node_ids"] = {"src_node_id": edge.src_id,
                           "dst_node_id": edge.dst_id}
        res["properties"] = edge.properties
        logger.debug('{}: ({})-[{}]->({})'.format(
            edge, edge.src.label, edge.label, edge.dst.label))
        return res

    def validate(self, edge, *args, **kwargs):
        if not edge.src:
            logger.error('Edge {} has no source.'.format(edge))
            return False
        if not edge.dst:
            logger.error('Edge {} has no desination.'.format(edge))
            return False

        edge_as_dict = self.munge_edge_into_dict(edge)
        return (avro_validate(self.schema, edge_as_dict) and
                validate_no_unexpected_props(self.schema, edge_as_dict))
