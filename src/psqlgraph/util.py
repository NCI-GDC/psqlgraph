import logging

from psqlgraph.exc import ValidationError

logger = logging.getLogger(__name__)


def validate(f, value, types, enum=None):
    """Validation decorator types for hybrid_properties"""
    if enum:
        if value not in enum and value is not None:
            raise ValidationError(
                "Value '{}' not in allowed value list for {} for property {}.".format(
                    value, enum, f.__name__
                )
            )
    if not types:
        return

    _types = types + (type(None),)
    if str in types:
        _types = _types + (str,)

    if not isinstance(value, _types):
        raise ValidationError(
            (
                "Value '{}' is of type {} and is not one of the allowed types "
                "for property {}: {}."
            ).format(value, type(value), f.__name__, _types)
        )


def sanitize(properties):
    sanitized = {}
    for key, value in properties.items():
        if not value or isinstance(value, (int, bool, list, float, type(None))):
            sanitized[str(key)] = value
        elif isinstance(value, str):
            sanitized[str(key)] = str(value)
        else:
            raise ValueError(f"Cannot serialize {type(value)} to JSONB property")
    return sanitized
