import logging
import random
import time
from functools import wraps
from types import FunctionType

from sqlalchemy.exc import IntegrityError

from psqlgraph.exc import ValidationError

#  PsqlNode modules
DEFAULT_RETRIES = 0


def validate(f, value, types, enum=None):
    """Validation decorator types for hybrid_properties"""
    if enum:
        if value not in enum and value is not None:
            raise ValidationError(
                ("Value '{}' not in allowed value list for {} for property {}.").format(
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


def pg_property(*pg_args, **pg_kwargs):
    if len(pg_args) == 1 and isinstance(pg_args[0], FunctionType):
        fn = pg_args[0]
        fn.__pg_setter__ = True
        fn.__pg_types__ = None
        fn.__pg_enum__ = pg_kwargs.get("enum", None)
        return fn

    def decorator(fn):
        fn.__pg_setter__ = True
        fn.__pg_types__ = pg_args
        fn.__pg_enum__ = pg_kwargs.get("enum", None)

        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        return wrapper

    return decorator


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


def default_backoff(retries, max_retries):
    """This is the default backoff function used in the case of a retry by
    and function wrapped with the ``@retryable`` decorator.

    The behavior of the default backoff function is to sleep for a
    pseudo-random time between 0 and 2 seconds.

    """

    time.sleep(random.random() * (max_retries - retries) / max_retries * 2)


def retryable(func):
    """This wrapper can be used to decorate a function to retry an
    operation in the case of an SQLalchemy IntegrityError.  This error
    means that a race-condition has occured and operations that have
    occured within the session may no longer be valid.

    You can set the number of retries by passing the keyword argument
    ``max_retries`` to the wrapped function.  It's therefore important
    that ``max_retries`` is included as a kwarg in the definition of
    the wrapped function.

    Setting ``max_retries`` to 0 will prevent retries upon failure;
    wrapped function will execute once.

    Similar to ``max_retries``, the kwarg ``backoff`` is a callback
    function that allows the user of the library to over-ride the
    default backoff function in the case of a retry.  See `func
    default_backoff`

    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        retries = 0
        max_retries = kwargs.get("max_retries", DEFAULT_RETRIES)
        backoff = kwargs.get("backoff", default_backoff)
        while retries <= max_retries:
            try:
                return func(*args, **kwargs)
            except IntegrityError:
                logging.debug(f"Race-condition caught? ({retries}/{max_retries} retries)")
                if retries >= max_retries:
                    logging.error(f"Unable to execute {func}, max retries exceeded")
                    raise
                retries += 1
                backoff(retries, max_retries)

    return wrapper
