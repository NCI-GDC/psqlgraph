import time
import random
import logging
from functools import wraps

from types import FunctionType

from sqlalchemy.exc import IntegrityError
from psqlgraph.exc import ValidationError

#  PsqlNode modules
DEFAULT_RETRIES = 0


def validate(f, value, types, enum=None):
    """Validation decorator types for hybrid_properties

    """
    if enum:
        if value not in enum and value is not None:
            raise ValidationError((
                "Value '{}' not in allowed value list for {} for property {}."
            ).format(value, enum, f.__name__))
    if not types:
        return
    _types = types+(type(None),)
    # If type is str, accept unicode as well, it will be sanitized
    if str in _types and type(value).__name__ == 'unicode':
        return

    if not isinstance(value, _types):
        raise ValidationError((
            "Value '{}' is of type {} and is not one of the allowed types "
            "for property {}: {}."
        ).format(value, type(value), f.__name__, _types))


def pg_property(*pg_args, **pg_kwargs):
    if len(pg_args) == 1 and isinstance(pg_args[0], FunctionType):
        fn = pg_args[0]
        fn.__pg_setter__ = True
        fn.__pg_types__ = None
        fn.__pg_enum__ = pg_kwargs.get('enum', None)
        return fn

    def decorator(fn):
        fn.__pg_setter__ = True
        fn.__pg_types__ = pg_args
        fn.__pg_enum__ = pg_kwargs.get('enum', None)

        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def sanitize(properties):
    sanitized = {}
    for key, value in properties.items():
        v_type = type(value).__name__
        if v_type in ['list', 'int', 'str', 'bool', 'long', 'float', 'NoneType']:
            sanitized[str(key)] = value
        elif v_type == 'unicode':
            sanitized[str(key)] = value.encode('ascii', 'ignore')
        else:
            raise ValueError(
                'Cannot serialize {} to JSONB property'.format(type(value)))
    return sanitized


def default_backoff(retries, max_retries):
    """This is the default backoff function used in the case of a retry by
    and function wrapped with the ``@retryable`` decorator.

    The behavior of the default backoff function is to sleep for a
    pseudo-random time between 0 and 2 seconds.

    """

    time.sleep(random.random()*(max_retries-retries)/max_retries*2)


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
        max_retries = kwargs.get('max_retries', DEFAULT_RETRIES)
        backoff = kwargs.get('backoff', default_backoff)
        while retries <= max_retries:
            try:
                return func(*args, **kwargs)
            except IntegrityError:
                logging.debug(
                    'Race-condition caught? ({0}/{1} retries)'.format(
                        retries, max_retries))
                if retries >= max_retries:
                    logging.error(
                        'Unabel to execute {f}, max retries exceeded'.format(
                            f=func))
                    raise
                retries += 1
                backoff(retries, max_retries)
            else:
                break
    return wrapper
