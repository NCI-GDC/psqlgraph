class PSQLGraphError(Exception):
    """Base class for PostgreSQL Graph Errors.
    """
    pass


class QueryError(PSQLGraphError):
    pass


class ProgrammingError(PSQLGraphError):
    pass


class NodeCreationError(PSQLGraphError):
    pass


class EdgeCreationError(PSQLGraphError):
    pass


class ValidationError(PSQLGraphError):
    pass


class SessionClosedError(PSQLGraphError):
    """An operation was requested from a closed session.
    """
    pass
