from sqlalchemy.orm.session import Session

from psqlgraph import exc


# NOTE TODO Find a better way to handle method docstring inheritance?
def inherit_docstring_from(cls):
    def docstring_inheriting_decorator(fn):
        fn.__doc__ = getattr(cls,fn.__name__).__doc__
        return fn
    return docstring_inheriting_decorator


class GraphSession(Session):
    __doc__ = Session.__doc__

    @inherit_docstring_from(Session)
    def __init__(self, *args, **kwargs):

        self._psqlgraph_closed = False

        super(GraphSession, self).__init__(*args, **kwargs)

    @inherit_docstring_from(Session)
    def connection(self, *args, **kwargs):

        if self._psqlgraph_closed:
            raise exc.SessionClosedError('session closed')

        return super(GraphSession, self).connection(*args, **kwargs)

    def close(self, *args, **kwargs):
        """Close this Session.

        This clears all items and ends any transaction in progress.

        This also closes and prevents any new connections from being opened.

        """

        self._psqlgraph_closed = True

        return super(GraphSession, self).close()
