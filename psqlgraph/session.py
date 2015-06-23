import sqlalchemy

from sqlalchemy.exc import ResourceClosedError
from sqlalchemy.orm.session import Session


# NOTE TODO Find a better way to handle method docstring inheritance?
def inherit_docstring_from(cls):
    def docstring_inheriting_decorator(fn):
        fn.__doc__ = getattr(cls,fn.__name__).__doc__
        return fn
    return docstring_inheriting_decorator


class ShortSession(Session):
    """A short-lived session that prohibits use after it is closed.

    """
    __doc__ += Session.__doc__

    @inherit_docstring_from(Session)
    def __init__(self, *args, **kwargs):
        
        self.closed = False
        
        return super(ShortSession, self).__init__(*args, **kwargs)

    @inherit_docstring_from(Session)
    def connection(self, *args, **kwargs):
        
        if self.closed:
            raise ResourceClosedError('short session closed')
        
        return super(ShortSession, self).connection(*args, **kwargs)

    def close(self, *args, **kwargs):
        """Close this Session.
        
        This clears all items and ends any transaction in progress.
        
        This also closes and prevents any new connections from being opened.
        
        """
        
        self.closed = True
        
        return super(ShortSession, self).close(*args, **kwargs)
