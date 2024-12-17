from psqlgraph.graph import Base, CommonBase
from psqlgraph.voided import VoidedBase

__all__ = ("CommonBase", "Base", "VoidedBase")


def create_all(engine, base=Base):
    """Create all tables associated with the provided declarative base, defaults to
        ORMBase if not specified
    Args:
        engine (sqlalchemy.engine.Engine): active engine instance
        base (sqlalchemy.ext.declarative.DeclarativeMeta): a declarative base class
    """
    base.metadata.create_all(engine)
    VoidedBase.metadata.create_all(engine)


def drop_all(engine, base=Base):
    """Drops all tables associated with a given delcarative base
    Args:
        engine (sqlalchemy.engine.Engine): active engine instance
        base (sqlalchemy.ext.declarative.DeclarativeMeta): a declarative base class
    """
    base.metadata.drop_all(engine)
    VoidedBase.metadata.drop_all(engine)
