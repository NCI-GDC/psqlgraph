import uuid

from sqlalchemy.types import TypeDecorator, Text


class PSQLGraphUUID(TypeDecorator):
    """ PSQLGraph UUID type.
    """
    impl = Text

    def process_bind_param(self, value, dialect):
        return str(uuid.UUID(value)).lower()

    def process_result_value(self, value, dialect):
        return value
