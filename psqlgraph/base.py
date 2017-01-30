from attributes import PropertiesDict, SystemAnnotationDict
from sqlalchemy import Column, Text, DateTime, text, event
from sqlalchemy.dialects.postgres import ARRAY, JSONB, UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import object_session, sessionmaker
from sqlalchemy.orm.util import polymorphic_union
from util import sanitize, validate


abstract_classes = ['Node', 'Edge', 'Base']
NODE_TABLENAME_SCHEME = 'node_{class_name}'
EDGE_TABLENAME_SCHEME = 'edge_{class_name}'

#: The column type of ``node_id``, UUID treated as a string
NODE_ID_TYPE = UUID(as_uuid=False)


class CommonBase(object):

    _session_hooks_before_insert = []
    _session_hooks_before_update = []
    _session_hooks_before_delete = []

    # ======== Columns ========
    created = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    acl = Column(
        ARRAY(Text),
        default=list(),
    )

    _sysan = Column(
        # WARNING: Do not update this column directly. See
        # `.system_annotations`
        JSONB,
        default={},
    )

    _props = Column(
        # WARNING: Do not update this column directly.
        # See `.properties` or `.props`
        JSONB,
        default={},
    )

    @classmethod
    def get_label(cls):
        return getattr(cls, '__label__', cls.__name__.lower())

    # ======== Table Attributes ========
    @declared_attr
    def __mapper_args__(cls):
        name = cls.__name__
        if name in abstract_classes:
            pjoin = polymorphic_union({
                scls.__tablename__: scls.__table__ for scls in
                cls.get_subclasses()}, 'type')
            return {
                'polymorphic_identity': name,
                'with_polymorphic': ('*', pjoin),
            }
        else:
            return {
                'polymorphic_identity': name,
                'concrete': True,
            }

    def __init__(self, *args, **kwargs):
        raise NotImplemented()

    # ======== Properties ========
    @hybrid_property
    def properties(self):
        return PropertiesDict(self)

    @properties.setter
    def properties(self, properties):
        """To set each property, _set_property is called, which calls
        __setitem__ which calls setattr(). The final call to setattr
        will pass through any validation defined in a subclass
        property setter.

        """
        for key, val in sanitize(properties).items():
            setattr(self, key, val)

    @hybrid_property
    def props(self):
        """Alias of properties

        """
        return self.properties

    @props.setter
    def props(self, properties):
        """Alias of properties

        """
        self.properties = properties

    @hybrid_property
    def sysan(self):
        """Alias of properties

        """
        return self.system_annotations

    @sysan.setter
    def sysan(self, sysan):
        """Alias of properties

        """
        self.system_annotations = sysan

    def _set_property(self, key, val):
        """Property dict is cloned (to make sure that SQLAlchemy flushes it)
        before setting the key value pair.

        """
        if not self.has_property(key):
            raise KeyError('{} has no property {}'.format(type(self), key))
        self._props = {k: v for k, v in self._props.iteritems()}
        self._props[key] = val

    def _get_property(self, key):
        """If the property is defined in the model but not present on the
        instance, return None, else return the value associated with key.

        """
        if not self.has_property(key):
            raise KeyError('{} has no property {}'.format(type(self), key))
        if key not in self._props:
            return None
        return self._props[key]

    def property_template(self, properties={}):
        """Returns a dictionary of {key: None} templating all of the
        properties defined on the model.

        """
        temp = {k: None for k in self.get_property_list()}
        temp.update(properties)
        return temp

    def __getitem__(self, key):
        """Returns value corresponding to key in _props

        """
        return getattr(self, key)

    def __setitem__(self, key, val):
        """Sets value corresponding to key in _props.  This calls the model's
        hybrid_property setter method in the instance's model class.

        """
        setattr(self, key, val)

    @classmethod
    def get_property_list(cls):
        """Returns a list of hybrid_properties defined on the subclass model

        """
        return [
            attr for attr in dir(cls)
            if attr in cls.__dict__
            and isinstance(cls.__dict__[attr], hybrid_property)
            and getattr(getattr(cls, attr), '_is_pg_property', True)
        ]

    @classmethod
    def has_property(cls, key):
        """Returns boolean if key is a property defined on the subclass model

        """
        return key in cls.get_property_list()

    # ======== Label ========
    @hybrid_property
    def label(self):
        """Custom label on the model

        .. note: This is not the polymorphic identity, see `_type`
        """
        return self.get_label()

    @label.setter
    def label(self, label):
        """Custom setter as an application level ban from changing labels.

        """
        if not isinstance(self.label, Column)\
           and self.get_label() is not None\
           and self.get_label() != label:
            raise AttributeError('Cannot change label from {} to {}'.format(
                self.get_label(), label))

    # ======== System Annotations ========
    @hybrid_property
    def system_annotations(self):
        """Returns a system annotation proxy pointing to _sysan.  Any updates
        to this dict will be proxied to the model's _sysan JSONB
        column.

        """
        return SystemAnnotationDict(self)

    @system_annotations.setter
    def system_annotations(self, sysan):
        """Directly set the model's _sysan column with dict sysan.

        """
        self._sysan = sanitize(sysan)

    def get_name(self):
        """Convenience wrapper for getting class name
        """
        return type(self).__name__

    def get_session(self):
        """Returns the session an object is bound to if bound to a session

        """
        return object_session(self)

    def merge(self, acl=None, system_annotations={}, properties={}):
        """Merge the model's system_annotations and properties.

        .. note: acl will be overwritten, merging acls is not supported
        """
        self.system_annotations.update(system_annotations)
        for key, value in properties.items():
            setattr(self, key, value)
        if acl is not None:
            self.acl = acl

    def _merge_onto_existing(self, old_props, old_sysan):
        # properties
        temp = {}
        temp.update(old_props)
        temp.update(self._props)
        self._props = temp

        # system annotations
        temp = {}
        temp.update(old_sysan)
        temp.update(self._sysan)
        self._sysan = temp

    def _get_clean_session(self, session=None):
        """Create a new session from an objects session using the same
        connection to allow for clean queries against the database

        """
        if not session:
            session = self.get_session()
        Clean = sessionmaker()
        Clean.configure(bind=session.bind)
        return Clean()

    def _validate(self, session=None):
        """Final validation currently only includes checking nonnull
        properties

        """
        for key in getattr(self, '__nonnull_properties__', []):
            assert self.properties[key] is not None, (
                "Null value in key '{}' violates non-null constraint for {}."
            ).format(key, self)

    @classmethod
    def get_pg_properties(cls):
        return cls.__pg_properties__


def create_hybrid_property(name, fset):
    @hybrid_property
    def hybrid_prop(instance):
        # Note: this does not use an 'in' clause or a .get() with a
        # default because that doesn't allow you to use
        # Node.property_key in a filter on a query.
        try:
            return instance._props[name]
        except KeyError:
            return None

    @hybrid_prop.setter
    def hybrid_prop(instance, value):
        validate(fset, value, fset.__pg_types__, fset.__pg_enum__)
        fset(instance, value)
    return hybrid_prop


@event.listens_for(CommonBase, 'mapper_configured', propagate=True)
def create_hybrid_properties(mapper, cls):
    # This dictionary will be a property name to allowed types
    # dictionary.  It will be populated at mapper configuration using
    # all model properties defined with @pg_property
    cls.__pg_properties__ = {}

    for pg_attr in dir(cls):
        if pg_attr in ['properties', 'props', 'system_annotations', 'sysan']:
            continue

        f = getattr(cls, pg_attr)
        if not getattr(f, '__pg_setter__', False):
            continue

        h_prop = create_hybrid_property(pg_attr, f)
        setattr(cls, pg_attr, h_prop)
        cls.__pg_properties__[pg_attr] = f.__pg_types__


class VoidedBaseClass(object):

    @hybrid_property
    def props(self):
        """Alias of properties

        """
        return self.properties

    @props.setter
    def props(self, properties):
        """Alias of properties

        """
        self.properties = properties

    @hybrid_property
    def sysan(self):
        """Alias of properties

        """
        return self.system_annotations

    @sysan.setter
    def sysan(self, sysan):
        """Alias of properties

        """
        self.system_annotations = sysan


VoidedBase = declarative_base(cls=VoidedBaseClass)
ORMBase = declarative_base(cls=CommonBase)


def create_all(engine):
    ORMBase.metadata.create_all(engine)
    VoidedBase.metadata.create_all(engine)
