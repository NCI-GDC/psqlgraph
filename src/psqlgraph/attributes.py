from abc import abstractmethod

from psqlgraph.util import sanitize


class PropertiesDictError(Exception):
    pass


class JsonProperty(dict):
    """Handles unicode to str conversion while retrieving properties"""

    def __setitem__(self, key, value):

        self.set_item(key, value)
        super().__setitem__(key, value)

    @abstractmethod
    def set_item(self, key, value):
        pass


class SystemAnnotationDict(JsonProperty):
    """Transparent wrapper for _sysan so you can update it as
    if it were a dict and the changes get pushed to the sqlalchemy object

    """

    def __init__(self, source):
        self.source = source
        super().__init__(sanitize(source._sysan))

    def update(self, system_annotations=None, **kwargs):

        if system_annotations == self:
            return

        system_annotations = system_annotations or {}
        system_annotations = sanitize(system_annotations)
        temp = sanitize(self.source._sysan)
        temp.update(system_annotations)
        self.source._sysan = temp
        super().update(self.source._sysan)

    def set_item(self, key, val):
        temp = dict(self.source._sysan)
        temp[key] = val
        self.source.system_annotations = temp

    def __delitem__(self, key):
        del self.source._sysan[key]
        self.update()


class PropertiesDict(JsonProperty):
    """Transparent wrapper for _props so you can update it as
    if it were a dict and the changes get pushed to the sqlalchemy object

    """

    def __init__(self, source):
        self.source = source
        super().__init__(source.property_template(source._props))

    def update(self, properties=None, **kwargs):

        if properties == self:
            return

        properties = properties or {}
        properties = sanitize(properties)
        for key, val in properties.items():
            if not self.source.has_property(key):
                raise AttributeError(f"{self.source} has no property {key}")
            setattr(self.source, key, val)
        super().update(self.source._props)

    def set_item(self, key, val):
        setattr(self.source, key, val)

    def __delitem__(self, key):
        raise RuntimeError("You cannot delete ORM properties, only void them.")
