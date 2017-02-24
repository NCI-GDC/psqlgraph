from psqlgraph import Edge, Node, pg_property


class StateMemberOfCity(Edge):

    __src_class__ = 'State'
    __dst_class__ = 'City'
    __src_dst_assoc__ = 'cities'
    __dst_src_assoc__ = 'states'


class City(Node):

    @pg_property
    def key0(self, value):
        self._set_property('key0', value)

    @pg_property
    def key1(self, value):
        self._set_property('key1', value)


class State(Node):

    __label__ = 'state'

    @pg_property
    def key0(self, value):
        self._set_property('key0', value)

    @pg_property
    def key1(self, value):
        self._set_property('key1', value)
