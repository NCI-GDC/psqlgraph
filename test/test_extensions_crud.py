from psqlgraph import ext, create_all, PsqlGraphDriver, pg_property
from psqlgraph.base import drop_all

MdaNode, MdaEdge = ext.register_base_class(package_namespace="mda")


class E1(MdaEdge):

    __label__ = "edge_t1_t2"
    __src_class__ = "T1"
    __dst_class__ = "T2"
    __src_dst_assoc__ = "t2s"
    __dst_src_assoc__ = "t1s"


class T2(MdaNode):

    _pg_edges = {}

    @pg_property
    def bar(self, value):
        self._set_property("bar", value)


class T1(MdaNode):

    _pg_edges = {}

    @pg_property
    def foo(self, value):
        self._set_property("foo", value)


def test_create_tables(pg_conf):

    g = PsqlGraphDriver(package_namespace="mda", **pg_conf)
    orm_base = ext.get_orm_base("mda")

    # clean out any existing record
    drop_all(g.engine, base=orm_base)
    create_all(g.engine, base=orm_base)

    with g.session_scope() as s:
        # create sample entries
        t1 = T1(node_id="t1", foo="spez")
        t2 = T2(node_id="t2", bar="spez")
        t1.t2s.append(t2)
        s.add(t1)

    # sample query
    with g.session_scope():
        spezes = g.nodes().props(foo="spez").all()
        assert len(spezes) == 1

        node = spezes[0]
        assert node.label == "t1"
