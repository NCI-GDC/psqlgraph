[![Build Status](https://magnum.travis-ci.com/NCI-GDC/psqlgraph.svg?token=LApTVTN34FyXpxo5zU44&branch=master)](https://magnum.travis-ci.com/NCI-GDC/psqlgraph)

# Overview

The psqlgraph library is a layer on top of [SQLAlchemy's](http://www.sqlalchemy.org/) ORM layer that attemps to capitalize on the benefits of SQL while utilizing Postgresql's JSONB support for SQL-less flexibility.  Psqlgraph allows you to interact with your data graphically by defining Node and Edge models to maintain flexible many-to-many relationships.

# Usage

## Creating a model

**NOTE: When creating models, all edge classes must be imported BEFORE node classes.** This allows the library to link the edges to the nodes at module load.

### Nodes

Creating a node model is straightforward.  We specify the properties of the node with the `hybrid_property` method.  Each hybrid_property needs a 'getter' and a 'setter'.  The setter is where any custom validation happens.
```python
from psqlgraph import Node, Edge


class Foo(Node):

     # Optional: specify a custom label (defaults to lowercase of class name, e.g. foo)
     #  __label__ = 'foo_node'
     
     # Optional: specify a non-null constraint key list
     #  __nonnull_properties__ = ['key1']

     @hybrid_property
     def key1(self):
         return self._get_property('key1')

     @key1.setter
     def key1(self, value):
         assert isinstance(value, (str, type(None)))  # insert custom validation here!
         self._set_property('key1', value)
```
You can also provide a list of keys that are non-nullable.  This will be checked when the node is flushed to the database (basically whenever you commit a session or query the database).

### Edges

Creating an edge model is similarly simple. Again note that edges must be declared _**BEFORE**_ nodes, this example is out of order for clarity's sake.

```python
class Edge1(Edge):
    __src_class__ = 'Foo'
    __dst_class__ = 'Bar'
    __src_dst_assoc__ = 'foos'
    __dst_src_assoc__ = 'bars'
```

The above edge would join two node classes Foo and Bar.  Edges are direction.  This allows a constitent source->destination relationship.  The namnes of the source and destination classes are specified in `__src_class__` and `__dst_class__` respectively.  When these node classes are instantiated, they will get a `foo._Edge1_out` and `bar._Edge1_in` attributes which are SQLAlchemy relationships specifying the connected edge objects.  You are required to specify the `__src_dst_assoc__` and `__dst_src_assoc__` association attributes as well (though you can set them to None, if so, they will be ignored).  These attributes specify what the [AssociationProxy](http://docs.sqlalchemy.org/en/latest/orm/extensions/associationproxy.html) will be called.  You will then be able to refer directly to the related objects, e.g. `foo.bars` will be a list of `Bar` objects related to your `Foo` object.

## Using your models

### The session

Psqlgraph is basically a glorified session/model factory.  It has a context scope function which is the bread and butter of dealing with the database.  The session scope provides a (SQLAlchemy session)[http://docs.sqlalchemy.org/en/latest/orm/session.html] which is used as following (assuming we have classes `Foo, Bar, Baz` and edges `Edge1, Edge2` between `(Foo, Bar)` and `(Bar, Baz)` respectively:

```python
g = PsqlGraphDriver(host, user, password, database)
with g.session_scope() as session:
     foo = Foo('1')
     bar = Bar('2')
     baz = Baz('3')
     edge1 = Edge1('1', '2')
     edge2 = Edge2('2', '3')
     session.add_all([foo, bar, edge1, edge2])
     # We could call commit() here, but it will be done
     # automatically as we exit the session
     ## session.commit()  
```

As we exit the `with` clause, the session is automatically committed, which means that the changes are flushed to the database and written (at this point they also become visible to other sessions).

### Querying

You can use the driver's `.nodes()` function to produce a polymorphic query over your `Node` types. You can also specify a single node model to use: `.nodes(Foo)`.  The following example follows the one above

```python
with g.session_scope() as session:
     # To get foo.bars to contain bar, and bar.bazes to contian baz:
     foo = g.nodes().ids('1').one()
     bar = g.nodes(Bar).one()
     baz = g.nodes(Baz).ids('3').first()
     
     # To get all Foo nodes connected to Bar nodes, 
     # connected to Baz nodes with the id '3':
     foo = g.nodes(Foo).path('bars.bazes').ids('3').all()
```

# Installation

## Dependencies

Before continuing you must have the following programs installed:

- [Python 2.7+](http://python.org/)
- [Postgresql 9.4](http://www.postgresql.org/download/)

The psqlgraph library requires the following pip dependencies

- [SQLAlchemy](http://www.sqlalchemy.org/)
- [Psycopg2](http://initd.org/psycopg/)

### Project Dependencies

Project dependencies are managed using [PIP](https://pip.readthedocs.org/en/latest/)

### Building Documentation

Documentation is built using [Sphinx](http://sphinx-doc.org/).

```
❯ cd doc
❯ make html
sphinx-build -b html -d build/doctrees   source build/html
Running Sphinx v1.2.3
     ...
dumping object inventory... done
build succeeded.

Build finished. The HTML pages are in build/html.
```

## Test Setup

Running the setup script will:

1. Setup the test postgres tables

```
❯ python psqlgraph/setup_psql_graph.py
Setting up test database
Dropping old test data
Creating tables in test database
```

# Tests

Running the setup script will test the library against a local postgres installation

```
❯  nosetests -v
test_concurrent_node_update_by_id (test_psql_graph.TestPsqlGraphDriver) ... ok
test_edge_lookup_leaves (test_psql_graph.TestPsqlGraphDriver) ... ok
test_edge_merge_and_lookup (test_psql_graph.TestPsqlGraphDriver) ... ok
test_edge_merge_and_lookup_properties (test_psql_graph.TestPsqlGraphDriver) ... ok
test_node_clobber (test_psql_graph.TestPsqlGraphDriver) ... ok
test_node_delete (test_psql_graph.TestPsqlGraphDriver) ... ok
test_node_delete_property_keys (test_psql_graph.TestPsqlGraphDriver) ... ok
test_node_delete_system_annotation_keys (test_psql_graph.TestPsqlGraphDriver) ... ok
Insert a single node and query, compare that the result of the ... ok
Verify that the library handles the case where a user queries for ... ok
Verify that the library handles the case where a user queries for a ... ok
test_node_unique_id_constraint (test_psql_graph.TestPsqlGraphDriver) ... ok
Insert a single node, update it, verify that ... ok
Insert a single node, update it, verify that ... ok
Insert a single node, update it, verify that ... ok
test_null_node_merge (test_psql_graph.TestPsqlGraphDriver) ... ok
test_null_node_void (test_psql_graph.TestPsqlGraphDriver) ... ok

...

```
