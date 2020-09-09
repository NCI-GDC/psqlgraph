.. psqlgraph documentation master file, created by
   sphinx-quickstart on Mon Dec  8 11:04:21 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

psqlgraph documentation
=======================

.. toctree::
   :maxdepth: 4

:mod:`psqlgraph`
----------------
Postgresql Graph Representation Library

If you're just a detailed explanation of querying, feel free to skip
to the section on :ref:`label-graph-queries`.

If you're just a detailed explanation of how to write nodes, feel free to skip
to the section on :ref:`label-using-the-session`.


================
Creating a model
================

.. note:: When creating models, all **Edge** classes must be imported
   **BEFORE** the **Node** classes. This allows the library to link
   the edges to the nodes at module load.

-----
Nodes
-----

Creating a node model is straightforward.  We specify the properties
of the node with the ``pg_property`` method.  Each pg_property needs a
'setter'.  The setter is where any custom validation happens::

    from psqlgraph import Node, Edge, pg_property

    class Foo(Node):

         # Optional: specify a custom label (defaults to
         # lowercase of class name, e.g. foo)
         __label__ = 'foo_node'

         # Optional: specify a non-null constraint key list
         __nonnull_properties__ = ['key1']

         @pg_property
         def key1(self, value):
             # insert custom validation here!
             self._set_property('key1', value)

         @pg_property(int)
         def key2(self, value):
             # Attempting to do node.key2 = '10' will
             # raise a ValidationError
             self._set_property('key1', value)

         @pg_property(enum=('allowed_value_1', 'allowed_2'))
         def key2(self, value):
             # Attempting to do node.key2 = 'not_allowed' will
             # raise a ValidationError
             self._set_property('key1', value)

You can also provide a list of keys that are non-nullable.  This will
be checked when the node is flushed to the database (basically
whenever you commit a session or query the database).

-----
Edges
-----

Creating an edge model is similarly simple. Again note that edges must
be declared **BEFORE** nodes, this example is out of order for
clarity's sake::

    class Edge1(Edge):
        __src_class__ = 'Foo'
        __dst_class__ = 'Bar'
        __src_dst_assoc__ = 'foos'
        __dst_src_assoc__ = 'bars'

The above edge would join two node classes Foo and Bar.  Edges are
direction.  This allows a constitent source-to-destination
relationship.  The namnes of the source and destination classes are
specified in ``__src_class__`` and ``__dst_class__`` respectively.
When these node classes are instantiated, they will get a
``foo._Edge1_out`` and ``bar._Edge1_in`` attributes which are
SQLAlchemy relationships specifying the connected edge objects.  You
are required to specify the ``__src_dst_assoc__`` and
``__dst_src_assoc__`` association attributes as well (though you can
set them to None, if so, they will be ignored).  These attributes
specify what the `AssociationProxy`_ will be called.  You will then be
able to refer directly to the related objects, e.g. ``foo.bars`` will
be a list of ``Bar`` objects related to your ``Foo`` object.

.. _AssociationProxy: http://docs.sqlalchemy.org/en/latest/orm/extensions/associationproxy.html

=================
Using your models
=================

Basic usage would be something like the following::

    # IMPORTANT: You must import your models module.  This is how they are
    # registered as models with psqlgraph.
    from models import Test, Foo

    g = PsqlGraphDriver(host, user, password, database)
    with g.session_scope() as session:
        # create some nodes
        a = Test('a')
        b = Foo('b')

        # update our nodes
        a.key1 = 'Our first property!'
        a.foos.append(b)

        # b will now be a Foo in the list of a's Foos
        assert b in a.foos

        # actually add the nodes to the session
        session.add(a)
        session.merge(b)
    # ^-- Here we exit the session scope  --^
    #     anything you've added or merged
    #     into the session will now be
    #     flushed to the database.


    # Now our changes above are stored in the database,
    # let's look back at them with a new session.
    with g.session_scope() as session:
        a = g.nodes(Test).props(key1='Our first property!').one()
        b = g.nodes(Foo).ids('b').one()
        assert b in a.foos

        excited = g.nodes(Test).filter(
            Test.key1.astext.endswith('Our first property!')).one()
        assert excited = a

Here we create nodes of types ``Test, Foo`` and add ``b`` to the list
of ``Foo`` objects specified on the class ``Test``.  This is done
behind the scenes with an `AssociationProxy`_ which represents a
many-to-many relationship by creating a row in an edge table that
specifies the source (``a``) and the destination (``b``).  From a
simplified perspect you can interact with association proxies like
``Test.foos`` as if they were python lists.

See sections :ref:`label-using-the-session` and :ref:`label-queries`
for more detailed usage.


.. _AssociationProxy: http://docs.sqlalchemy.org/en/latest/orm/extensions/associationproxy.html
.. _label-using-the-session:

-----------------
Using The Session
-----------------

.. note:: You cannot use psqlgraph without importing your models.  In
          order to use any of your models, you must import them into
          the current runtime.  For example, importing all classes
          from test/models.py will load the nodes and edges as
          subclasses.

Psqlgraph is basically a glorified session/model factory.  It has a
context scope function which is the bread and butter of dealing with
the database.  The session scope provides a `SQLAlchemy session`_ which
is used as following (assuming we have classes ``Foo, Bar, Baz`` and
edges ``Edge1, Edge2`` between ``Foo, Bar`` and ``Bar, Baz``
respectively::

    g = PsqlGraphDriver(host, user, password, database)
    with g.session_scope() as session:
         foo = Foo('1')
         bar = Bar('2')
         baz = Baz('3')
         edge1 = Edge1('1', '2')
         edge2 = Edge2('2', '3')
         session.add_all([foo, bar, edge1, edge2])
         ### We could call commit() here, but it will be done
         ### automatically as we exit the session scope context
         # session.commit()

.. _SQLAlchemy session: http://docs.sqlalchemy.org/en/latest/orm/session.html

As we exit the ``with`` clause, the session is automatically committed,
which means that the changes are flushed to the database and written
(at this point they also become visible to other sessions).

You can create a node by calling the model's constructor::

    node1 = TestNode('id1')

You can update properties in one of many ways, all of them are
equivalent::

    node1.key1 = 'demonstration'
    node1['key1'] = 'demonstration'
    node1.properies['key1'] = 'demonstration'

Similarly, you can update the system annotations directly on the node.
These will not be validated using any `hybrid_property` setter
method that is specified as a ``@pg_property``::

    node.system_annotations['source'] = 'the moon'

.. _hybrid_property: http://docs.sqlalchemy.org/en/latest/orm/extensions/hybrid.html

When you are done updating your node, you can insert it or merge it
into a session::

    with g.session_scope() as session:
        session.insert(node1)

or::

    with g.session_scope() as session:
        session.merge(node1)

the difference being that :func:`insert` will raise an exception if
the node already exists in the database.


.. _label-queries:

--------
Querying
--------

You can use the driver's :py:func:`nodes` function to
produce a polymorphic query over your ``Node`` types. You can also
specify a single node model to use: ``.nodes(Foo)``.  The following
example follows the one above::

    with g.session_scope() as session:
         # To get foo.bars to contain bar,
         # and bar.bazes to contian baz:
         foo = g.nodes().ids('1').one()
         bar = g.nodes(Bar).one()
         baz = g.nodes(Baz).ids('3').first()

         # To get all Foo nodes connected to Bar nodes,
         # connected to Baz nodes with the id '3':
         foo = g.nodes(Foo).path('bars.bazes').ids('3').all()
         assert foo.bars[0].node_id == bar.node_id




:mod:`PsqlGraphDriver`
----------------------

.. autoclass:: psqlgraph.PsqlGraphDriver
   :members:


.. _label-graph-queries:

:mod:`Graph Queries`
--------------------

.. autoclass:: psqlgraph.query.GraphQuery
   :members:

Tests
-----
.. code-block:: bash

    $ python test/setup_test_psqlgraph.py
    $ nosetest -v

Building Documentation
----------------------
.. code-block:: bash

    $ python setup.py install # suggested to install using a virtualenv
    $ cd doc
    $ make latexpdf


Utility Methods
---------------

.. automodule:: psqlgraph.util
   :members: retryable, default_backoff

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
