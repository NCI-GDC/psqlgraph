.. psqlgraph documentation master file, created by
   sphinx-quickstart on Mon Dec  8 11:04:21 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

psqlgraph documentation
=========================

.. toctree::
   :maxdepth: 4

:mod:`psqlgraph` -- Postgresql Graph Represetation Library
----------------------------------------------------------


Install
+++++++
.. code-block:: bash

    $ python setup.py install # suggested to install using a virtualenv

Test
++++
.. code-block:: bash

    $ python bin/setup_psqlgraph.py
    $ nosetest -v

Build Documentation
+++++++++++++++++++
.. code-block:: bash

    $ python setup.py install # suggested to install using a virtualenv
    $ cd doc
    $ make latexpdf


Module Methods
++++++++++++++

.. automodule:: psqlgraph
   :members: retryable, default_backoff

Module Schema Validation Bases
++++++++++++++++++++++++++++++

.. autoclass:: PsqlNodeValidator
   :members:

.. autoclass:: PsqlEdgeValidator
   :members:


Module Exceptions
+++++++++++++++++

.. autoclass:: QueryError
   :members:

.. autoclass:: ProgrammingError
   :members:

.. autoclass:: NodeCreationError
   :members:

.. autoclass:: EdgeCreationError
   :members:

PsqlGraphDriver
+++++++++++++++

.. autoclass:: PsqlGraphDriver
   :members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
