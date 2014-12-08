.. psqlgraph documentation master file, created by
   sphinx-quickstart on Mon Dec  8 11:04:21 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to psqlgraph's documentation!
=====================================

Contents:

.. toctree::
   :maxdepth: 2

:mod:`psqlgraph` -- Postgresql Graph Represetation Library
----------------------------------------------------------


Install
+++++++++
.. code-block:: bash

    $ python setup.py install

Install
+++++++++
.. code-block:: bash

    $ python bin/setup_psqlgraph.py
    $ nosetest -v

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
