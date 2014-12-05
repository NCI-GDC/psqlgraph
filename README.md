[![Build Status](https://magnum.travis-ci.com/NCI-GDC/psqlgraph.svg?token=LApTVTN34FyXpxo5zU44&branch=jsm-dev)](https://magnum.travis-ci.com/NCI-GDC/psqlgraph)

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

Running the setup script will:

1. Test the library against local postgres installation

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
