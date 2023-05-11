[![Codacy Badge](https://api.codacy.com/project/badge/Grade/562dc95026254b1a82b39062322bd845)](https://www.codacy.com/manual/NCI-GDC/psqlgraph?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=NCI-GDC/psqlgraph&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/562dc95026254b1a82b39062322bd845)](https://www.codacy.com/manual/NCI-GDC/psqlgraph?utm_source=github.com&utm_medium=referral&utm_content=NCI-GDC/psqlgraph&utm_campaign=Badge_Coverage)
[![Build Status](https://travis-ci.org/NCI-GDC/psqlgraph.svg?branch=develop)](https://travis-ci.org/NCI-GDC/psqlgraph)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
# Overview

The psqlgraph library is a layer on top of [SQLAlchemy's](http://www.sqlalchemy.org/) ORM layer that attempts to capitalize on the benefits of SQL while utilizing Postgresql's JSONB support for SQL-less flexibility.  Psqlgraph allows you to interact with your data graphically by defining Node and Edge models to maintain flexible many-to-many relationships.


- [Overview](#overview)
- [Usage](#usage)
- [Installation](#installation)
  - [Dependencies](#dependencies)
    - [Project Dependencies](#project-dependencies)
    - [Building Documentation](#building-documentation)
  - [Test Setup](#test-setup)
- [Setup pre-commit hook to check for secrets](#setup-pre-commit-hook-to-check-for-secrets)
- [Contributing](#contributing)
- [Tests](#tests)


# Usage

For usage documentation please see /doc/build/html.

# Installation

## Dependencies

Before continuing you must have the following programs installed:

- [Python 3.6+](http://python.org/)
- [Postgresql 9.4+](http://www.postgresql.org/download/)

The psqlgraph library requires the following pip dependencies

- [SQLAlchemy](http://www.sqlalchemy.org/)
- [Psycopg2](http://initd.org/psycopg/)

### Project Dependencies

Project dependencies are managed using [PIP](https://pip.readthedocs.org/en/latest/)

### `pip install`

```shell
# Mac M2 - assuming you `brew install postgresql@13`
$ export PATH=$PATH:/opt/homebrew/opt/postgresql@13/bin/

$ pip install --no-deps -r dev-requirements.txt
```

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


# Setup pre-commit hook to check for secrets

We use [pre-commit](https://pre-commit.com/) to setup pre-commit hooks for this repo.
We use [detect-secrets](https://github.com/Yelp/detect-secrets) to search for secrets being committed into the repo.

To install the pre-commit hook, run
```
pre-commit install
```

To update the .secrets.baseline file run
```
detect-secrets scan --update .secrets.baseline
```

`.secrets.baseline` contains all the string that were caught by detect-secrets but are not stored in plain text. Audit the baseline to view the secrets .

```
detect-secrets audit .secrets.baseline
```
# Contributing
Read how to contribute [here](https://github.com/NCI-GDC/gdcapi/blob/master/CONTRIBUTING.md)

# Tests

Running the setup script will test the library against a local postgres installation

```
❯  pip install pytest
❯  cd test
❯  py.test -v
```
