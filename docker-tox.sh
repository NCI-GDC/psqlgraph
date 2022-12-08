#!/usr/bin/bash

/usr/bin/python3.7 -m pip install pipx --user
pipx install poetry 
pipx inject poetry poetry-dynamic-versioning
pipx install tox

tox -r
