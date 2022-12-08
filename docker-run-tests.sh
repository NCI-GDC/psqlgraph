/usr/bin/python3.10 -m pip install pipx --user
pipx install poetry 
pipx inject poetry poetry-dynamic-versioning
pipx install tox

tox -rs
