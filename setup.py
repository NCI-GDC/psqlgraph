from setuptools import setup

from psqlgraph.version import __version__

setup(
    version=__version__,
    name='psqlgraph',
    packages=["psqlgraph"],
    install_requires=[
        'psycopg2-binary',
        'sqlalchemy',
        'xlocal',
        'rstr',
        'requests',
        'six>=1.12.0'
    ]
)
