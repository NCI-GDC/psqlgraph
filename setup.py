from setuptools import setup

setup(
    version='1.2.2',
    name='psqlgraph',
    packages=["psqlgraph"],
    install_requires=[
        'psycopg2-binary',
        'sqlalchemy',
        'progressbar',
        'avro',
        'xlocal',
        'rstr',
        'requests'
    ]
)
