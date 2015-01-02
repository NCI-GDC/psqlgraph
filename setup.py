from setuptools import setup

setup(
    name='psqlgraph',
    packages=["psqlgraph"],
    install_requires=[
        'psycopg2',
        'sqlalchemy',
        'py2neo',
        'pyyaml', 
        'progressbar',
        'avro==1.7.7'
        ],
    )
