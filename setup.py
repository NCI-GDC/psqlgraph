from setuptools import setup

setup(
    name='psqlgraph',
    packages=["psqlgraph"],
    install_requires=[
        'psycopg2==2.5.4',
        'sqlalchemy==0.9.8',
        'py2neo==2.0.1',
        'progressbar',
        'avro==1.7.7',
        'xlocal==0.5'
    ],
)
