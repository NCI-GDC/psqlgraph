from setuptools import setup

setup(
    version='0.0.2',
    name='psqlgraph',
    packages=[
        "psqlgraph",
        "psqlgraph.copier",
    ],
    install_requires=[
        'pytest==2.7.2',
        'psycopg2==2.5.4',
        'sqlalchemy==0.9.9',
        'py2neo==2.0.1',
        'progressbar',
        'avro==1.7.7',
        'xlocal==0.5',
        'requests>=2.5.2, <=2.6.0'
    ]
)
