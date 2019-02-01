from setuptools import setup

setup(
    version='0.0.2',
    name='psqlgraph',
    packages=["psqlgraph"],
    install_requires=[
        'psycopg2==2.7.3.2',
        'sqlalchemy==0.9.9',
        'py2neo==4.2.0',
        'progressbar',
        'avro==1.7.7',
        'xlocal==0.5',
        'requests>=2.5.2, <=2.7.0'
    ]
)
