from setuptools import setup

setup(
    version='2.0.0',
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
