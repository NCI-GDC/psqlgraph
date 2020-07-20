from setuptools import setup

setup(
    use_scm_version={
        'local_scheme': 'dirty-tag',
        'write_to': 'psqlgraph/_version.py',
    },
    setup_requires=['setuptools_scm'],
    name='psqlgraph',
    packages=["psqlgraph"],
    install_requires=[
        'psycopg2~=2.8.5',
        'sqlalchemy~=1.3',
        'xlocal~=0.5',
        'rstr~=2.2.6',
        'six~=1.15.0',
    ]
)
