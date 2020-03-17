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
        'psycopg2-binary',
        'sqlalchemy',
        'xlocal',
        'rstr',
        'requests',
        'six>=1.12.0'
    ]
)
