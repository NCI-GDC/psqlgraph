from setuptools import setup

setup(
    name='psqlgraph',
    packages=["psqlgraph"],
    install_requires=[
        'psycopg2',
        'sqlalchemy',
        'py2neo',
        # 'cdisutils',
        ],
    # dependency_links=[
    # 'git+ssh://git@github.com/NCI-GDC/cdisutils.git#egg=cdisutils',
    # ]
    )
