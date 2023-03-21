from setuptools import setup

setup(
    use_scm_version={"local_scheme": "dirty-tag", "write_to": "psqlgraph/_version.py",},
    setup_requires=["setuptools_scm<6"],
    name="psqlgraph",
    packages=["psqlgraph"],
    install_requires=["psycopg2", "sqlalchemy~=1.3,<1.4", "xlocal", "rstr",],
)
