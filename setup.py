from setuptools import setup

setup(
    use_scm_version={
        "local_scheme": "dirty-tag",
        "write_to": "psqlgraph/_version.py",
    },
    setup_requires=["setuptools_scm<6"],
    name="psqlgraph",
    packages=["psqlgraph"],
    package_data={
        "psqlgraph": [
            "py.typed",
        ]
    },
    install_requires=[
        "psycopg2~=2.8.5",
        "sqlalchemy~=1.3,<1.4",
        "xlocal~=0.5",
        "rstr~=2.2.6",
        "six~=1.15.0",
    ],
)
