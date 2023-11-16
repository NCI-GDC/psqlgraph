import os
import re

from setuptools import setup
from setuptools_scm import Configuration, _do_parse


def get_version() -> str:
    """Create version string based on branch name.

    release branch -> 1.2.3rc4
    develop branch -> 1.2.3a4
    other branch -> 1.2.3.dev4+<branch_name with .>  e.g.: 5.0.2.dev14+feat.dev.2303.downstream.pipeline

    Returns:
        version
    """
    config = Configuration()
    version = _do_parse(config)
    if version.distance == 0:
        return str(version.tag)

    from setuptools_scm.version import guess_next_version

    branch = os.getenv("CI_COMMIT_REF_NAME", version.branch)
    if branch.startswith("release"):
        fmt = "{guessed}rc{distance}"
    elif branch.startswith("develop"):
        fmt = "{guessed}a{distance}"
    else:
        local = re.sub("[^0-9a-zA-Z]+", ".", branch)
        fmt = f"{{guessed}}.dev{{distance}}+{local}"

    return version.format_next_version(guess_next_version, fmt)


setup(
    setuptools_git_versioning={
        "enabled": True,
        "version_callback": get_version,
    },
)
