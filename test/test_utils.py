from psqlgraph import util


def test_sanitize():
    props = dict(state="PASSED", versions=["a", "b"])
    sprops = util.sanitize(props)
    assert props["state"] == sprops["state"]
