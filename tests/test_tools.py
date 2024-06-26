from moriarty.tools import sample_as_weights


def test_sample_as_weights():
    assert sample_as_weights([1, 2, 3]) in [1, 2, 3]
