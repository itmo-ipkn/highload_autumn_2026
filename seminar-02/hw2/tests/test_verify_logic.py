import logic


def test_find_gaps():
    assert logic.find_gaps([1, 2, 3, 6], 1, 6) == [4, 5]
    assert logic.find_gaps(set(), 1, 3) == [1, 2, 3]
    assert logic.find_gaps([1, 2, 3], 1, 3) == []


def test_split_gaps_explained_by_dlt():
    missing = [2, 5, 7]
    explained, unexplained = logic.split_gaps(missing, {2, 7})
    assert explained == [2, 7]
    assert unexplained == [5]
    # a seq in the sink is never counted as a gap even if it is also in the DLT
    explained, unexplained = logic.split_gaps([], {3})
    assert explained == [] and unexplained == []


def test_count_inversions():
    # arrival order 3 then 1: one inversion (a smaller seq written after a larger one)
    assert logic.count_inversions([3, 1]) == 1
    assert logic.count_inversions([1, 2, 3]) == 0
    assert logic.count_inversions([2, 1, 3]) == 1
    assert logic.count_inversions([1, 3, 2]) == 1
    assert logic.count_inversions([3, 2, 1]) == 3


def test_pod_index_from_user():
    assert logic.pod_index_from_user('user_2_05') == 2
    assert logic.pod_index_from_user('alice') is None
