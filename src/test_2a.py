from test import make_instances, check_one_leader, run_for, check_terms, connect, check_no_leader


def test_initial_election():
    cluster = make_instances(3)
    # is a leader elected?
    check_one_leader(cluster)

    # sleep a bit to avoid racing with followers learning of the
    # election, then check that all peers agree on the term.
    run_for(0.05)
    term1 = check_terms(cluster)

    # does the leader+term stay the same if there is no network failure?
    run_for(2)
    term2 = check_terms(cluster)
    assert term1 == term2

    # there should still be a leader.
    check_one_leader(cluster)


def test_reelection():
    cluster = make_instances(3)
    leader1 = check_one_leader(cluster)

    # if the leader disconnects, a new one should be elected.
    connect(cluster, leader1, False)
    run_for(1)
    check_one_leader(cluster)

    # if the old leader rejoins, that shouldn't disturb the new leader.
    connect(cluster, leader1, True)
    run_for(1)
    leader2 = check_one_leader(cluster)

    # if there's no quorum, no leader should be elected.
    connect(cluster, leader2, False)
    connect(cluster, next(raft for raft in cluster if raft != leader2), False)
    run_for(1)
    check_no_leader(cluster)

    # if a quorum arises, it should elect a leader.
    connect(cluster, next(raft for raft in cluster if raft != leader2), True)
    check_one_leader(cluster)

    # re-join of last node shouldn't prevent leader from existing.
    connect(cluster, leader2, True)
    run_for(1)
    check_one_leader(cluster)
