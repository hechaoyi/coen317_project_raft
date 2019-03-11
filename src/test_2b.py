from random import random

from test import make_instances, n_committed, one, check_one_leader, connect, run_for, run_coro


def test_basic_agree():
    with make_instances(5) as cluster:
        for i in range(3):
            cnt, _ = n_committed(cluster, i)
            assert cnt == 0
            assert one(cluster, str(random()), 5) == i


def test_fail_agree():
    with make_instances(3) as cluster:
        one(cluster, str(random()), 3)

        # follower network disconnection
        leader = check_one_leader(cluster)
        connect(cluster, next(raft for raft in cluster if raft != leader), False)

        # agree despite one disconnected server?
        one(cluster, str(random()), 2)
        one(cluster, str(random()), 2)
        run_for(1)
        one(cluster, str(random()), 2)
        one(cluster, str(random()), 2)

        # re-connect
        connect(cluster, next(raft for raft in cluster if raft != leader), True)

        # agree with full set of servers?
        one(cluster, str(random()), 3)
        run_for(1)
        one(cluster, str(random()), 3)


def test_fail_no_agree():
    with make_instances(5) as cluster:
        one(cluster, str(random()), 5)

        # 3 of 5 followers disconnect
        leader = check_one_leader(cluster)
        idx = cluster.index(leader)
        connect(cluster, cluster[(idx + 1) % 5], False)
        connect(cluster, cluster[(idx + 2) % 5], False)
        connect(cluster, cluster[(idx + 3) % 5], False)

        ok, index, _ = run_coro(leader.received_command(str(random())))
        assert ok
        assert index == 1

        run_for(2)
        cnt, _ = n_committed(cluster, index)
        assert cnt == 0

        # repair
        connect(cluster, cluster[(idx + 1) % 5], True)
        connect(cluster, cluster[(idx + 2) % 5], True)
        connect(cluster, cluster[(idx + 3) % 5], True)

        # the disconnected majority may have chosen a leader from
        # among their own ranks, forgetting index 2.
        run_for(1)
        leader2 = check_one_leader(cluster)
        ok, index, _ = run_coro(leader2.received_command(str(random())))
        assert ok
        assert 1 <= index <= 2

        one(cluster, str(random()), 5)


def test_rejoin():
    with make_instances(3) as cluster:
        one(cluster, str(random()), 3)

        # leader network failure
        leader1 = check_one_leader(cluster)
        connect(cluster, leader1, False)

        # make old leader try to agree on some entries
        run_coro(leader1.received_command(str(random())))
        run_coro(leader1.received_command(str(random())))
        run_coro(leader1.received_command(str(random())))

        # new leader commits, also for index=2
        one(cluster, str(random()), 2)

        # new leader network failure
        leader2 = check_one_leader(cluster)
        connect(cluster, leader2, False)

        # old leader connected again
        connect(cluster, leader1, True)

        one(cluster, str(random()), 2)

        # all together now
        connect(cluster, leader2, True)

        one(cluster, str(random()), 3)


def test_backup():
    with make_instances(5) as cluster:
        one(cluster, str(random()), 5)

        # put leader and one follower in a partition
        leader1 = check_one_leader(cluster)
        idx = cluster.index(leader1)
        connect(cluster, cluster[(idx + 2) % 5], False)
        connect(cluster, cluster[(idx + 3) % 5], False)
        connect(cluster, cluster[(idx + 4) % 5], False)

        # submit lots of commands that won't commit
        for _ in range(50):
            run_coro(leader1.received_command(str(random())))

        run_for(0.5)

        connect(cluster, cluster[(idx + 0) % 5], False)
        connect(cluster, cluster[(idx + 1) % 5], False)

        # allow other partition to recover
        connect(cluster, cluster[(idx + 2) % 5], True)
        connect(cluster, cluster[(idx + 3) % 5], True)
        connect(cluster, cluster[(idx + 4) % 5], True)

        # lots of successful commands to new group.
        for _ in range(50):
            one(cluster, str(random()), 3)

        # now another partitioned leader and one follower
        leader2 = check_one_leader(cluster)
        other = cluster[(idx + 2) % 5]
        if leader2 == other:
            other = cluster[(idx + 3) % 5]
        connect(cluster, other, False)

        # lots more commands that won't commit
        for _ in range(50):
            run_coro(leader2.received_command(str(random())))

        run_for(0.5)

        # bring original leader back to life,
        for raft in cluster:
            connect(cluster, raft, False)
        connect(cluster, cluster[(idx + 0) % 5], True)
        connect(cluster, cluster[(idx + 1) % 5], True)
        connect(cluster, other, True)

        # lots of successful commands to new group.
        for _ in range(50):
            one(cluster, str(random()), 3)

        # now everyone
        for raft in cluster:
            connect(cluster, raft, True)
        one(cluster, str(random()), 5)
