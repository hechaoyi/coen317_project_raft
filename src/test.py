import asyncio
import collections
import logging
import random
from time import time

from raft2 import Raft

logger = logging.getLogger(__name__)
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s'))
logger.addHandler(sh)
logger.setLevel(logging.INFO)


class RaftLocalRpcWrapper:
    def __init__(self, raft, reliability=0.1):
        self.raft = raft
        self.raft_id = raft.id
        self.blocking_time = lambda: random.uniform(0.01, reliability)
        self.connected = True

    async def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        await asyncio.sleep(self.blocking_time())
        if not self.connected:
            raise Exception('rpc call dropped')
        return await self.raft.received_request_vote(term, candidate_id, last_log_index, last_log_term)

    async def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        await asyncio.sleep(self.blocking_time())
        if not self.connected:
            raise Exception('rpc call dropped')
        return await self.raft.received_append_entries(
            term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)

    async def command(self, cmd):
        await asyncio.sleep(self.blocking_time())
        if not self.connected:
            raise Exception('rpc call dropped')
        return await self.raft.received_command(cmd)


def make_instances(n):
    asyncio.set_event_loop(asyncio.new_event_loop())
    res = [Raft(f'R{i}') for i in range(1, n + 1)]
    for i in range(n):
        res[i].add_peers(RaftLocalRpcWrapper(res[j]) for j in range(n) if j != i)
    return res


def connect(cluster, raft, onoff=True):
    for peer in raft.peers:
        peer.connected = onoff
    logger.info(f'{raft.id} {"can" if onoff else "cannot"} access the cluster')
    for r in cluster:
        for p in r.peers:
            if p.raft_id == raft.id:
                p.connected = onoff
                logger.info(f'{r.id} {"can" if onoff else "cannot"} access {raft.id}')


def run_for(sec):
    async def wait():
        await asyncio.sleep(sec)

    asyncio.get_event_loop().run_until_complete(wait())


def run_until(func, interval=0.1, timeout=5):
    async def wait():
        while not func() and time() - start_at < timeout:
            await asyncio.sleep(interval)

    start_at = time()
    asyncio.get_event_loop().run_until_complete(wait())
    return func()


def check_one_leader(cluster):
    def check_once():
        leaders = collections.defaultdict(list)
        for raft in cluster:
            if raft.state == 'L':
                leaders[raft.current_term].append(raft)
        if not leaders:
            return False
        assert all(len(l) == 1 for l in leaders.values())
        return leaders[max(leaders.keys())][0]

    leader = run_until(check_once)
    assert leader
    return leader


def check_terms(cluster):
    terms = set(raft.current_term for raft in cluster)
    assert len(terms) == 1
    return terms.pop()


def check_no_leader(cluster):
    def connected(raft):
        return any(peer.connected for peer in raft.peers)

    assert not any(raft.state == 'L' and connected(raft) for raft in cluster)
