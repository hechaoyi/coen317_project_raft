import asyncio
import collections
import logging
import random
from contextlib import contextmanager
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
            raise Exception(f'rpc call dropped for {self.raft_id}')
        return await self.raft.received_request_vote(term, candidate_id, last_log_index, last_log_term)

    async def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        await asyncio.sleep(self.blocking_time())
        if not self.connected:
            raise Exception(f'rpc call dropped for {self.raft_id}')
        return await self.raft.received_append_entries(
            term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)

    async def command(self, cmd, wait=False):
        await asyncio.sleep(self.blocking_time())
        if not self.connected:
            raise Exception(f'rpc call dropped for {self.raft_id}')
        return await self.raft.received_command(cmd, wait)


@contextmanager
def make_instances(n):
    asyncio.set_event_loop(asyncio.new_event_loop())
    res = [Raft(f'R{i}') for i in range(1, n + 1)]
    for i in range(n):
        res[i].add_peers(RaftLocalRpcWrapper(res[j]) for j in range(n) if j != i)
    try:
        yield res
    finally:
        for task in asyncio.all_tasks(asyncio.get_event_loop()):
            task.cancel()
        asyncio.get_event_loop().stop()
        asyncio.get_event_loop().run_forever()


def connect(cluster, raft, onoff=True):
    setattr(raft, 'connected', onoff)
    for peer in raft.peers:
        s = onoff and connected(peer.raft)
        if peer.connected != s:
            peer.connected = s
            logger.info(f'{raft.id} {"can" if s else "cannot"} access {peer.raft_id}')
    for r in cluster:
        for p in r.peers:
            if p.raft_id == raft.id:
                s = onoff and connected(r)
                if p.connected != s:
                    p.connected = s
                    logger.info(f'{r.id} {"can" if s else "cannot"} access {raft.id}')


def connected(raft):
    return not hasattr(raft, 'connected') or getattr(raft, 'connected')


def run_for(sec):
    asyncio.get_event_loop().run_until_complete(asyncio.sleep(sec))


def run_until(func, interval=0.1, timeout=5):
    async def wait():
        while time() - start_at < timeout:
            res = func()
            if res is not None: return res
            await asyncio.sleep(interval)

    start_at = time()
    return asyncio.get_event_loop().run_until_complete(wait())


def run_coro(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def run_coro_until(coro, interval=0.1, timeout=5):
    async def wait():
        while time() - start_at < timeout:
            res = await coro()
            if res is not None: return res
            await asyncio.sleep(interval)

    start_at = time()
    return asyncio.get_event_loop().run_until_complete(wait())


def check_one_leader(cluster):
    def check_once():
        leaders = collections.defaultdict(list)
        for raft in cluster:
            if raft.state == 'L':
                leaders[raft.current_term].append(raft)
        if not leaders:
            return None
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
    assert not any(raft.state == 'L' and connected(raft) for raft in cluster)


def n_committed(cluster, index):
    logs = [raft.logs[index].command for raft in cluster if raft.commit_index >= index]
    assert len(set(logs)) <= 1
    return len(logs), (logs[0] if logs else None)


def one(cluster, command, expected_count):
    async def req():
        index = -1
        # try all the servers, maybe one is the leader.
        for _ in range(len(cluster)):
            raft, start[0] = cluster[start[0] % len(cluster)], start[0] + 1
            if not connected(raft):
                continue
            success, index, _ = await raft.received_command(command)
            if success:
                break
        if index > -1:
            # somebody claimed to be the leader and to have
            # submitted our command; wait a while for agreement.
            start_at = time()
            while time() - start_at < 2:
                cnt, cmd = n_committed(cluster, index)
                if cnt >= expected_count and cmd == command:
                    # committed and it was the command we submitted.
                    return index
                await asyncio.sleep(0.02)

    start = [0]
    idx = run_coro_until(req)
    assert idx is not None and idx >= 0, f'{idx}'
    return idx
