import asyncio
import collections
import functools
import logging
import random
from concurrent.futures import Future
from time import time

import requests
import simplejson as json

logger = logging.getLogger(__name__)
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s'))
logger.addHandler(sh)
logger.setLevel(logging.INFO)
Log = collections.namedtuple('Log', 'term command')


class Raft:
    def __init__(self, identity, election_timeout_lower=0.15, election_timeout_higher=0.3, delayed_start=0.0):
        self.state = 'F'
        self.last_heartbeat = time()
        self.current_term = 0
        self.voted_for = None
        self.leader = None
        self.logs = []
        self.commit_index = -1
        self.next_indexes = None
        self.match_indexes = None

        # Constants
        self.id = identity
        self.peers = []
        self.majority = 1
        self.random_election_timeout = lambda: random.uniform(election_timeout_lower, election_timeout_higher)
        self.tick_interval = election_timeout_lower / 3
        self.committed_condition = asyncio.Condition()

        self.loop = asyncio.get_event_loop()
        self.loop.call_later(delayed_start, lambda: self.loop.create_task(self.tick()))
        logger.info(f'{self.id}[{self.state}]: Started')

    def add_peers(self, peer):
        self.peers.extend(peer)
        self.majority = (len(self.peers) + 1) // 2 + 1
        return self

    def bridge_coroutine(self, coro):
        origin, future = self.loop.create_task(coro), Future()
        origin.add_done_callback(lambda _: future.set_result(origin.result()))
        return future.result()

    async def tick(self):
        logger.debug(f'{self.id}[{self.state}]: Ticking')
        self.loop.call_later(self.tick_interval, lambda: self.loop.create_task(self.tick()))
        if self.state == 'F':
            if time() - self.last_heartbeat > self.random_election_timeout():
                logger.info(f'{self.id}[{self.state}]: Heartbeat timed out, converting to candidate')
                await self.convert_to_candidate()
        elif self.state == 'C':
            if time() - self.last_heartbeat > self.random_election_timeout():
                logger.info(f'{self.id}[{self.state}]: Election timed out, majority not reached, reelecting')
                await self.convert_to_candidate()
        elif self.state == 'L':
            if time() - self.last_heartbeat > self.tick_interval:
                await self.broadcast_entries()

    async def convert_to_candidate(self):
        self.state = 'C'
        self.last_heartbeat = time()
        self.current_term += 1
        self.voted_for = self.id
        self.leader = None
        last_log_index = len(self.logs) - 1
        last_log_term = -1 if last_log_index < 0 else self.logs[last_log_index].term
        try:
            votes, request_futures = 1, [peer.request_vote(self.current_term, self.id,
                                                           last_log_index, last_log_term)
                                         for peer in self.peers]
            finished = False
            for future in asyncio.as_completed(request_futures):
                try:
                    term, granted = await future
                    if finished or self.state != 'C' or await self.check_if_term_updated(term):
                        finished = True
                        continue
                    votes += granted
                    if votes >= self.majority:
                        logger.info(f'{self.id}[{self.state}]: Majority reached, converting to leader')
                        self.state = 'L'
                        self.next_indexes = {peer: len(self.logs) for peer in self.peers}
                        self.match_indexes = {peer: -1 for peer in self.peers}
                        await self.broadcast_entries()
                        finished = True
                        continue
                except Exception as e:
                    logger.info(f'{self.id}[{self.state}]: RequestVote failed: {e}')
        except asyncio.TimeoutError as e:
            logger.info(f'{self.id}[{self.state}]: RequestVotes failed: {e}')

    async def broadcast_entries(self):
        self.last_heartbeat = time()
        await asyncio.wait([self.transmit_entries(peer) for peer in self.peers])

    async def transmit_entries(self, peer):
        prev_log_index = self.next_indexes[peer] - 1
        prev_log_term = -1 if prev_log_index < 0 else self.logs[prev_log_index].term
        entries = self.logs[self.next_indexes[peer]:]
        from_index = self.next_indexes[peer]
        next_index = len(self.logs)
        try:
            term, success = await peer.append_entries(self.current_term, self.id,
                                                      prev_log_index, prev_log_term,
                                                      entries, self.commit_index)
            if self.state != 'L' or await self.check_if_term_updated(term):
                return
            if success:
                self.next_indexes[peer] = max(self.next_indexes[peer], next_index)
                self.match_indexes[peer] = max(self.match_indexes[peer], next_index - 1)
                index = sorted(self.match_indexes.values())[-self.majority]
                for i in range(index, self.commit_index, -1):
                    if self.logs[i].term == self.current_term:
                        logger.info(f'{self.id}[{self.state}]: Leader\'s commit index move forward to {i}')
                        self.commit_index = i
                        # TODO apply to the state machine
                        async with self.committed_condition:
                            self.committed_condition.notify_all()
                        break
            else:
                self.next_indexes[peer] = min(self.next_indexes[peer], from_index - 1)
                await self.transmit_entries(peer)
        except Exception as e:
            logger.info(f'{self.id}[{self.state}]: AppendEntries failed: {e}')

    async def check_if_term_updated(self, term):
        if term > self.current_term:
            logger.info(f'{self.id}[{self.state}]: Term updated to {term}')
            self.current_term = term
            self.voted_for = None
            if self.state != 'F':
                logger.info(f'{self.id}[{self.state}]: Term updated, converting to follower')
                self.state = 'F'
                self.last_heartbeat = time()
                self.leader = None
            return True
        return False

    async def received_request_vote(self, term, candidate_id, last_log_index, last_log_term):
        await self.check_if_term_updated(term)
        if term < self.current_term:
            granted = False
        elif not (self.voted_for is None or self.voted_for == candidate_id):
            granted = False
        else:
            local_last_log_index = len(self.logs) - 1
            local_last_log_term = -1 if local_last_log_index < 0 else self.logs[local_last_log_index].term
            if last_log_term < local_last_log_term or \
                    (last_log_term == local_last_log_term and last_log_index < local_last_log_index):
                granted = False
            else:
                granted = True
        if granted:
            self.last_heartbeat = time()
            self.voted_for = candidate_id
        logger.info(f'{self.id}[{self.state}]: RequestVote received: {term} - {candidate_id}, granted: {granted}')
        return self.current_term, granted

    async def received_append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        await self.check_if_term_updated(term)
        if term < self.current_term:
            return self.current_term, False
        if self.state != 'F':
            logger.info(f'{self.id}[{self.state}]: Current leader discovered, converting to follower')
            self.state = 'F'
        self.last_heartbeat = time()
        self.leader = next(peer for peer in self.peers if peer.raft_id == leader_id)

        if prev_log_index >= len(self.logs) or \
                (prev_log_index >= 0 and self.logs[prev_log_index].term != prev_log_term):
            return self.current_term, False
        if entries:
            logger.info(f'{self.id}[{self.state}]: AppendEntries received: {entries}')
            if prev_log_index == len(self.logs) - 1:
                self.logs += entries
            elif self.logs[prev_log_index + 1:prev_log_index + 1 + len(entries)] != entries:
                logger.info(f'{self.id}[{self.state}]: Existing entry conflicts with leader\'s, deleting')
                self.logs[prev_log_index + 1:] = entries
        index = min(leader_commit, len(self.logs) - 1)
        if index > self.commit_index:
            logger.info(f'{self.id}[{self.state}]: Follower\'s commit index move forward to {index}')
            self.commit_index = index
            # TODO apply to the state machine
        return self.current_term, True

    async def received_command(self, cmd):
        logger.info(f'{self.id}[{self.state}]: Client command received: {cmd}')
        if self.state == 'L':
            index, term = len(self.logs), self.current_term
            self.logs.append(Log(self.current_term, cmd))
            _ = self.loop.create_task(self.broadcast_entries())
            while self.commit_index < index:
                async with self.committed_condition:
                    await self.committed_condition.wait()
            return self.current_term == term
        elif self.leader is not None:
            return await self.leader.command(cmd)
        else:
            return False


class RaftLocalRpcWrapper:
    def __init__(self, raft, reliability=0.1):
        self.raft = raft
        self.raft_id = raft.id
        self.blocking_time = lambda: random.uniform(0.01, reliability)

    async def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        await asyncio.sleep(self.blocking_time())
        return await self.raft.received_request_vote(term, candidate_id, last_log_index, last_log_term)

    async def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        await asyncio.sleep(self.blocking_time())
        return await self.raft.received_append_entries(
            term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)

    async def command(self, cmd):
        await asyncio.sleep(self.blocking_time())
        return await self.raft.received_command(cmd)


class RaftRemoteRpcWrapper:
    def __init__(self, raft, loop, executor):
        self.raft_id = raft
        self.loop = loop
        self.executor = executor

    async def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        data = {'term': term, 'candidateId': candidate_id, 'lastLogIndex': last_log_index, 'lastLogTerm': last_log_term}
        func = functools.partial(requests.post, 'http://' + self.raft_id + '/requestVote', data=data, timeout=0.1)
        result = (await self.loop.run_in_executor(self.executor, func)).json()
        return result.get('term', 0), result.get('voteGranted', False)

    async def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        data = {'term': term, 'leaderId': leader_id,
                'prevLogIndex': prev_log_index, 'prevLogTerm': prev_log_term,
                'entries': json.dumps(entries), 'leaderCommit': leader_commit}
        func = functools.partial(requests.post, 'http://' + self.raft_id + '/appendEntries', data=data, timeout=0.1)
        result = (await self.loop.run_in_executor(self.executor, func)).json()
        return result.get('term', 0), result.get('success', False)

    async def command(self, cmd):
        func = functools.partial(requests.post,
                                 'http://' + self.raft_id + '/command', data={'command': cmd}, timeout=3)
        result = (await self.loop.run_in_executor(self.executor, func)).json()
        return result.get('success', False)


def make_instances(n):
    res = [Raft(f'R{i}') for i in range(1, n + 1)]
    for i in range(n):
        res[i].add_peers(RaftLocalRpcWrapper(res[j]) for j in range(n) if j != i)
    return res


if __name__ == '__main__':
    async def command(r, c):
        await r.received_command(c)
        logger.info(f'command done: {c}')


    r1, *_ = make_instances(5)
    r1.loop.call_later(1, lambda: r1.loop.create_task(command(r1, 'a')))
    r1.loop.call_later(2, lambda: r1.loop.create_task(command(r1, 'b')))
    r1.loop.call_later(3, lambda: r1.loop.create_task(command(r1, 'c')))
    r1.loop.call_later(4, lambda: r1.loop.create_task(command(r1, 'd')))
    r1.loop.call_later(5, lambda: r1.loop.create_task(command(r1, 'e')))
    r1.loop.run_forever()
