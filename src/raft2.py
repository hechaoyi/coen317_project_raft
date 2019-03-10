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
    def __init__(self, identity, election_timeout_lower=0.15, election_timeout_higher=0.3, delayed_start=0.0,
                 socketio=None, executor=None):
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
        self.apply_callback = []
        self.majority = 1
        self.random_election_timeout = lambda: random.uniform(election_timeout_lower, election_timeout_higher)
        self.heartbeat_interval = election_timeout_lower / 3
        self.ticking_interval = min(election_timeout_lower / 3, 0.05)
        self.committed_condition = asyncio.Condition()
        self.failure = False

        self.socketio = socketio
        self.executor = executor
        self.loop = asyncio.get_event_loop()
        self.loop.call_later(delayed_start, lambda: self.loop.create_task(self.tick()))
        logger.info(f'{self.id}[{self.state}]: Started')
        self.election_timeout_once = self.random_election_timeout()
        self.loop.create_task(self.event({'type': 'stateChanged', 'from': None, 'to': 'F',
                                          'timer': self.election_timeout_once}))

    def add_peers(self, peer):
        self.peers.extend(peer)
        self.majority = (len(self.peers) + 1) // 2 + 1
        return self

    def bridge_coroutine(self, coro):
        origin, future = self.loop.create_task(coro), Future()
        origin.add_done_callback(lambda _: future.set_result(origin.result()))
        return future.result()

    async def turn_on(self):
        self.failure = False
        return self.current_term, 'on'

    async def turn_off(self):
        self.failure = True
        return self.current_term, 'off'

    async def event(self, event):
        if self.socketio and self.executor:
            await self.loop.run_in_executor(self.executor,
                                            lambda: self.socketio.emit('raftEvent', event))

    async def tick(self):
        if not self.failure:
            logger.debug(f'{self.id}[{self.state}]: Ticking')
            self.loop.call_later(self.ticking_interval, lambda: self.loop.create_task(self.tick()))
            if self.state == 'F':
                if time() - self.last_heartbeat > self.election_timeout_once:
                    logger.info(f'{self.id}[{self.state}]: Heartbeat timed out, converting to candidate')
                    await self.convert_to_candidate()
            elif self.state == 'C':
                if time() - self.last_heartbeat > self.election_timeout_once:
                    logger.info(f'{self.id}[{self.state}]: Election timed out, majority not reached, reelecting')
                    await self.convert_to_candidate()
            elif self.state == 'L':
                if time() - self.last_heartbeat > self.heartbeat_interval:
                    await self.broadcast_entries()

    async def convert_to_candidate(self):
        self.election_timeout_once = self.random_election_timeout()
        await self.event({'type': 'stateChanged', 'from': self.state, 'to': 'C', 'timer': self.election_timeout_once})
        self.state = 'C'
        self.last_heartbeat = time()
        self.current_term += 1
        self.voted_for = self.id
        self.leader = None
        last_log_index = len(self.logs) - 1
        last_log_term = -1 if last_log_index < 0 else self.logs[last_log_index].term
        votes, request_futures = 1, [peer.request_vote(self.current_term, self.id,
                                                       last_log_index, last_log_term)
                                     for peer in self.peers]
        if not request_futures:
            await self.convert_to_leader()
            return
        finished = False
        for future in asyncio.as_completed(request_futures):
            try:
                term, granted = await future
                if finished or self.state != 'C' or await self.check_if_term_updated(term):
                    finished = True
                    continue
                votes += granted
                if votes >= self.majority:
                    await self.convert_to_leader()
                    finished = True
                    continue
            except Exception as e:
                logger.info(f'{self.id}[{self.state}]: RequestVote failed: {e}')

    async def convert_to_leader(self):
        logger.info(f'{self.id}[{self.state}]: Majority reached, converting to leader')
        await self.event({'type': 'stateChanged', 'from': self.state, 'to': 'L'})
        self.state = 'L'
        self.next_indexes = {peer: len(self.logs) for peer in self.peers}
        self.match_indexes = {peer: -1 for peer in self.peers}
        await self.broadcast_entries()

    async def broadcast_entries(self):
        await self.event({'type': 'broadcastingEntries'})
        self.last_heartbeat = time()
        if self.peers:
            await asyncio.wait([self.transmit_entries(peer) for peer in self.peers])
        elif len(self.logs) - 1 > self.commit_index:
            await self.leader_commit(len(self.logs) - 1)

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
                index = sorted(self.match_indexes.values())[1 - self.majority]
                for i in range(index, self.commit_index, -1):
                    if self.logs[i].term == self.current_term:
                        await self.leader_commit(i)
                        break
            else:
                self.next_indexes[peer] = min(self.next_indexes[peer], from_index - 1)
                await self.transmit_entries(peer)
        except Exception as e:
            logger.debug(f'{self.id}[{self.state}]: AppendEntries failed: {e}')

    async def leader_commit(self, index):
        logger.info(f'{self.id}[{self.state}]: Leader\'s commit index move forward to {index}')
        oplogs = [self.logs[j].command for j in range(self.commit_index + 1, index + 1)]
        for callback in self.apply_callback:
            callback(oplogs)
        self.commit_index = index
        async with self.committed_condition:
            self.committed_condition.notify_all()

    async def check_if_term_updated(self, term):
        if term > self.current_term:
            logger.info(f'{self.id}[{self.state}]: Term updated to {term}')
            self.current_term = term
            self.voted_for = None
            if self.state != 'F':
                logger.info(f'{self.id}[{self.state}]: Term updated, converting to follower')
                self.election_timeout_once = self.random_election_timeout()
                await self.event({'type': 'stateChanged', 'from': self.state, 'to': 'F',
                                  'timer': self.election_timeout_once})
                self.state = 'F'
                self.last_heartbeat = time()
                self.leader = None
            return True
        return False

    async def received_request_vote(self, term, candidate_id, last_log_index, last_log_term):
        if not self.failure:
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
                self.election_timeout_once = self.random_election_timeout()
                self.last_heartbeat = time()
                self.voted_for = candidate_id
            logger.info(f'{self.id}[{self.state}]: RequestVote received: {term} - {candidate_id}, granted: {granted}')
            return self.current_term, granted
        else:
            return self.current_term, False

    async def received_append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        if not self.failure:
            await self.check_if_term_updated(term)
            if term < self.current_term:
                return self.current_term, False
            if self.state != 'F':
                logger.info(f'{self.id}[{self.state}]: Current leader discovered, converting to follower')
                self.state = 'F'
            self.election_timeout_once = self.random_election_timeout()
            await self.event({'type': 'heartbeatReceived', 'timer': self.election_timeout_once})
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
                oplogs = [self.logs[j].command for j in range(self.commit_index + 1, index + 1)]
                for callback in self.apply_callback:
                    callback(oplogs)
                self.commit_index = index
            return self.current_term, True
        else:
            return self.current_term, False

    async def received_command(self, cmd, wait=False):
        if not self.failure:
            logger.info(f'{self.id}[{self.state}]: Client command received: {cmd}')
            if self.state == 'L':
                index, term = len(self.logs), self.current_term
                self.logs.append(Log(self.current_term, str(cmd)))
                _ = self.loop.create_task(self.broadcast_entries())
                if not wait:
                    return True, index
                while self.commit_index < index:
                    async with self.committed_condition:
                        await self.committed_condition.wait()
                if self.logs[index].term == term:
                    return True, index
            elif self.leader is not None:
                try:
                    return await self.leader.command(cmd, wait)
                except Exception as e:
                    logger.info(f'{self.id}[{self.state}]: Command failed: {e}')
            return False, -1
        else:
            return False, -1


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

    async def command(self, cmd, wait=False):
        await asyncio.sleep(self.blocking_time())
        return await self.raft.received_command(cmd, wait)

    async def turn_on(self):
        # no need to sleep 
        return await self.raft.turn_on()

    async def turn_off(self):
        # no need to sleep 
        return await self.raft.turn_off()


class RaftRemoteRpcWrapper:
    def __init__(self, raft, loop, executor):
        self.raft_id = raft
        self.loop = loop
        self.executor = executor

    async def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        data = {'term': term, 'candidateId': candidate_id, 'lastLogIndex': last_log_index, 'lastLogTerm': last_log_term}
        func = functools.partial(requests.post, 'http://' + self.raft_id + '/requestVote', data=data, timeout=1)
        result = (await self.loop.run_in_executor(self.executor, func)).json()
        return result.get('term', 0), result.get('voteGranted', False)

    async def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        data = {'term': term, 'leaderId': leader_id,
                'prevLogIndex': prev_log_index, 'prevLogTerm': prev_log_term,
                'entries': json.dumps(entries), 'leaderCommit': leader_commit}
        func = functools.partial(requests.post, 'http://' + self.raft_id + '/appendEntries', data=data, timeout=1)
        result = (await self.loop.run_in_executor(self.executor, func)).json()
        return result.get('term', 0), result.get('success', False)

    async def command(self, cmd, wait=False):
        data = {'command': cmd, 'wait': '1' if wait else '0'}
        func = functools.partial(requests.post, 'http://' + self.raft_id + '/command', data=data, timeout=3)
        result = (await self.loop.run_in_executor(self.executor, func)).json()
        return result.get('success', False), result.get('index', -1)


def make_instances(n):
    res = [Raft(f'R{i}') for i in range(1, n + 1)]
    for i in range(n):
        res[i].add_peers(RaftLocalRpcWrapper(res[j]) for j in range(n) if j != i)
    return res


if __name__ == '__main__':
    async def command(r, c):
        logger.info(f'command done: {await r.received_command(c)}')


    r1, *_ = make_instances(5)
    r1.loop.call_later(1, lambda: r1.loop.create_task(command(r1, 'a')))
    r1.loop.call_later(2, lambda: r1.loop.create_task(command(r1, 'b')))
    r1.loop.call_later(3, lambda: r1.loop.create_task(command(r1, 'c')))
    r1.loop.call_later(4, lambda: r1.loop.create_task(command(r1, 'd')))
    r1.loop.call_later(5, lambda: r1.loop.create_task(command(r1, 'e')))
    r1.loop.run_forever()
