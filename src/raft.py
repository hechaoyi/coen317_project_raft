import collections
import random
import threading
import time

import requests
from flask import json
from requests_futures.sessions import FuturesSession

STATE_FOLLOWER = 1
STATE_CANDIDATE = 2
STATE_LEADER = 3
TIMEOUT_LOWER = 1.5
TIMEOUT_HIGHER = 3.0

Log = collections.namedtuple('Log', 'term command')


class Raft:
    def __init__(self, identity, peers, logger):
        self.state = STATE_FOLLOWER
        self.last_heartbeat = time.time()
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.logs = []

        self.session = None
        self.futures = None
        self.next_indexes = None

        self.identity = identity
        self.peers = list(peers)
        self.majority = (len(self.peers) + 1) // 2 + 1
        self.lock = threading.RLock()
        self.logger = logger
        self.logger.info('Started as follower')

    def tick(self):
        with self.lock:
            if self.state == STATE_FOLLOWER:
                if time.time() - self.last_heartbeat > random.uniform(TIMEOUT_LOWER, TIMEOUT_HIGHER):
                    self.logger.info('Election timed out, no heartbeat received, converting to candidate')
                    self.convert_to_candidate()
                    self.tick()
            elif self.state == STATE_CANDIDATE:
                votes = 1
                for future in self.futures:
                    if future.done():
                        try:
                            result = future.result().json()
                            if self.check_if_term_updated(result.get('term', 0)):
                                break
                            votes += result.get('voteGranted', False)
                        except Exception as e:
                            self.logger.info('RequestVote failed: %s', e)
                else:
                    if votes >= self.majority:
                        self.logger.info('Majority reached, converting to leader')
                        self.convert_to_leader()
                        self.tick()
                    elif time.time() - self.last_heartbeat > random.uniform(TIMEOUT_LOWER, TIMEOUT_HIGHER):
                        self.logger.info('Election timed out, majority not reached, reelecting')
                        self.convert_to_candidate()
                        self.tick()
                    else:
                        self.logger.info('Majority not reached, waiting responses')
            elif self.state == STATE_LEADER:
                for _ in range(len(self.futures)):
                    future = self.futures.popleft()
                    if future.done():
                        try:
                            result = future.result().json()
                            if self.check_if_term_updated(result.get('term', 0)):
                                break
                            if result.get('success', False):
                                self.next_indexes[future.peer] = max(
                                    self.next_indexes[future.peer], future.next_index)
                            else:
                                self.next_indexes[future.peer] = min(
                                    self.next_indexes[future.peer], future.from_index - 1)
                                self.transmit_entries(future.peer)
                        except Exception as e:
                            self.logger.info('AppendEntries failed: %s', e)
                    else:
                        self.futures.append(future)
                else:
                    # TODO commit_index
                    if time.time() - self.last_heartbeat > TIMEOUT_LOWER / 3:
                        self.broadcast_entries()

    def convert_to_candidate(self):
        self.state = STATE_CANDIDATE
        self.last_heartbeat = time.time()
        self.current_term += 1
        self.voted_for = self.identity
        self.leader_id = None
        if self.session is not None:
            self.session.close()
        self.session = FuturesSession()
        last_log_index = len(self.logs) - 1
        last_log_term = -1 if last_log_index < 0 else self.logs[last_log_index].term
        self.futures = [self.session.post('http://' + peer + '/requestVote',
                                          data={
                                              'term': self.current_term, 'candidateId': self.identity,
                                              'lastLogIndex': last_log_index, 'lastLogTerm': last_log_term
                                          },
                                          timeout=0.03)
                        for peer in self.peers]

    def convert_to_leader(self):
        self.state = STATE_LEADER
        self.leader_id = self.identity
        self.session.close()
        self.session = FuturesSession()
        self.futures = collections.deque()
        self.next_indexes = {peer: len(self.logs) for peer in self.peers}

    def convert_to_follower(self):
        self.state = STATE_FOLLOWER
        self.leader_id = None
        if self.session is not None:
            self.session.close()
            self.session = self.futures = None

    def check_if_term_updated(self, term):
        if term > self.current_term:
            self.logger.info('Term updated to %d', term)
            self.last_heartbeat = time.time()
            self.current_term = term
            self.voted_for = None
            if self.state != STATE_FOLLOWER:
                self.logger.info('Term updated, converting to follower')
                self.convert_to_follower()
            return True
        return False

    def broadcast_entries(self):
        for peer in self.peers:
            self.transmit_entries(peer)
        self.last_heartbeat = time.time()

    def transmit_entries(self, peer):
        prev_log_index = self.next_indexes[peer] - 1
        prev_log_term = -1 if prev_log_index < 0 else self.logs[prev_log_index].term
        entries = self.logs[self.next_indexes[peer]:]
        future = self.session.post('http://' + peer + '/appendEntries',
                                   data={
                                       'term': self.current_term, 'leaderId': self.identity,
                                       'prevLogIndex': prev_log_index, 'prevLogTerm': prev_log_term,
                                       'entries': json.dumps(entries)
                                   },
                                   timeout=0.03)
        future.peer = peer
        future.from_index = self.next_indexes[peer]
        future.next_index = len(self.logs)
        self.futures.append(future)

    def received_request_vote(self, term, candidate_id, last_log_index, last_log_term):
        with self.lock:
            self.check_if_term_updated(term)
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
                    self.voted_for = candidate_id
                    granted = True
        self.logger.info('RequestVote RPC received: %d - %s, granted: %s', term, candidate_id, granted)
        return self.current_term, granted

    def received_append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries):
        if entries:
            self.logger.info('AppendEntries RPC received: %s', entries)
        with self.lock:
            self.check_if_term_updated(term)
            if term < self.current_term:
                return self.current_term, False
            if self.state != STATE_FOLLOWER:
                self.logger.info('Current leader discovered, converting to follower')
                self.convert_to_follower()
            self.leader_id = leader_id
            self.last_heartbeat = time.time()

            if prev_log_index >= len(self.logs) or \
                    (prev_log_index >= 0 and self.logs[prev_log_index].term != prev_log_term):
                return self.current_term, False
            if entries:
                entries = [Log(e['term'], e['command']) for e in entries]
                if prev_log_index == len(self.logs) - 1:
                    self.logs += entries
                elif self.logs[prev_log_index + 1:prev_log_index + 1 + len(entries)] != entries:
                    self.logger.info('Existing entry conflicts with leader\'s, deleting')
                    self.logs[prev_log_index + 1:] = entries
                # TODO commit_index
            return self.current_term, True

    def received_command(self, command):
        self.logger.info('Client command received: %s', command)
        with self.lock:
            if self.state == STATE_LEADER:
                self.logs.append(Log(self.current_term, command))
                self.broadcast_entries()
                return True  # TODO wait
            elif self.leader_id is not None:
                result = requests.post('http://' + self.leader_id + '/command', data={'command': command})
                return result.json().get('success', False)
            else:
                return False
