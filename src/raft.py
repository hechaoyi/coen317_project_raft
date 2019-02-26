import collections
import random
import threading
import time

from requests_futures.sessions import FuturesSession

STATE_FOLLOWER = 1
STATE_CANDIDATE = 2
STATE_LEADER = 3
TIMEOUT_LOWER = 0.15
TIMEOUT_HIGHER = 0.30


class Raft:
    def __init__(self, identity, peers, logger):
        self.state = STATE_FOLLOWER
        self.last_heartbeat = time.time()
        self.current_term = 0
        self.voted_for = None

        self.session = None
        self.futures = None

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
                        result = future.result().json()
                        if self.check_if_term_updated(result.get('term', 0)):
                            break
                        votes += result.get('voteGranted', False)
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
                        result = future.result().json()
                        if self.check_if_term_updated(result.get('term', 0)):
                            break
                    else:
                        self.futures.append(future)
                else:
                    if time.time() - self.last_heartbeat > TIMEOUT_LOWER / 3:
                        self.futures += [self.session.post('http://' + peer + '/appendEntries',
                                                           data={'term': self.current_term, 'leaderId': self.identity},
                                                           timeout=0.03)
                                         for peer in self.peers]
                        self.last_heartbeat = time.time()

    def convert_to_candidate(self):
        self.state = STATE_CANDIDATE
        self.last_heartbeat = time.time()
        self.current_term += 1
        self.voted_for = self.identity
        if self.session is not None:
            self.session.close()
        self.session = FuturesSession()
        self.futures = [self.session.post('http://' + peer + '/requestVote',
                                          data={'term': self.current_term, 'candidateId': self.identity},
                                          timeout=0.03)
                        for peer in self.peers]

    def convert_to_leader(self):
        self.state = STATE_LEADER
        self.session.close()
        self.session = FuturesSession()
        self.futures = collections.deque()

    def check_if_term_updated(self, term):
        if term > self.current_term:
            self.logger.info('Term updated to %d', term)
            self.last_heartbeat = time.time()
            self.current_term = term
            self.voted_for = None
            if self.state != STATE_FOLLOWER:
                self.logger.info('Term updated, converting to follower')
                self.state = STATE_FOLLOWER
                if self.session is not None:
                    self.session.close()
                    self.session = self.futures = None
            return True
        return False

    def received_request_vote(self, term, candidate_id):
        self.logger.info('RequestVote RPC received')
        with self.lock:
            self.check_if_term_updated(term)
            if term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
                self.voted_for = candidate_id
                return self.current_term, True
            return self.current_term, False

    def received_append_entries(self, term, leader_id):
        self.logger.info('AppendEntries RPC received')
        with self.lock:
            self.check_if_term_updated(term)
            if term == self.current_term and (self.voted_for is None or self.voted_for == leader_id):
                self.last_heartbeat = time.time()
                return self.current_term, True
            return self.current_term, False
