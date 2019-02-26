import threading
import time
from os import environ

import requests
from flask import Flask, jsonify, request

from raft import Raft

app = Flask(__name__)


@app.route('/')
def hello():
    app.logger.info('requests: %s', requests.get('http://' + peers + '/appendEntries', timeout=0.1))
    return 'Hello World'


@app.route('/appendEntries', methods=['GET', 'POST'])
def append_entries():
    term, success = raft.received_append_entries(
        int(request.form.get('term', '0')), request.form.get('leaderId'))
    return jsonify({'term': term, 'success': success})


@app.route('/requestVote', methods=['GET', 'POST'])
def request_vote():
    term, vote_granted = raft.received_request_vote(
        int(request.form.get('term', '0')), request.form.get('candidateId'))
    return jsonify({'term': term, 'voteGranted': vote_granted})


peers = environ['PEERS']
raft = Raft(environ['IDENTITY'], peers.split(',') if peers else [], app.logger)


def event_loop():
    while True:
        try:
            raft.tick()
        except Exception as e:
            app.logger.error('event loop error: %s', e)
        time.sleep(0.03)


threading.Thread(target=event_loop, daemon=True).start()
