import threading
import time
from os import environ

from flask import Flask, jsonify, request, json

from raft import Raft

app = Flask(__name__)


@app.route('/')
def hello():
    logs = json.dumps(raft.logs, indent=4)
    if raft.state == 1:
        return 'Follower following {} at term {}\n{}'.format(raft.leader_id, raft.current_term, logs)
    if raft.state == 2:
        return 'Candidate at term {}\n{}'.format(raft.current_term, logs)
    return 'Leader at term {}\n{}'.format(raft.current_term, logs)


@app.route('/command', methods=['GET', 'POST'])
def command():
    success = raft.received_command(
        request.form['command'])
    return jsonify(success=success)


@app.route('/appendEntries', methods=['GET', 'POST'])
def append_entries():
    term, success = raft.received_append_entries(
        int(request.form['term']), request.form['leaderId'],
        int(request.form['prevLogIndex']), int(request.form['prevLogTerm']),
        json.loads(request.form['entries']), int(request.form['leaderCommit']))
    return jsonify({'term': term, 'success': success})


@app.route('/requestVote', methods=['GET', 'POST'])
def request_vote():
    term, vote_granted = raft.received_request_vote(
        int(request.form['term']), request.form['candidateId'],
        int(request.form['lastLogIndex']), int(request.form['lastLogTerm']))
    return jsonify({'term': term, 'voteGranted': vote_granted})


peers = environ['PEERS']
raft = Raft(environ['IDENTITY'], peers.split(',') if peers else [], app.logger)


def event_loop():
    while True:
        try:
            raft.tick()
        except Exception as e:
            app.logger.error('event loop error: %s', e, exc_info=0)
        time.sleep(1)
        # time.sleep(0.1)


threading.Thread(target=event_loop, daemon=True).start()
