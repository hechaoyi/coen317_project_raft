from concurrent.futures.thread import ThreadPoolExecutor
from os import environ
from threading import Thread

from flask import Flask, request, jsonify, json

from raft2 import Raft, RaftRemoteRpcWrapper, Log

app = Flask(__name__)


@app.route('/')
def hello():
    logs = json.dumps(raft.logs, indent=4)
    if raft.state == 'F':
        return f'Follower following {raft.leader.raft_id} at term {raft.current_term}\n{logs}'
    if raft.state == 'C':
        return f'Candidate at term {raft.current_term}\n{logs}'
    return f'Leader at term {raft.current_term}\n{logs}'


@app.route('/appendEntries', methods=['GET', 'POST'])
def append_entries():
    entries = [Log(e['term'], e['command']) for e in json.loads(request.form['entries'])]
    term, success = raft.bridge_coroutine(
        raft.received_append_entries(
            int(request.form['term']), request.form['leaderId'],
            int(request.form['prevLogIndex']), int(request.form['prevLogTerm']),
            entries, int(request.form['leaderCommit'])
        ))
    return jsonify({'term': term, 'success': success})


@app.route('/requestVote', methods=['GET', 'POST'])
def request_vote():
    term, vote_granted = raft.bridge_coroutine(
        raft.received_request_vote(
            int(request.form['term']), request.form['candidateId'],
            int(request.form['lastLogIndex']), int(request.form['lastLogTerm']),
        ))
    return jsonify({'term': term, 'voteGranted': vote_granted})


@app.route('/command', methods=['GET', 'POST'])
def command():
    success, index = raft.bridge_coroutine(
        raft.received_command(
            request.form['command'], request.form.get('wait') == '1',
        ))
    return jsonify({'success': success, 'index': index})


raft = Raft(environ['IDENTITY'], delayed_start=2.0)
peers, executor = environ['PEERS'], ThreadPoolExecutor(max_workers=20)
raft.add_peers([RaftRemoteRpcWrapper(peer, raft.loop, executor)
                for peer in (peers.split(',') if peers else [])])
Thread(target=raft.loop.run_forever, daemon=True).start()
