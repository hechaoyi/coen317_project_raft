import asyncio
import time
from concurrent.futures.thread import ThreadPoolExecutor
from os import environ
from threading import Thread

import graphene
from flask import Flask, request, jsonify, json
from flask_cors import CORS
from flask_graphql import GraphQLView
from flask_socketio import SocketIO

from kv import KVService
from raft2 import Raft, RaftRemoteRpcWrapper, Log

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app)


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


@app.route('/turn_off', methods=['GET', 'POST'])
def turn_off():
    global failure
    failure = True
    return jsonify({'term': raft.current_term, 'status': 'off'})


@app.route('/turn_on', methods=['GET', 'POST'])
def turn_on():
    global failure
    failure = False
    return jsonify({'term': raft.current_term, 'status': 'on'})


@app.route('/command', methods=['GET', 'POST'])
def command():
    success, index, result = raft.bridge_coroutine(
        raft.received_command(
            request.form['command'], request.form.get('wait') == '1',
        ))
    return jsonify({'success': success, 'index': index, 'result': result})


@app.route('/get', methods=['GET', 'POST'])
def get():
    key = request.args.get('key') or request.form.get('key')
    wait = request.args.get('wait') or request.form.get('wait')
    if not key: return jsonify({'success': False, 'message': '\'key\' not given'})
    success, result = kv.get(key, wait == '1')
    return jsonify({'success': success, 'data': result})


@app.route('/put', methods=['GET', 'POST'])
def put():
    key = request.args.get('key') or request.form.get('key')
    value = request.args.get('value') or request.form.get('value')
    wait = request.args.get('wait') or request.form.get('wait')
    if not key: return jsonify({'success': False, 'message': '\'key\' not given'})
    if not value: return jsonify({'success': False, 'message': '\'value\' not given'})
    return jsonify({'success': kv.put(key, value, wait == '1')})


@app.route('/append', methods=['GET', 'POST'])
def append():
    key = request.args.get('key') or request.form.get('key')
    value = request.args.get('value') or request.form.get('value')
    wait = request.args.get('wait') or request.form.get('wait')
    if not key: return jsonify({'success': False, 'message': '\'key\' not given'})
    if not value: return jsonify({'success': False, 'message': '\'value\' not given'})
    return jsonify({'success': kv.append(key, value, wait == '1')})


@app.route('/inspect', methods=['GET', 'POST'])
def inspect():
    return jsonify({'success': True, 'data': {
        'id': raft.id, 'state': raft.state, 'term': raft.current_term,
        'commitIndex': raft.commit_index, 'logs': raft.logs, 'on/off': not failure
    }})


# GraphQL
class Result(graphene.ObjectType):
    success = graphene.Boolean()
    data = graphene.String()


class Query(graphene.ObjectType):
    get = graphene.Field(Result,
                         key=graphene.String(required=True),
                         wait=graphene.Boolean(default_value=False))
    put = graphene.Boolean(key=graphene.String(required=True),
                           value=graphene.String(required=True),
                           wait=graphene.Boolean(default_value=False))
    append = graphene.Boolean(key=graphene.String(required=True),
                              value=graphene.String(required=True),
                              wait=graphene.Boolean(default_value=False))

    def resolve_get(self, _, key, wait):
        success, result = kv.get(key, wait)
        return Result(success=success, data=result)

    def resolve_put(self, _, key, value, wait):
        return kv.put(key, value, wait)

    def resolve_append(self, _, key, value, wait):
        return kv.append(key, value, wait)


view = GraphQLView.as_view('graphql', schema=graphene.Schema(query=Query), graphiql=True)
app.add_url_rule('/graphql', view_func=view)
# GraphQL


peers, executor = environ['PEERS'], ThreadPoolExecutor(max_workers=20)
raft = Raft(environ['IDENTITY'],
            election_timeout_lower=18, election_timeout_higher=24, delayed_start=10,
            socketio=socketio, executor=executor, delayed_vote_granting=True)
raft.add_peers([RaftRemoteRpcWrapper(peer, raft.loop, executor)
                for peer in (peers.split(',') if peers else [])])
kv = KVService(raft)
failure = False


def asyncio_event_loop_driver():
    while True:
        if failure:
            time.sleep(0.1)
        else:
            raft.loop.run_until_complete(asyncio.sleep(0.1))


Thread(target=asyncio_event_loop_driver, daemon=True).start()

if __name__ == '__main__':
    socketio.run(app, '0.0.0.0', 80)
