from concurrent.futures.thread import ThreadPoolExecutor
from os import environ
from threading import Thread

import graphene
from flask import Flask, request, jsonify, json
from flask_graphql import GraphQLView
from flask_socketio import SocketIO

from kv import KVService
from raft2 import Raft, RaftRemoteRpcWrapper, Log

app = Flask(__name__)
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
    term, status = raft.bridge_coroutine(
        raft.turn_off())
    return jsonify({'term': term, 'status': status})

@app.route('/turn_on', methods=['GET', 'POST'])
def turn_on():
    term, status = raft.bridge_coroutine(
        raft.turn_on())
    return jsonify({'term': term, 'status': status})

@app.route('/command', methods=['GET', 'POST'])
def command():
    success, index = raft.bridge_coroutine(
        raft.received_command(
            request.form['command'], request.form.get('wait') == '1',
        ))
    return jsonify({'success': success, 'index': index})


@app.route('/get', methods=['GET', 'POST'])
def get():
    data = kv.get(request.form['key'])
    return jsonify({'success': True, 'data': data})


@app.route('/put', methods=['GET', 'POST'])
def put():
    success = kv.put(request.form['key'], request.form['value'], request.form.get('wait') == '1')
    return jsonify({'success': success})


@app.route('/append', methods=['GET', 'POST'])
def append():
    success = kv.append(request.form['key'], request.form['value'], request.form.get('wait') == '1')
    return jsonify({'success': success})


# GraphQL
class Query(graphene.ObjectType):
    get = graphene.String(key=graphene.String(required=True))
    put = graphene.Boolean(key=graphene.String(required=True),
                           value=graphene.String(required=True),
                           wait=graphene.Boolean(default_value=False))
    append = graphene.Boolean(key=graphene.String(required=True),
                              value=graphene.String(required=True),
                              wait=graphene.Boolean(default_value=False))

    def resolve_get(self, _, key):
        return kv.get(key)

    def resolve_put(self, _, key, value, wait):
        return kv.put(key, value, wait)

    def resolve_append(self, _, key, value, wait):
        return kv.append(key, value, wait)


view = GraphQLView.as_view('graphql', schema=graphene.Schema(query=Query), graphiql=True)
app.add_url_rule('/graphql', view_func=view)
# GraphQL


peers, executor = environ['PEERS'], ThreadPoolExecutor(max_workers=20)
raft = Raft(environ['IDENTITY'], 3, 5, delayed_start=2.0, socketio=socketio, executor=executor)
raft.add_peers([RaftRemoteRpcWrapper(peer, raft.loop, executor)
                for peer in (peers.split(',') if peers else [])])
kv = KVService(raft)
Thread(target=raft.loop.run_forever, daemon=True).start()

if __name__ == '__main__':
    socketio.run(app)
