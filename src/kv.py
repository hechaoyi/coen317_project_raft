class KVService:
    def __init__(self, raft):
        self.mapping = {}
        self.raft = raft
        raft.apply_callback.append(self.apply_oplog)

    def get(self, key):
        return self.mapping.get(key)

    def put(self, key, value, wait=False):
        return self._command(f'PUT|{key}|{value}', wait)

    def append(self, key, value, wait=False):
        return self._command(f'APPEND|{key}|{value}', wait)

    def _command(self, cmd, wait=False):
        success, _ = self.raft.bridge_coroutine(self.raft.received_command(cmd, wait))
        return success

    def apply_oplog(self, command_list):
        for command in command_list:
            parts = command.split('|')
            if parts[0] == 'PUT' and len(parts) == 3:
                key, val = parts[1:]
                self.mapping[key] = val
            elif parts[0] == 'APPEND' and len(parts) == 3:
                key, val = parts[1:]
                self.mapping[key] += val
