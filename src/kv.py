class KVService:
    def __init__(self, raft):
        self.mapping = {}
        self.raft = raft
        raft.apply_callback.append(self.apply_oplog)

    def get(self, key, wait=False):
        if not wait:
            return True, self.mapping.get(key)
        success, _, result = self._command(f'GET|{key}', True)
        return success, result

    def put(self, key, value, wait=False):
        success, *_ = self._command(f'PUT|{key}|{value}', wait)
        return success

    def append(self, key, value, wait=False):
        success, *_ = self._command(f'APPEND|{key}|{value}', wait)
        return success

    def _command(self, cmd, wait=False):
        return self.raft.bridge_coroutine(self.raft.received_command(cmd, wait))

    def apply_oplog(self, command_list):
        for command, future in command_list:
            parts = command.split('|')
            if parts[0] == 'PUT' and len(parts) == 3:
                key, val = parts[1:]
                self.mapping[key] = val
                if future:
                    future.set_result(None)
            elif parts[0] == 'APPEND' and len(parts) == 3:
                key, val = parts[1:]
                self.mapping[key] += val
                if future:
                    future.set_result(None)
            elif parts[0] == 'GET' and len(parts) == 2:
                key = parts[1]
                if future:
                    future.set_result(self.mapping.get(key))
