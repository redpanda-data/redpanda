import os

from ducktape.cluster.remoteaccount import RemoteCommandError

from rptest.services.http_server import HttpServer
from rptest.util import inject_remote_script


class MockIamRolesServer(HttpServer):
    LOG_DIR = "/tmp/mock_iam_server"
    STDOUT_CAPTURE = os.path.join(LOG_DIR, "mock_iam_server.stdout")

    logs = {
        "mock_iam_server_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": True
        },
    }

    def __init__(self,
                 context,
                 script,
                 port=8080,
                 stop_timeout_sec=2,
                 mock_target='aws',
                 ttl_sec=3600):
        super(HttpServer, self).__init__(context, 1)
        self.ttl_sec = ttl_sec
        self.script = script
        self.port = port
        self.stop_timeout_sec = stop_timeout_sec
        self.requests = []
        self.hostname = self.nodes[0].account.hostname
        self.address = f'{self.hostname}:{self.port}'
        self.url = f'http://{self.address}'
        self.remote_script_path = None
        self.mock_target = mock_target

    def _worker(self, idx, node):
        node.account.ssh(f"mkdir -p {MockIamRolesServer.LOG_DIR}",
                         allow_fail=False)
        self.remote_script_path = inject_remote_script(node, self.script)
        cmd = f'python3 -u {self.remote_script_path} ' \
              f'--port {self.port} ' \
              f'--mock {self.mock_target} ' \
              f'--ttl_sec {self.ttl_sec} 2>&1'
        cmd += f' | tee -a {MockIamRolesServer.STDOUT_CAPTURE} &'

        self.logger.debug(f'Starting IAM role server {self.url}')
        for line in node.account.ssh_capture(cmd):
            self.logger.debug(f'received request: {line}')
            if 'response_code' in line:
                self.requests.append(self.try_parse_json(node, line))

    def pids(self, node):
        try:
            cmd = f"ps ax | grep {self.script} | grep -v grep | awk '{{print $1}}'"
            pid_arr = [
                pid for pid in node.account.ssh_capture(
                    cmd, allow_fail=True, callback=int)
            ]
            return pid_arr
        except (RemoteCommandError, ValueError):
            return []
