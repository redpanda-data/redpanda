# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import os
import requests

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service
from rptest.util import wait_until, wait_until_result

SERVER_DIR = '/opt/ocsf-server'
SCHEMA_DIR = '/opt/ocsf-schema'
KEY_FILE = os.path.join(SERVER_DIR, "server.key")
CRT_FILE = os.path.join(SERVER_DIR, "server.crt")
SCHEMA_SERVER_BIN = os.path.join(SERVER_DIR, "dist/bin/schema_server")

CMD_TMPL = "HTTPS_KEY_FILE={key_file} HTTPS_CERT_FILE={cert_file} SCHEMA_DIR={schema_dir} {bin_file} {cmd}"

HTTP_PORT = 8000


class OcsfSchemaError(Exception):
    """Exception used by OCSF services
    """
    def __init__(self, error):
        super(OcsfSchemaError, self).__init__(error)


class OcsfServer(Service):
    """Service used to start and interact with the OCSF server
    
    This service provides the ability to validate OCSF schema
    """
    def __init__(self, context):
        super(OcsfServer, self).__init__(context, num_nodes=1)

    def _format_cmd(self, cmd: str):
        return CMD_TMPL.format(key_file=KEY_FILE,
                               cert_file=CRT_FILE,
                               schema_dir=SCHEMA_DIR,
                               bin_file=SCHEMA_SERVER_BIN,
                               cmd=cmd)

    def _start_cmd(self):
        return self._format_cmd("daemon")

    def _pid_cmd(self):
        return self._format_cmd("pid")

    def _stop_cmd(self):
        return self._format_cmd("stop")

    def _create_uri(self, node, path):
        if node is None:
            node = self.nodes[0]
        hostname = node.account.hostname
        return f'http://{hostname}:{HTTP_PORT}/api/{path}'

    def get_api_version(self, node=None):
        """Returns current verison of OCSF server
        
        Parameters
        ----------
        node: default=None
        
        Returns
        -------
        Version of the OCSF server
        """
        uri = self._create_uri(node, 'version')
        self.logger.debug(f'Getting version via "{uri}"')

        def _wait_for_api_version():
            r = requests.get(uri)
            if r.status_code != 200:
                return (False, None)
            return (True, r)

        r = wait_until_result(_wait_for_api_version,
                              timeout_sec=5,
                              backoff_sec=1,
                              retry_on_exc=True,
                              err_msg=f'Could not get API version from {uri}')

        r = requests.get(uri)
        if r.status_code != 200:
            raise Exception(f'Unexepected status code: {r.status_code}')
        return r.json()['version']

    def validate_schema(self, schema, node=None):
        """Validates a provided schema

        Will throw an OcsfSchemaError if the schema fails to validate
        
        Parameters
        ----------
        schema : json
            The schema to generate
            
        node : default=None
        """
        uri = self._create_uri(node, 'validate')
        self.logger.debug(
            f'Attempting to validate schema {schema} against {uri}')
        r = requests.post(uri,
                          headers={
                              'content-type': 'application/json',
                              'accept': 'application/json'
                          },
                          json=schema)
        if r.status_code != 200:
            raise Exception(f'Unexpected status code: {r.status_code}')
        self.logger.debug(f'Response from server: {r.json()}')
        resp = r.json()
        if len(resp) != 0:
            raise OcsfSchemaError(json.dumps(resp))

    def pids(self, node):
        pid_cmd = self._pid_cmd()
        self.logger.debug(f'Getting OCSF server pid with cmd "{pid_cmd}"')
        try:
            line = node.account.ssh_capture(pid_cmd,
                                            allow_fail=False,
                                            callback=int)
            p = next(line)
            if node.account.alive(p):
                return [p]
        except (RemoteCommandError, ValueError):
            self.logger.warn(f'pid file not found for ocsf server')

        return []

    def start_node(self, node):
        cmd = self._start_cmd()
        self.logger.debug(f'Starting OCSF Server with cmd "{cmd}"')
        node.account.ssh(cmd, allow_fail=False)

        def _wait_for_version():
            try:
                _ = self.get_api_version()
                return True
            except Exception:
                # Ignore exceptions as server may be coming up and
                # connections may time out
                pass
            return False

        wait_until(_wait_for_version,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg='Failed to get version from server during startup')

    def stop_node(self, node):
        cmd = self._stop_cmd()
        self.logger.debug(f'Stopping OCSF server with cmd "{cmd}"')
        node.account.ssh(cmd, allow_fail=True)
