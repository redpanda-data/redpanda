# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import json
import collections
import re
from typing import Optional, Any

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode

from polaris.management.api_client import ApiClient
from polaris.management.configuration import Configuration
from polaris.management.api.polaris_default_api import PolarisDefaultApi


class PolarisCatalog(Service):
    """Polaris Catalog service
    
    The polaris catalog service maintain lifecycle of catalog process on the nodes. 
    The service deploys polaris in a test mode with in-memory storage which is intended 
    to be used for dev/test purposes.
    """
    PERSISTENT_ROOT = "/var/lib/polaris"
    INSTALL_PATH = "/opt/polaris"
    JAR = "polaris-service-1.0.0-all.jar"
    JAR_PATH = os.path.join(INSTALL_PATH, "polaris-service/build/libs", JAR)
    LOG_FILE = os.path.join(PERSISTENT_ROOT, "polaris.log")
    POLARIS_CONFIG = os.path.join(PERSISTENT_ROOT, "polaris-server.yml")
    logs = {
        # Includes charts/ and results/ directories along with benchmark.log
        "polaris_logs": {
            "path": LOG_FILE,
            "collect_default": True
        },
    }
    # the only way to access polaris credentials running with the in-memory
    # storage is to parse them from standard output
    credentials_pattern = re.compile(
        "realm: default-realm root principal credentials: (?P<client_id>.+):(?P<password>.+)"
    )

    nodes: list[ClusterNode]

    def _cmd(self, node):
        java = "/opt/java/java-21"
        return f"{java} -jar {PolarisCatalog.JAR_PATH} server  {PolarisCatalog.POLARIS_CONFIG} \
            1>> {PolarisCatalog.LOG_FILE} 2>> {PolarisCatalog.LOG_FILE} &"

    def __init__(self, ctx, node: ClusterNode | None = None):
        super(PolarisCatalog, self).__init__(ctx, num_nodes=0 if node else 1)

        if node:
            self.nodes = [node]
        self._ctx = ctx
        # catalog API url
        self.catalog_url = None
        # polaris management api url
        self.management_url = None
        self.client_id = None
        self.password = None

    def _parse_credentials(self, node):
        line = node.account.ssh_output(
            f"grep 'root principal credentials' {PolarisCatalog.LOG_FILE}"
        ).decode('utf-8')
        m = PolarisCatalog.credentials_pattern.match(line)
        if m is None:
            raise Exception(f"Unable to find credentials in line: {line}")
        self.client_id = m['client_id']
        self.password = m['password']

    def start_node(self, node, timeout_sec=60, **kwargs):
        node.account.ssh("mkdir -p %s" % PolarisCatalog.PERSISTENT_ROOT,
                         allow_fail=False)
        # polaris server settings
        cfg_yaml = self.render("polaris-server.yml")
        node.account.create_file(PolarisCatalog.POLARIS_CONFIG, cfg_yaml)
        cmd = self._cmd(node)
        self.logger.info(
            f"Starting polaris catalog service on {node.name} with command {cmd}"
        )
        node.account.ssh(cmd, allow_fail=False)

        # wait for the healthcheck to return 200
        def _polaris_ready():
            out = node.account.ssh_output(
                "curl -s -o /dev/null -w '%{http_code}' http://localhost:8182/healthcheck"
            )
            status_code = int(out.decode('utf-8'))
            self.logger.info(f"health check result status code: {status_code}")
            return status_code == 200

        wait_until(_polaris_ready,
                   timeout_sec=timeout_sec,
                   backoff_sec=0.4,
                   err_msg="Error waiting for polaris catalog to start",
                   retry_on_exc=True)

        # setup urls and credentials
        self.catalog_url = f"http://{node.account.hostname}:8181/api/catalog"
        self.management_url = f'http://{node.account.hostname}:8181/api/management/v1'
        self._parse_credentials(node)
        self.logger.info(
            f"Polaris catalog ready, credentials - client_id: {self.client_id}, password: {self.password}"
        )

    def _get_token(self) -> str:
        client = ApiClient(configuration=Configuration(host=self.catalog_url))
        response = client.call_api('POST',
                                   f'{self.catalog_url}/v1/oauth/tokens',
                                   header_params={
                                       'Content-Type':
                                       'application/x-www-form-urlencoded'
                                   },
                                   post_params={
                                       'grant_type': 'client_credentials',
                                       'client_id': self.client_id,
                                       'client_secret': self.password,
                                       'scope': 'PRINCIPAL_ROLE:ALL'
                                   }).response.data

        if 'access_token' not in json.loads(response):
            raise Exception('Failed to get access token')
        return json.loads(response)['access_token']

    def management_client(self) -> ApiClient:
        token = self._get_token()
        return ApiClient(configuration=Configuration(host=self.management_url,
                                                     access_token=token))

    def catalog_client(self) -> ApiClient:
        token = self._get_token()
        return ApiClient(configuration=Configuration(host=self.catalog_url,
                                                     access_token=token))

    def wait_node(self, node, timeout_sec=None):
        ## unused as there is nothing to wait for here
        return False

    def stop_node(self, node, allow_fail=False, **_):

        node.account.kill_java_processes(PolarisCatalog.JAR,
                                         allow_fail=allow_fail)

        def _stopped():
            out = node.account.ssh_output("jcmd").decode('utf-8')
            return PolarisCatalog.JAR not in out

        wait_until(_stopped,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Error stopping Polaris")

    def clean_node(self, node, **_):
        self.stop_node(node, allow_fail=True)
        node.account.remove(PolarisCatalog.PERSISTENT_ROOT, allow_fail=True)
