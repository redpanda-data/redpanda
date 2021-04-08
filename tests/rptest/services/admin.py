# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import requests
from ducktape.utils.util import wait_until


class Admin:
    def __init__(self, redpanda):
        self.redpanda = redpanda

    def create_user(self, username, password, algorithm):
        self.redpanda.logger.info(
            f"Creating user {username}:{password}:{algorithm}")

        path = f"v1/security/users"

        def handle(node):
            url = f"http://{node.account.hostname}:9644/{path}"
            data = dict(
                username=username,
                password=password,
                algorithm=algorithm,
            )
            reply = requests.post(url, json=data)
            self.redpanda.logger.debug(f"{reply.status_code} {reply.text}")
            return reply.status_code == 200

        self._send_request(handle)

    def delete_user(self, username):
        self.redpanda.logger.info(f"Deleting user {username}")

        path = f"v1/security/users/{username}"

        def handle(node):
            url = f"http://{node.account.hostname}:9644/{path}"
            reply = requests.delete(url)
            return reply.status_code == 200

        self._send_request(handle)

    def _send_request(self, handler):
        def try_send():
            nodes = [n for n in self.redpanda.nodes]
            random.shuffle(nodes)
            for node in nodes:
                if handler(node):
                    return True

        wait_until(try_send,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Failed to complete request")
