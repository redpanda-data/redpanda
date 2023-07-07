# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

import subprocess
import random
import string
import sys

BOOTSTRAP_CONFIG = {
    'kafka_tcp_keepalive_timeout': 1,
    'kafka_tcp_keepalive_probe_interval_seconds': 1,
    'kafka_tcp_keepalive_probes': 3
}


class TcpKeepaliveTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        rp_conf = BOOTSTRAP_CONFIG.copy()

        super(TcpKeepaliveTest, self).__init__(*args,
                                               extra_rp_conf=rp_conf,
                                               **kwargs)

    @cluster(num_nodes=1)
    def test_tcp_keepalive(self):
        """
        Test that TCP keepalive causes clients to disconnect
        """

        try:
            # create a random group
            group_name = ''.join(random.choices(string.ascii_letters, k=20))
            subprocess.check_call(['sudo', 'groupadd', group_name])

            # spawn netcat running in that group
            with subprocess.Popen([
                    'sudo', 'sg', group_name, "nc -v {} 9092".format(
                        self.redpanda.nodes[0].account.hostname)
            ],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT) as nc_proc:
                # wait for us to connect
                for line in nc_proc.stdout:
                    line = line.decode('utf-8').lower()
                    print(line, file=sys.stderr)
                    if 'succeeded' in line:
                        break

                # add iptables rule for that group to drop all packets
                subprocess.check_call([
                    'sudo', 'iptables', '-A', 'OUTPUT', '-m', 'owner',
                    '--gid-owner', group_name, '-j', 'DROP'
                ])

                # tcp keepalive should now time out and RP should RST the connection making netcat stop
                nc_proc.wait(timeout=10)

            # confirm RP also saw the client gone
            wait_until(lambda: self.redpanda.search_log_node(
                self.redpanda.nodes[0], 'Disconnected'),
                       timeout_sec=10,
                       err_msg="Failed to find disconnect message in log")

        finally:
            # if these fail nothing we can do so don't check the return code
            subprocess.call([
                'sudo', 'iptables', '-D', 'OUTPUT', '-m', 'owner',
                '--gid-owner', group_name, '-j', 'DROP'
            ])
            subprocess.call(['sudo', 'groupdel', group_name])
