# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.compatibility.sarama_helpers import sarama_sasl_scram


class SaramaScramTest(RedpandaTest):
    """
    Test Sarama's example that uses SASL/SCRAM authentication.
    The example runs in the foreground so there is no need for
    a BackgroundThreadService.
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        extra_rp_conf = dict(enable_sasl=True, )
        super(SaramaScramTest, self).__init__(test_context,
                                              extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_sarama_sasl_scram(self):
        # Get the SASL SCRAM command and a ducktape node
        cmd = sarama_sasl_scram(self.redpanda, self.topic)
        n = random.randint(0, len(self.redpanda.nodes))
        node = self.redpanda.get_node(n)

        def try_cmd():
            # Allow fail because the process exits with
            # non-zero if redpanda is in the middle of a
            # leadership election. Instead we want to
            # retry the cmd.
            result = node.account.ssh_output(cmd,
                                             allow_fail=True,
                                             timeout_sec=10).decode()
            self.logger.debug(result)
            return "wrote message at partition:" in result

        # Using wait_until for auto-retry because sometimes
        # redpanda is in the middle of a leadership election
        wait_until(lambda: try_cmd(),
                   timeout_sec=60,
                   backoff_sec=5,
                   err_msg="sarama sasl scram test failed")
