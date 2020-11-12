# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import os

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest


class DemoTest(RedpandaTest):
    def __init__(self, ctx):
        super(DemoTest, self).__init__(test_context=ctx)

    @cluster(num_nodes=3)
    def test_demo_script(self):
        env = dict(KAFKA_PATH="/opt/kafka-2.4.1",
                   SERVERS=self.redpanda.brokers(),
                   TOPIC="ducky-demo-topic",
                   REPLICATION_FACTOR="3",
                   RECORD_COUNT="5000",
                   BACKGROUND_PRODUCE="0",
                   PRODUCER_COUNT="2",
                   **os.environ)
        p = subprocess.Popen(["/opt/v/tools/demo_script.sh"],
                             bufsize=1,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             shell=True,
                             env=env)
        for line in iter(p.stdout.readline, b''):
            self.logger.debug(line.rstrip())
        p.wait()
        if p.returncode != 0:
            raise RuntimeError("demo script failed {}".format(p.returncode))
