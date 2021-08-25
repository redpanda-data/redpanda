# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import os

from ducktape.tests.test import Test
from rptest.services.redpanda import RedpandaService
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.util import Scale


class RedpandaTest(Test):
    """
    Base class for tests that use the Redpanda service.
    """

    # List of topics to be created automatically when the cluster starts. Each
    # topic is defined by an instance of a TopicSpec.
    topics = ()

    def __init__(self,
                 test_context,
                 num_brokers=3,
                 extra_rp_conf=dict(),
                 enable_pp=False,
                 enable_sr=False,
                 resource_settings=None):
        super(RedpandaTest, self).__init__(test_context)
        self.scale = Scale(test_context)
        self.redpanda = RedpandaService(test_context,
                                        num_brokers,
                                        KafkaCliTools,
                                        extra_rp_conf=extra_rp_conf,
                                        enable_pp=enable_pp,
                                        enable_sr=enable_sr,
                                        topics=self.topics,
                                        resource_settings=resource_settings)

    @property
    def topic(self):
        """
        Return the name of the auto-created initial topic. Accessing this
        property requires exactly one initial topic be configured.
        """
        assert len(self.topics) == 1
        return self.topics[0].name

    def setUp(self):
        self.redpanda.start()
