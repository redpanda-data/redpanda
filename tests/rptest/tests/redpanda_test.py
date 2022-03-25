# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from ducktape.tests.test import Test
from rptest.services.redpanda import RedpandaService
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.default import DefaultClient
from rptest.util import Scale


class RedpandaTest(Test):
    """
    Base class for tests that use the Redpanda service.
    """

    # List of topics to be created automatically when the cluster starts. Each
    # topic is defined by an instance of a TopicSpec.
    topics = []

    def __init__(self,
                 test_context,
                 num_brokers=None,
                 extra_rp_conf=dict(),
                 enable_pp=False,
                 enable_sr=False,
                 **kwargs):
        """
        Any trailing keyword arguments are passed through to the
        RedpandaService constructor.
        """
        super(RedpandaTest, self).__init__(test_context)
        self.scale = Scale(test_context)

        if num_brokers is None:
            # Default to a 3 node cluster if sufficient nodes are available, else
            # a single node cluster.  This is just a default: tests are welcome
            # to override constructor to pass an explicit size.  This logic makes
            # it convenient to mix 3 node and 1 node cases in the same class, by
            # just modifying the @cluster node count per test.
            if test_context.cluster.available().size() >= 3:
                num_brokers = 3
            else:
                num_brokers = 1

        self.redpanda = RedpandaService(test_context,
                                        num_brokers,
                                        extra_rp_conf=extra_rp_conf,
                                        enable_pp=enable_pp,
                                        enable_sr=enable_sr,
                                        **kwargs)
        self._client = DefaultClient(self.redpanda)

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
        self._create_initial_topics()

    def client(self):
        return self._client

    def _create_initial_topics(self):
        for spec in self.topics:
            self.logger.debug(f"Creating initial topic {spec}")
            self.client().create_topic(spec)
