# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import string

from ducktape.tests.test import Test
from rptest.services.redpanda import RedpandaService


class TopicSpec:
    """
    A topic specification.

    It is often the case that in a test the name of a topic does not matter. To
    simplify for this case, a random name is generated if none is provided.
    """
    CLEANUP_COMPACT = "compact"
    CLEANUP_DELETE = "delete"

    def __init__(self,
                 *,
                 name=None,
                 partitions=1,
                 replication_factor=1,
                 cleanup_policy=None):
        self.name = name or f"topic-{self._random_topic_suffix()}"
        self.partitions = partitions
        self.replication_factor = replication_factor
        self.cleanup_policy = cleanup_policy

    def __str__(self):
        return self.name

    def _random_topic_suffix(self, size=4):
        return "".join(
            random.choice(string.ascii_lowercase) for _ in range(size))


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
                 topics=None,
                 log_level='info'):
        super(RedpandaTest, self).__init__(test_context)

        self.redpanda = RedpandaService(test_context,
                                        num_brokers=num_brokers,
                                        extra_rp_conf=extra_rp_conf,
                                        topics=self.topics,
                                        log_level=log_level)

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
