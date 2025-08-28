# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.si_utils import quiesce_uploads


class TopicPropertiesTest(RedpandaTest):
    def __init__(self, test_context):
        super(TopicPropertiesTest, self).__init__(
            extra_rp_conf=dict({"iceberg_enabled": True}),
            si_settings=SISettings(test_context=test_context, fast_uploads=True),
            test_context=test_context,
            num_brokers=1,
        )

    @cluster(num_nodes=1)
    def test_iceberg_topic_as_read_replica_is_rejected(self):
        topic = "read_replica_tp"
        rpk = RpkTool(self.redpanda)

        # Create a topic that can be pointed at with a read replica.
        # NOTE: it's unusual to do this on the cluster hosting the RRR, but
        # that's unimportant for validating topic properties.
        rpk.create_topic(topic, replicas=1, config={"redpanda.remote.delete": False})
        rpk.produce(topic, key="foo", msg="bar")
        quiesce_uploads(self.redpanda, [topic], timeout_sec=30)
        rpk.delete_topic(topic)

        # Iceberg topic shouldn't be creatable as a read replica.
        try:
            rpk.create_topic(
                topic,
                replicas=1,
                config={
                    "redpanda.iceberg.mode": "key_value",
                    "redpanda.remote.readreplica": self.si_settings.cloud_storage_bucket,
                },
            )
        except Exception as e:
            self.logger.info(f"Creation failed as expected: expected exception {e}")
        else:
            raise RuntimeError("Topic creation should have failed to create")

        # Shouldn't be able to set a read replica to be an Icberg topic.
        rpk.create_topic(
            topic,
            replicas=1,
            config={
                "redpanda.remote.readreplica": self.si_settings.cloud_storage_bucket,
            },
        )
        try:
            rpk.alter_topic_config(self.topic, "redpanda.iceberg.mode", "key_value")
        except Exception as e:
            self.logger.info(f"Alter failed as expected: expected exception {e}")
        else:
            raise RuntimeError("Topic should have failed to alter")
