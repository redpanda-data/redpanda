# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import time

from collections import defaultdict
from ducktape.errors import DucktapeError
from ducktape.mark import matrix

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.clients.kafka_cat import KafkaCat
from rptest.services.cluster import cluster
from rptest.services.redpanda import ValidationMode
from rptest.tests.redpanda_test import RedpandaTest

# Expected log errors in tests that test misbehaving
# idempotency clients.
TX_ERROR_LOGS = []

NOT_LEADER_FOR_PARTITION = (
    "Tried to send a message to a replica that is not the leader for some partition"
)


class SaramaProduceTest(RedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_idempotence": True,
            "id_allocator_replication": 3,
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
        }

        super(SaramaProduceTest, self).__init__(
            test_context=test_context, extra_rp_conf=extra_rp_conf
        )

    @cluster(num_nodes=3, log_allow_list=TX_ERROR_LOGS)
    @matrix(version=["2.1.0", "2.6.0"])
    def test_produce(self, version):
        verifier_bin = "/opt/redpanda-tests/go/sarama/produce_test/produce_test"

        self.redpanda.logger.info("creating topics")

        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1")

        self.redpanda.logger.info("testing sarama produce")
        retries = 5
        for i in range(0, retries):
            try:
                cmd = "{verifier_bin} --brokers {brokers} --version {version}".format(
                    verifier_bin=verifier_bin,
                    brokers=self.redpanda.brokers(),
                    version=version,
                )
                subprocess.check_output(
                    ["/bin/sh", "-c", cmd], stderr=subprocess.STDOUT
                )
                self.redpanda.logger.info("sarama produce test passed")
                break
            except subprocess.CalledProcessError as e:
                error = str(e.output)
                self.redpanda.logger.info("sarama produce failed with " + error)
                if i + 1 != retries and NOT_LEADER_FOR_PARTITION in error:
                    time.sleep(5)
                    continue
                raise DucktapeError("sarama produce failed with " + error)


class SaramaLegacyProduceTest(RedpandaTest):
    """
    This test case uses an old version of Sarama which does NOT properly set the max_timestamp in a batch.
    This version is useful for testing redpanda behavior with improper timestamps.
    """

    # Indexed by [validation_mode][compression.type is not None (true/false)]
    # `legacy` and `relaxed` modes will warn if the batch is compressed.
    # `strict` mode will not warn at all.
    expected_log_lines = defaultdict(dict)
    expected_log_lines[ValidationMode.RELAXED][False] = None
    expected_log_lines[ValidationMode.RELAXED][True] = (
        "Produced batch for partition {kafka/topic/0} has max_timestamp left unset ({-1}) by client (client_id: {sarama}). Decompressing batch and setting max_timestamp manually since 'kafka_produce_batch_validation' is set to 'relaxed'. It is strongly recommended that you update your client to set the max_timestamp when producing"
    )
    expected_log_lines[ValidationMode.LEGACY][False] = None
    expected_log_lines[ValidationMode.LEGACY][True] = (
        "Produced batch for partition {kafka/topic/0} has max_timestamp left unset ({-1}) by client (client_id: {sarama}). Accepting batch since 'kafka_produce_batch_validation' is set to 'legacy'. It is strongly recommended that you update your client to set the max_timestamp when producing"
    )
    expected_log_lines[ValidationMode.STRICT][False] = None
    expected_log_lines[ValidationMode.STRICT][True] = None

    def __init__(self, test_context):
        super().__init__(test_context=test_context)

    @cluster(num_nodes=1)
    @matrix(
        validation_mode=[
            ValidationMode.LEGACY,
            ValidationMode.RELAXED,
            ValidationMode.STRICT,
        ],
        compression_type=[TopicSpec.COMPRESSION_NONE, TopicSpec.COMPRESSION_LZ4],
    )
    def test_produce(self, validation_mode, compression_type):
        verifier_bin = (
            "/opt/redpanda-tests/go/sarama/produce_legacy_version_test/produce_test"
        )

        self.redpanda.set_cluster_config(
            {
                "kafka_produce_batch_validation": validation_mode,
            }
        )

        topic = "topic"
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic)

        now = int(time.time())

        self.redpanda.logger.info("testing sarama produce")
        retries = 5
        for i in range(0, retries):
            try:
                cmd = f"{verifier_bin} --brokers {self.redpanda.brokers()} --compression_type {compression_type.value}"
                subprocess.check_output(
                    ["/bin/sh", "-c", cmd], stderr=subprocess.STDOUT
                )
                self.redpanda.logger.info("sarama produce test passed")
                break
            except subprocess.CalledProcessError as e:
                error = str(e.output)
                self.redpanda.logger.info("sarama produce failed with " + error)
                if i + 1 != retries and NOT_LEADER_FOR_PARTITION in error:
                    time.sleep(5)
                    continue
                raise DucktapeError("sarama produce failed with " + error)

        is_compressed = compression_type is not TopicSpec.COMPRESSION_NONE
        expected_log_line = self.expected_log_lines[validation_mode][is_compressed]

        if expected_log_line is None:
            # We shouldn't see any warning for an unset max_timestamp if it is not expected.
            pattern = "Produced batch for partition {kafka/topic/0} has max_timestamp left unset ({-1}) by client"
            assert not self.redpanda.search_log_all(pattern), (
                f"Saw unexpected log line in redpanda logs for {validation_mode=}, {compression_type=} when maximum timestamp is unset in a produced batch."
            )
        else:
            assert self.redpanda.search_log_all(expected_log_line), (
                f"Did not see expected log line in redpanda logs for {validation_mode=}, {compression_type=} when maximum timestamp is unset in a produced batch."
            )

        # Perform a timequery
        kcat = KafkaCat(self.redpanda)
        offset = kcat.query_offset(topic, 0, now)
        if validation_mode is ValidationMode.LEGACY and is_compressed:
            # Legacy mode with compression makes no guarantees
            # about timequeries not being broken because of an
            # unset `max_timestamp`.
            assert offset == -1, (
                f"Expected timequery to be broken for {validation_mode=}, {compression_type=} when maximum timestamp is unset in a produced batch."
            )
        else:
            # All other possible validation mode and compression
            # combinations should do the right thing.
            assert offset >= 0, (
                f"Expected timequery to return the correct result for {validation_mode=}, {compression_type=} when maximum timestamp is unset in a produced batch."
            )
