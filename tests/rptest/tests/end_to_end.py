# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Modifications copyright (C) 2021 Vectorized
# - Reformatted code
# - Replaced dependency on Kafka with Redpanda
# - Imported annotate_missing_msgs helper from kafka test suite

from collections import namedtuple

from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from rptest.services.redpanda import RedpandaService
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.services.verifiable_producer import VerifiableProducer, is_int_with_prefix

TopicPartition = namedtuple('TopicPartition', ['topic', 'partition'])


def annotate_missing_msgs(missing, acked, consumed, msg):
    missing_list = list(missing)
    msg += "%s acked message did not make it to the Consumer. They are: " %\
        len(missing_list)
    if len(missing_list) < 20:
        msg += str(missing_list) + ". "
    else:
        msg += ", ".join(str(m) for m in missing_list[:20])
        msg += "...plus %s more. Total Acked: %s, Total Consumed: %s. " \
            % (len(missing_list) - 20, len(set(acked)), len(set(consumed)))
    return msg


class EndToEndTest(Test):
    """
    Test for common pattern:
      - Produce and consume in the background
      - Perform some action (e.g. partition movement)
      - Run validation
    """
    def __init__(self, test_context, extra_rp_conf=None):
        super(EndToEndTest, self).__init__(test_context=test_context)
        if extra_rp_conf is None:
            self._extra_rp_conf = {}
        else:
            self._extra_rp_conf = extra_rp_conf
        self.records_consumed = []
        self.last_consumed_offsets = {}
        self.redpanda = None
        self.topic = None

    def start_redpanda(self, num_nodes=1, extra_rp_conf=None):
        if extra_rp_conf is not None:
            # merge both configurations, the extra_rp_conf passed in
            # paramter takes the precedence
            self._extra_rp_conf = {**self._extra_rp_conf, **extra_rp_conf}
        assert self.redpanda is None
        self.redpanda = RedpandaService(self.test_context,
                                        num_nodes,
                                        KafkaCliTools,
                                        extra_rp_conf=self._extra_rp_conf)
        self.redpanda.start()

    def start_consumer(self, num_nodes=1, group_id="test_group"):
        assert self.redpanda
        assert self.topic
        self.consumer = VerifiableConsumer(
            self.test_context,
            num_nodes=num_nodes,
            redpanda=self.redpanda,
            topic=self.topic,
            group_id=group_id,
            on_record_consumed=self.on_record_consumed)
        self.consumer.start()

    def start_producer(self, num_nodes=1, throughput=1000):
        assert self.redpanda
        assert self.topic
        self.producer = VerifiableProducer(
            self.test_context,
            num_nodes=num_nodes,
            redpanda=self.redpanda,
            topic=self.topic,
            throughput=throughput,
            message_validator=is_int_with_prefix)
        self.producer.start()

    def on_record_consumed(self, record, node):
        partition = TopicPartition(record["topic"], record["partition"])
        record_id = record["value"]
        offset = record["offset"]
        self.last_consumed_offsets[partition] = offset
        self.records_consumed.append(record_id)

    def await_consumed_offsets(self, last_acked_offsets, timeout_sec):
        def has_finished_consuming():
            for partition, offset in last_acked_offsets.items():
                if not partition in self.last_consumed_offsets:
                    return False
                last_commit = self.consumer.last_commit(partition)
                if not last_commit or last_commit <= offset:
                    return False
            return True

        wait_until(has_finished_consuming,
                   timeout_sec=timeout_sec,
                   err_msg="Consumer failed to consume up to offsets %s after waiting %ds." %\
                   (str(last_acked_offsets), timeout_sec))

    def _collect_all_logs(self):
        for s in self.test_context.services:
            self.mark_for_collect(s)

    def await_startup(self, min_records=5, timeout_sec=30):
        try:
            wait_until(lambda: self.consumer.total_consumed() >= min_records,
                       timeout_sec=timeout_sec,
                       err_msg="Timed out after %ds while awaiting initial record delivery of %d records" %\
                       (timeout_sec, min_records))
        except BaseException:
            self._collect_all_logs()
            raise

    def run_validation(self,
                       min_records=5000,
                       producer_timeout_sec=30,
                       consumer_timeout_sec=30,
                       enable_idempotence=False):
        try:
            wait_until(lambda: self.producer.num_acked > min_records,
                       timeout_sec=producer_timeout_sec,
                       err_msg="Producer failed to produce messages for %ds." %\
                       producer_timeout_sec)

            self.logger.info("Stopping producer after writing up to offsets %s" %\
                         str(self.producer.last_acked_offsets))
            self.producer.stop()

            self.await_consumed_offsets(self.producer.last_acked_offsets,
                                        consumer_timeout_sec)
            self.consumer.stop()

            self.validate(enable_idempotence)
        except BaseException:
            self._collect_all_logs()
            raise

    def validate(self, enable_idempotence):
        self.logger.info("Number of acked records: %d" %
                         len(self.producer.acked))
        self.logger.info("Number of consumed records: %d" %
                         len(self.records_consumed))

        success = True
        msg = ""

        # Correctness of the set difference operation depends on using equivalent
        # message_validators in producer and consumer
        missing = set(self.producer.acked) - set(self.records_consumed)

        if len(missing) > 0:
            success = False
            msg = annotate_missing_msgs(missing, self.producer.acked,
                                        self.records_consumed, msg)

        # Are there duplicates?
        if len(set(self.records_consumed)) != len(self.records_consumed):
            num_duplicates = abs(
                len(set(self.records_consumed)) - len(self.records_consumed))

            if enable_idempotence:
                success = False
                msg += "Detected %d duplicates even though idempotence was enabled.\n" % num_duplicates
            else:
                msg += "(There are also %d duplicate messages in the log - but that is an acceptable outcome)\n" % num_duplicates

        # Collect all logs if validation fails
        if not success:
            self._collect_all_logs()

        assert success, msg
