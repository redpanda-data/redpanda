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
# Modifications Copyright 2021 Redpanda Data, Inc.
# - Reformatted code
# - Replaced dependency on Kafka with Redpanda
# - Imported annotate_missing_msgs helper from kafka test suite

from collections import defaultdict, namedtuple
from typing import Optional
import os
from typing import Optional
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from rptest.services.redpanda import RedpandaService
from rptest.services.redpanda_installer import InstallOptions
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.clients.default import DefaultClient
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.services.verifiable_producer import VerifiableProducer, is_int_with_prefix
from rptest.archival.s3_client import S3Client
from rptest.clients.rpk import RpkTool
from rptest.clients.rpk import RpkException

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
    def __init__(self,
                 test_context,
                 extra_rp_conf=None,
                 extra_node_conf=None,
                 si_settings=None):
        super(EndToEndTest, self).__init__(test_context=test_context)
        if extra_rp_conf is None:
            self._extra_rp_conf = {}
        else:
            self._extra_rp_conf = extra_rp_conf

        self._extra_node_conf = extra_node_conf

        self.records_consumed = []
        self.last_consumed_offsets = {}
        self.redpanda: Optional[RedpandaService] = None
        self.si_settings = si_settings
        self.topic = None
        self._client = None

    def start_redpanda(self,
                       num_nodes=1,
                       extra_rp_conf=None,
                       si_settings=None,
                       environment=None,
                       install_opts: Optional[InstallOptions] = None,
                       new_bootstrap=False,
                       max_num_seeds=3):
        if si_settings is not None:
            self.si_settings = si_settings

        if self.si_settings:
            self.si_settings.load_context(self.logger, self.test_context)

        if extra_rp_conf is not None:
            # merge both configurations, the extra_rp_conf passed in
            # paramter takes the precedence
            self._extra_rp_conf = {**self._extra_rp_conf, **extra_rp_conf}
        assert self.redpanda is None

        self.redpanda = RedpandaService(self.test_context,
                                        num_nodes,
                                        extra_rp_conf=self._extra_rp_conf,
                                        extra_node_conf=self._extra_node_conf,
                                        si_settings=self.si_settings,
                                        environment=environment)
        if new_bootstrap:
            seeds = [
                self.redpanda.nodes[i]
                for i in range(0, min(len(self.redpanda.nodes), max_num_seeds))
            ]
            self.redpanda.set_seed_servers(seeds)
        version_to_install = None
        if install_opts:
            if install_opts.install_previous_version:
                version_to_install = \
                    self.redpanda._installer.highest_from_prior_feature_version(RedpandaInstaller.HEAD)
            if install_opts.version:
                version_to_install = install_opts.version

        if version_to_install:
            self.redpanda._installer.install(self.redpanda.nodes,
                                             version_to_install)

        self.redpanda.start(auto_assign_node_id=new_bootstrap,
                            omit_seeds_on_idx_one=not new_bootstrap)
        if version_to_install and install_opts.num_to_upgrade > 0:
            # Perform the upgrade rather than starting each node on the
            # appropriate version. Redpanda may not start up if starting a new
            # cluster with mixed-versions.
            nodes_to_upgrade = [
                self.redpanda.get_node(i + 1)
                for i in range(install_opts.num_to_upgrade)
            ]
            self.redpanda._installer.install(nodes_to_upgrade,
                                             RedpandaInstaller.HEAD)
            self.redpanda.restart_nodes(nodes_to_upgrade)

        self._client = DefaultClient(self.redpanda)
        self._rpk_client = RpkTool(self.redpanda)

    def rpk_client(self):
        assert self._rpk_client is not None
        return self._rpk_client

    def client(self):
        assert self._client is not None
        return self._client

    @property
    def debug_mode(self):
        """
        Useful for tests that want to change behaviour when running on
        the much slower debug builds of redpanda, which generally cannot
        keep up with significant quantities of data or partition counts.
        """
        return os.environ.get('BUILD_TYPE', None) == 'debug'

    def start_consumer(self,
                       num_nodes=1,
                       group_id="test_group",
                       verify_offsets=True,
                       redpanda_cluster=None):
        if redpanda_cluster is None:
            assert self.redpanda
            redpanda_cluster = self.redpanda
        assert self.topic
        self.consumer = VerifiableConsumer(
            self.test_context,
            num_nodes=num_nodes,
            redpanda=redpanda_cluster,
            topic=self.topic,
            group_id=group_id,
            on_record_consumed=self.on_record_consumed,
            verify_offsets=verify_offsets)
        self.consumer.start()

    def start_producer(self,
                       num_nodes=1,
                       throughput=1000,
                       repeating_keys=None,
                       enable_idempotence=False):
        assert self.redpanda
        assert self.topic
        self.producer = VerifiableProducer(
            self.test_context,
            num_nodes=num_nodes,
            redpanda=self.redpanda,
            topic=self.topic,
            throughput=throughput,
            message_validator=is_int_with_prefix,
            repeating_keys=repeating_keys,
            enable_idempotence=enable_idempotence)
        self.producer.start()

    def on_record_consumed(self, record, node):
        partition = TopicPartition(record["topic"], record["partition"])
        key = record["key"]
        record_id = record["value"]
        offset = record["offset"]
        self.last_consumed_offsets[partition] = offset
        self.records_consumed.append((key, record_id))

    def await_consumed_offsets(self, last_acked_offsets, timeout_sec):
        def has_finished_consuming():
            for partition, offset in last_acked_offsets.items():
                if partition not in self.last_consumed_offsets:
                    return False
                last_commit = self.consumer.last_commit(partition)
                if not last_commit or last_commit <= offset:
                    self.logger.debug(
                        f"waiting for partition {partition} offset {offset} "
                        f"to be committed, last committed offset: {last_commit}, "
                        f"last committed timestamp: {self.consumer.get_last_consumed()}, "
                        f"last consumed timestamp: {self.consumer.get_last_consumed()}"
                    )
                    return False
            return True

        wait_until(
            has_finished_consuming,
            timeout_sec=timeout_sec,
            err_msg=lambda:
            f"Consumer failed to consume up to offsets {str(last_acked_offsets)} after waiting {timeout_sec}s, last committed offsets: {self.consumer.get_committed_offsets()}."
        )

    def await_num_produced(self, min_records, timeout_sec=30):
        wait_until(lambda: self.producer.num_acked > min_records,
                   timeout_sec=timeout_sec,
                   err_msg="Producer failed to produce messages for %ds." %\
                   timeout_sec)

    def await_num_consumed(self, min_records, timeout_sec=30):
        wait_until(lambda: self.consumer.total_consumed() >= min_records,
                   timeout_sec=timeout_sec,
                   err_msg="Timed out after %ds while awaiting record consumption of %d records" %\
                   (timeout_sec, min_records))

    def _collect_segment_data(self):
        # TODO: data collection is disabled because it was
        # affecting other tests.
        # See issue https://github.com/redpanda-data/redpanda/issues/7179
        pass

    def _collect_all_logs(self):
        for s in self.test_context.services:
            self.mark_for_collect(s)

    def await_startup(self, min_records=5, timeout_sec=30):
        try:
            self.await_num_consumed(min_records, timeout_sec)
        except BaseException:
            self._collect_all_logs()
            raise

    def run_validation(self,
                       min_records=5000,
                       producer_timeout_sec=30,
                       consumer_timeout_sec=30,
                       enable_idempotence=False,
                       enable_compaction=False):
        try:
            self.await_num_produced(min_records, producer_timeout_sec)

            self.logger.info("Stopping producer after writing up to offsets %s" %\
                         str(self.producer.last_acked_offsets))
            self.producer.stop()
            self.run_consumer_validation(
                consumer_timeout_sec=consumer_timeout_sec,
                enable_idempotence=enable_idempotence,
                enable_compaction=enable_compaction)
        except BaseException:
            self._collect_all_logs()
            raise

    def run_consumer_validation(self,
                                consumer_timeout_sec=30,
                                enable_idempotence=False,
                                enable_compaction=False) -> None:
        try:
            # Take copy of this dict in case a rogue VerifiableProducer
            # thread modifies it.
            # Related: https://github.com/redpanda-data/redpanda/issues/3450
            last_acked_offsets = self.producer.last_acked_offsets.copy()

            self.logger.info("Producer's offsets after stopping: %s" %\
                         str(last_acked_offsets))

            self.await_consumed_offsets(last_acked_offsets,
                                        consumer_timeout_sec)

            self.consumer.stop()

            self.validate(enable_idempotence, enable_compaction)
        except BaseException:
            self._collect_all_logs()
            raise

    def validate_compacted(self):

        consumer_state = {}

        acked_producer_state = {}
        not_acked_producer_state = defaultdict(set)

        for k, v in self.producer.acked:
            acked_producer_state[k] = v

        # some of the not acked records may have been received and persisted,
        # we need to store all of them and check if consumer result is one of them
        for k, v in self.producer.not_acked:
            not_acked_producer_state[k].add(v)

        for k, v in self.records_consumed:
            consumer_state[k] = v

        msg = ""
        success = True
        errors = []

        for consumed_key, consumed_value in consumer_state.items():
            # invalid key consumed
            if consumed_key not in acked_producer_state and consumed_key not in not_acked_producer_state:
                return False, f"key {consumed_key} was consumed but it is missing in produced state"

            # success case, simply continue
            if acked_producer_state[consumed_key] == consumed_value:
                continue

            # we must check not acked state as it might have been caused
            # by request timeout and a message might still have been consumed by consumer
            self.logger.debug(
                f"Checking not acked produced messages for key: {consumed_key}, "
                f"previous acked value: {acked_producer_state[consumed_key]}, "
                f"consumed value: {consumed_value}")
            # consumed value is one of the not acked produced values
            if consumed_key in not_acked_producer_state and consumed_value in not_acked_producer_state[
                    consumed_key]:
                continue

            # consumed value is not equal to last acked produced value and any of not acked value, error out
            success = False
            errors.append((consumed_key, consumed_value,
                           acked_producer_state.get(consumed_key, None),
                           not_acked_producer_state.get(consumed_key, None)))

        if not success:
            msg += "Invalid value detected for consumed compacted topic records. errors: ["
            for key, consumed_value, produced_acked, producer_not_acked in errors:
                msg += f"key: {key} consumed value: {consumed_value}, " \
                       f"produced values: (acked: {produced_acked}, " \
                       f"not_acked: {producer_not_acked})\n"
            msg += "]"

        return success, msg

    def validate(self, enable_idempotence, enable_compaction):
        self.logger.info("Number of acked records: %d" %
                         len(self.producer.acked))
        self.logger.info("Number of consumed records: %d" %
                         len(self.records_consumed))

        success = True
        msg = ""
        if enable_compaction:
            success, msg = self.validate_compacted()
        else:
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
                    len(set(self.records_consumed)) -
                    len(self.records_consumed))

                if enable_idempotence:
                    success = False
                    msg += "Detected %d duplicates even though idempotence was enabled.\n" % num_duplicates
                else:
                    msg += "(There are also %d duplicate messages in the log - but that is an acceptable outcome)\n" % num_duplicates

            consumer_consistency = self.consumer.verify_position_offsets_consistency(
            )
            if not consumer_consistency[0]:
                success = False
                msg += '\n'.join(consumer_consistency[1]) + '\n'

        # Collect all logs if validation fails
        if not success:
            self._collect_all_logs()

        assert success, msg
