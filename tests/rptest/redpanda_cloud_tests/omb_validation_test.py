# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import math
from time import sleep

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.tests.test import TestContext
from rptest.services.producer_swarm import ProducerSwarm
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda_cloud import CloudTierName, ProductInfo, get_config_profile_name
from rptest.services.redpanda import (SISettings, RedpandaServiceCloud)
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import \
    OMBSampleConfigurations
from rptest.services.machinetype import get_machine_info

KiB = 1024
MiB = KiB * KiB
GiB = KiB * MiB
KB = 10**3
MB = 10**6
GB = 10**9
minutes = 60
hours = 60 * minutes


class OMBValidationTest(RedpandaTest):

    # common workload details shared among most/all test methods
    WORKLOAD_DEFAULTS = {
        "topics": 1,
        "message_size": 1 * KiB,
        "payload_file": "payload/payload-1Kb.data",
        "consumer_backlog_size_GB": 0,
        "test_duration_minutes": 5,
        "warmup_duration_minutes": 5,
    }

    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        self._ctx = test_ctx
        # Get tier name
        self.config_profile_name = get_config_profile_name(
            RedpandaServiceCloud.get_cloud_globals(self._ctx.globals))
        extra_rp_conf = None
        num_brokers = None

        if self.config_profile_name == CloudTierName.DOCKER:
            # TODO: Bake the docker config into a higher layer that will
            # automatically load these settings upon call to make_rp_service
            num_brokers = 3
            extra_rp_conf = {
                'log_segment_size': 128 * MiB,
                'cloud_storage_cache_size': 20 * GiB,
                'kafka_connections_max': 100,
            }

        super(OMBValidationTest,
              self).__init__(test_ctx,
                             *args,
                             num_brokers=num_brokers,
                             extra_rp_conf=extra_rp_conf,
                             cloud_tier=self.config_profile_name,
                             disable_cloud_storage_diagnostics=True,
                             **kwargs)

        # Load install pack and check profile
        install_pack = self.redpanda.get_install_pack()
        self.logger.info(f"Loaded install pack '{install_pack['version']}': "
                         f"Redpanda v{install_pack['redpanda_version']}, "
                         f"created at '{install_pack['created_at']}'")
        if self.config_profile_name not in install_pack['config_profiles']:
            # throw user friendly error
            _profiles = ", ".join(
                [f"'{k}'" for k in install_pack['config_profiles']])
            raise RuntimeError(
                f"'{self.config_profile_name}' not found among config profiles: {_profiles}"
            )
        config_profile = install_pack['config_profiles'][
            self.config_profile_name]
        cluster_config = config_profile['cluster_config']

        if self.config_profile_name == CloudTierName.DOCKER:
            si_settings = SISettings(
                test_ctx,
                log_segment_size=self.min_segment_size,
                cloud_storage_cache_size=cluster_config[
                    'cloud_storage_cache_size'],
            )
            self.redpanda.set_si_settings(si_settings)
            self.s3_port = si_settings.cloud_storage_api_endpoint_port

        self.num_brokers = config_profile['nodes_count']
        self.tier_limits: ProductInfo = self.redpanda.get_product()
        self.tier_machine_info = get_machine_info(
            config_profile['machine_type'])
        self.rpk = RpkTool(self.redpanda)

        self.base_validator = {
            OMBSampleConfigurations.E2E_LATENCY_50PCT:
            [OMBSampleConfigurations.lte(20)],
            OMBSampleConfigurations.E2E_LATENCY_75PCT:
            [OMBSampleConfigurations.lte(25)],
            OMBSampleConfigurations.E2E_LATENCY_99PCT:
            [OMBSampleConfigurations.lte(50)],
            OMBSampleConfigurations.E2E_LATENCY_999PCT:
            [OMBSampleConfigurations.lte(100)],
        }

    def _partition_count(self) -> int:
        machine_config = self.tier_machine_info
        return 5 * self.num_brokers * machine_config.num_shards

    def _producer_count(self, ingress_rate) -> int:
        """Determine the number of producers based on the ingress rate.
        We assume that each producer is capable of 5 MB/s."""
        return max(ingress_rate // (5 * MB), 1)

    def _consumer_count(self, egress_rate) -> int:
        """Determine the number of consumers based on the egress rate.
        We assume that each consumer is capable of 5 MB/s."""
        return max(egress_rate // (5 * MB), 1)

    def _mb_to_mib(self, mb):
        return math.floor(0.9537 * mb)

    @cluster(num_nodes=12)
    def test_max_connections(self):
        tier_limits = self.tier_limits

        # Constants
        #

        PRODUCER_TIMEOUT_MS = 5000
        OMB_WORKERS = 4
        SWARM_WORKERS = 7

        # OMB parameters
        #

        producer_rate = tier_limits.max_ingress // 5
        subscriptions = max(tier_limits.max_egress // tier_limits.max_ingress,
                            1)
        total_producers = self._producer_count(producer_rate)
        total_consumers = self._consumer_count(producer_rate * subscriptions)
        warmup_duration = self.WORKLOAD_DEFAULTS["warmup_duration_minutes"]
        test_duration = self.WORKLOAD_DEFAULTS["test_duration_minutes"]

        workload = self.WORKLOAD_DEFAULTS | {
            "name": "MaxConnectionsTestWorkload",
            "partitions_per_topic": self._partition_count(),
            "subscriptions_per_topic": subscriptions,
            "consumer_per_subscription": max(total_consumers // subscriptions,
                                             1),
            "producers_per_topic": total_producers,
            "producer_rate": producer_rate // (1 * KiB),
        }

        driver = {
            "name": "MaxConnectionsTestDriver",
            "replication_factor": 3,
            "request_timeout": 300000,
            "producer_config": {
                "enable.idempotence": "true",
                "acks": "all",
                "linger.ms": 1,
                "max.in.flight.requests.per.connection": 5,
            },
            "consumer_config": {
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
            },
        }

        validator = self.base_validator | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(producer_rate // (1 * MB))),
            ],
        }

        # ProducerSwarm parameters
        #

        producer_kwargs = {}
        producer_kwargs['min_record_size'] = 64
        producer_kwargs['max_record_size'] = 64

        effective_msg_size = producer_kwargs['min_record_size'] + (
            producer_kwargs['max_record_size'] -
            producer_kwargs['min_record_size']) // 2

        conn_limit = tier_limits.max_client_count - 3 * (total_producers +
                                                         total_consumers)
        _target_per_node = conn_limit // SWARM_WORKERS
        _conn_per_node = int(_target_per_node * 0.7)

        msg_rate_per_node = (1 * KiB) // effective_msg_size
        messages_per_sec_per_producer = max(
            msg_rate_per_node // _conn_per_node, 1)

        # single producer runtime
        # Roughly every 500 connection needs 60 seconds to ramp up
        time_per_500_s = 120
        warm_up_time_s = max(
            time_per_500_s * math.ceil(_target_per_node / 500), time_per_500_s)
        target_runtime_s = 60 * (test_duration +
                                 warmup_duration) + warm_up_time_s
        records_per_producer = messages_per_sec_per_producer * target_runtime_s

        self._ctx.logger.warn(
            f"Producers per node: {_conn_per_node} Messages per producer: {records_per_producer} Message rate: {messages_per_sec_per_producer} msg/s"
        )

        producer_kwargs[
            'messages_per_second_per_producer'] = messages_per_sec_per_producer

        benchmark = OpenMessagingBenchmark(self._ctx,
                                           self.redpanda,
                                           driver, (workload, validator),
                                           num_workers=OMB_WORKERS,
                                           topology="ensemble")

        # Create topic for swarm workers after OMB to avoid the reset
        swarm_topic_name = "swarm_topic"
        try:
            self.rpk.delete_topic(swarm_topic_name)
        except:
            # Ignore the exception that is thrown if the topic doesn't exist.
            pass

        self.rpk.create_topic(swarm_topic_name,
                              self._partition_count(),
                              replicas=3)

        swarm = []
        for _ in range(SWARM_WORKERS):
            _swarm_node = ProducerSwarm(
                self._ctx,
                self.redpanda,
                topic=swarm_topic_name,
                producers=_conn_per_node,
                records_per_producer=records_per_producer,
                timeout_ms=PRODUCER_TIMEOUT_MS,
                **producer_kwargs)

            swarm.append(_swarm_node)

        for s in swarm:
            s.start()

        # Allow time for the producers in the swarm to authenticate and start
        self._ctx.logger.info(
            f"waiting {warm_up_time_s} seconds to producer swarm to start")
        sleep(warm_up_time_s)

        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5

        try:
            benchmark.wait(timeout_sec=benchmark_time_min * 60)

            for s in swarm:
                s.wait(timeout_sec=5 * 60)

            benchmark.check_succeed()

        finally:
            self.rpk.delete_topic(swarm_topic_name)

    @cluster(num_nodes=12)
    def test_max_partitions(self):
        tier_limits = self.tier_limits

        partitions_per_topic = tier_limits.max_partition_count
        subscriptions = max(tier_limits.max_egress // tier_limits.max_ingress,
                            1)
        producer_rate = tier_limits.max_ingress // 2
        total_producers = self._producer_count(producer_rate)
        total_consumers = self._consumer_count(producer_rate * subscriptions)

        workload = self.WORKLOAD_DEFAULTS | {
            "name": "MaxPartitionsTestWorkload",
            "partitions_per_topic": partitions_per_topic,
            "subscriptions_per_topic": subscriptions,
            "consumer_per_subscription": max(total_consumers // subscriptions,
                                             1),
            "producers_per_topic": total_producers,
            "producer_rate": producer_rate / (1 * KiB),
        }

        validator = self.base_validator | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(producer_rate // (1 * MB))),
            ],
        }

        benchmark = OpenMessagingBenchmark(
            self._ctx,
            self.redpanda,
            "ACK_ALL_GROUP_LINGER_1MS_IDEM_MAX_IN_FLIGHT",
            (workload, validator),
            num_workers=10,
            topology="ensemble")
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()

    @cluster(num_nodes=12)
    def test_common_workload(self):
        tier_limits = self.tier_limits

        subscriptions = max(tier_limits.max_egress // tier_limits.max_ingress,
                            1)
        partitions = self._partition_count()
        total_producers = self._producer_count(tier_limits.max_ingress)
        total_consumers = self._consumer_count(tier_limits.max_egress)
        validator = self.base_validator | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(tier_limits.max_ingress // (1 * MB))),
            ],
        }

        workload = self.WORKLOAD_DEFAULTS | {
            "name": "CommonTestWorkload",
            "partitions_per_topic": partitions,
            "subscriptions_per_topic": subscriptions,
            "consumer_per_subscription": max(total_consumers // subscriptions,
                                             1),
            "producers_per_topic": total_producers,
            "producer_rate": tier_limits.max_ingress // (1 * KiB),
        }

        driver = {
            "name": "CommonTestDriver",
            "reset": "true",
            "replication_factor": 3,
            "request_timeout": 300000,
            "producer_config": {
                "enable.idempotence": "true",
                "acks": "all",
                "linger.ms": 1,
                "max.in.flight.requests.per.connection": 5,
            },
            "consumer_config": {
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
            },
        }

        benchmark = OpenMessagingBenchmark(self._ctx,
                                           self.redpanda,
                                           driver, (workload, validator),
                                           num_workers=10,
                                           topology="ensemble")
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()

    @cluster(num_nodes=12)
    def test_retention(self):
        tier_limits = self.tier_limits

        subscriptions = max(tier_limits.max_egress // tier_limits.max_ingress,
                            1)
        producer_rate = tier_limits.max_ingress
        partitions = self._partition_count()
        segment_bytes = 64 * MiB
        retention_bytes = 2 * segment_bytes
        # This will have 1/2 the test run with segment deletion occuring.
        test_duration_seconds = max(
            (2 * retention_bytes * partitions) // producer_rate, 5 * 60)
        total_producers = 10
        total_consumers = 10

        workload = self.WORKLOAD_DEFAULTS | {
            "name": "RetentionTestWorkload",
            "partitions_per_topic": partitions,
            "subscriptions_per_topic": subscriptions,
            "consumer_per_subscription": max(total_consumers // subscriptions,
                                             1),
            "producers_per_topic": total_producers,
            "producer_rate": producer_rate // (1 * KiB),
            "test_duration_minutes": test_duration_seconds // 60,
        }

        driver = {
            "name": "RetentionTestDriver",
            "replication_factor": 3,
            "request_timeout": 300000,
            "producer_config": {
                "enable.idempotence": "true",
                "acks": "all",
                "linger.ms": 1,
                "max.in.flight.requests.per.connection": 5,
            },
            "consumer_config": {
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
            },
            "topic_config": {
                "retention.bytes": retention_bytes,
                "retention.local.target.bytes": retention_bytes,
                "segment.bytes": segment_bytes,
            },
        }

        validator = self.base_validator | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(producer_rate // (1 * MB))),
            ],
        }

        benchmark = OpenMessagingBenchmark(self._ctx,
                                           self.redpanda,
                                           driver, (workload, validator),
                                           num_workers=10,
                                           topology="ensemble")
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
