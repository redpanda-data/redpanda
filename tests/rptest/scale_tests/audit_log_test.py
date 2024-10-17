# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import threading
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import SaslCredentials
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.scale_parameters import ScaleParameters
from rptest.services.kgo_repeater_service import KgoRepeaterService, repeater_traffic
from rptest.services.redpanda import SaslCredentials, SecurityConfig, MetricsEndpoint, MetricSample, LoggingConfig
from rptest.tests.cluster_config_test import wait_for_version_sync
from rptest.util import wait_until_result
from ducktape.errors import TimeoutError

# How much memory to assign to redpanda per partition. Redpanda will be started
# with MIB_PER_PARTITION * PARTITIONS_PER_SHARD * CORE_COUNT memory
DEFAULT_MIB_PER_PARTITION = 4

# How many partitions we will create per shard: this is the primary scaling
# factor that controls how many partitions a given cluster will get.
PARTITIONS_PER_SHARD = 1000


class AuditLogTestSecurityConfig(SecurityConfig):
    """
    Hardcoded security parameters for the audit scale tests. SASL will be
    the authentication method of choice
    """
    sasl_credentials = SaslCredentials('rob', '123rob456', 'SCRAM-SHA-256')

    def __init__(self):
        super(AuditLogTestSecurityConfig, self).__init__()
        self.enable_sasl = True
        self.kafka_enable_authorization = True
        self.endpoint_authn_method = 'sasl'
        self.tls_provider = None


class AuditBenchmarkResults():
    def __init__(self, time_elapsed: float, final_totals: tuple[int, int],
                 latencies: tuple[float, float, float], message_size: int):
        self.time_elapsed = time_elapsed
        self.messages_produced = final_totals[0]
        self.messages_consumed = final_totals[1]
        self.message_size = message_size
        self.p50 = latencies[0]
        self.p90 = latencies[1]
        self.p99 = latencies[2]

    def _mbps(self, b: float) -> float:
        return (b / self.time_elapsed) / (1024.0 * 1024.0)

    @property
    def produce_mbps(self) -> float:
        bytes_produced = self.messages_produced * self.message_size
        return self._mbps(bytes_produced)

    @property
    def consume_mbps(self) -> float:
        bytes_consumed = self.messages_consumed * self.message_size
        return self._mbps(bytes_consumed)

    @property
    def total_mbps(self) -> float:
        total_bytes_moved = (self.messages_produced +
                             self.messages_consumed) * self.message_size
        return self._mbps(total_bytes_moved)

    def __repr__(self):
        return str({
            'time_elapsed': self.time_elapsed,
            'total_produced': self.messages_produced,
            'total_consumed': self.messages_consumed,
            'p50': self.p50,
            'p90': self.p90,
            'p99': self.p99
        })


class AuditLogTest(RedpandaTest):
    """
    Test to ensure that enabling auditing does not create a discernable decrease
    in performance.
    """
    topics = ()

    def __init__(self, ctx, *args, **kwargs):
        # Ensure redpanda is setup with sasl_enabled and endpoints are configured
        # to work with SASL, otherwise audit client will not be able to connect
        self.security = AuditLogTestSecurityConfig()

        # We will send huge numbers of messages, so tune down the log verbosity
        # as we will be comparing performance to a known good baseline
        kwargs['log_config'] = LoggingConfig(
            'info', logger_levels={'auditing': 'debug'})

        kwargs['extra_rp_conf'] = {
            # Authentication is mandatory for auditing
            'enable_sasl':
            True,

            # No auditing will occur without the global feature flag set to True
            'audit_enabled':
            True,

            # The default is 8, but for a scale test its desired to increase the
            # number of partitions to handle the expected load
            'audit_log_num_partitions':
            12,

            # RF factor for the audit log, the default is 3 however if there is
            # only 1 node, the replication factor is 1 and will increase to this
            # configured value if the number of nodes can support it
            'audit_log_replication_factor':
            3,

            # Enable auditing for every supported event, there are currently 8
            'audit_enabled_event_types': [
                'management', 'produce', 'consume', 'describe', 'heartbeat',
                'authenticate', 'admin', 'schema_registry'
            ],

            # Adjust this value to give the system a chance to buffer more data
            # at the kafka client size
            # default: 16MiB
            'audit_client_max_buffer_size':
            16000000 * 10,

            # This is the frequency at which queues are drained for data to be
            # buffered to the kafka client. Increase this value to buffer more
            # data which increases the likelihood of compressing duplicate events
            # and reducing the total amount of data sent to the audit topic
            # default: 500ms
            'audit_queue_drain_interval_ms':
            2000 * 2,

            # How large the per shard audit buffers are. The more unique events
            # and the more time the data spends residing in the buffer, the
            # larger this may have to become. If this queue is exhausted events
            # for which audited is enabled for will fail is the event cannot be
            # enqueued
            # default: 1MiB
            'audit_queue_max_buffer_size_per_shard':
            1000000 * 10,
        }

        super().__init__(test_context=ctx,
                         security=self.security,
                         *args,
                         **kwargs)

        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.user_creds = AuditLogTestSecurityConfig.sasl_credentials

        self._ctx = ctx
        self.admin = Admin(self.redpanda,
                           auth=(self.superuser.username,
                                 self.superuser.password))
        self._super_rpk = RpkTool(self.redpanda,
                                  username=self.superuser.username,
                                  password=self.superuser.password,
                                  sasl_mechanism=self.superuser.algorithm)
        self._user_rpk = RpkTool(self.redpanda,
                                 username=self.user_creds.username,
                                 password=self.user_creds.password,
                                 sasl_mechanism=self.user_creds.algorithm)

    def _create_audit_test_sasl_user(self):
        self._super_rpk.sasl_create_user(self.user_creds.username,
                                         self.user_creds.password)
        self._super_rpk.sasl_allow_principal(
            f'User:{self.user_creds.username}', ['all'], 'topic', '*')
        self._super_rpk.sasl_allow_principal(
            f'User:{self.user_creds.username}', ['all'], 'group', '*')
        # Attempt to list topics to check if the above worked
        self._user_rpk.list_topics()

    def _repeater_worker_count(self, scale):
        workers = 32 * scale.node_cpus
        if self.redpanda.dedicated_nodes:
            return workers
        else:
            return min(workers, 4)

    def _get_audit_metrics(self):
        def get_metrics_from_nodes():
            patterns = [
                "audit_buffer_usage_ratio",
                "audit_client_buffer_usage_ratio",
            ]
            samples = self.redpanda.metrics_samples(patterns,
                                                    self.redpanda.nodes,
                                                    MetricsEndpoint.METRICS)
            success = samples is not None and set(
                samples.keys()) == set(patterns)
            return success, samples

        try:
            results = wait_until_result(get_metrics_from_nodes,
                                        timeout_sec=5,
                                        backoff_sec=1)

            def avg_metric(mss: list[MetricSample]):
                values = [x.value for x in mss]
                return sum(values) / len(values) if len(values) > 0 else -1

            return {
                name: avg_metric(mss.samples)
                for name, mss in results.items()
            }

        except TimeoutError as _:
            self.redpanda.logger.warn(f"Timed out getting audit metrics")

        return {}

    def _modify_cluster_config(self, cfg):
        patch_result = self.admin.patch_cluster_config(upsert=cfg)
        wait_for_version_sync(self.admin, self.redpanda,
                              patch_result['config_version'])

    def _enable_auditing(self):
        self._modify_cluster_config({'audit_enabled': True})

    def _disable_auditing(self):
        self._modify_cluster_config({'audit_enabled': False})

    def _run_repeater(self, topics: list[str], scale: ScaleParameters):
        repeater_kwargs = {'key_count': 2**32}
        repeater_msg_size = 16384
        rate_limit_bps = int(scale.expect_bandwidth)
        max_buffered_records = 64

        def make_result_set(initial_time: float, repeater: KgoRepeaterService):
            t2 = time.time()
            total_time_elapsed = t2 - initial_time
            final_totals = repeater.total_messages()
            latency = repeater.latency_reports()
            if latency == ():
                raise RuntimeError(
                    "Couldn't fetch latency report from kgo-repeater")
            return AuditBenchmarkResults(total_time_elapsed, final_totals,
                                         latency, repeater_msg_size)

        stop_thread = False

        def periodically_log_metrics():
            while stop_thread is not True:
                self.redpanda.logger.info(
                    f"Aggregated audit metrics: \n{self._get_audit_metrics()}")
                time.sleep(3)

        with repeater_traffic(context=self._ctx,
                              redpanda=self.redpanda,
                              sasl_options=self.user_creds,
                              topics=topics,
                              msg_size=repeater_msg_size,
                              rate_limit_bps=rate_limit_bps,
                              workers=self._repeater_worker_count(scale),
                              max_buffered_records=max_buffered_records,
                              **repeater_kwargs) as repeater:
            # soak for 5 minutes
            soak_time_seconds = 60 * 5
            soak_await_bytes = soak_time_seconds * scale.expect_bandwidth
            soak_await_msgs = soak_await_bytes / repeater_msg_size
            # Add some leeway to avoid flakiness
            soak_timeout = soak_time_seconds * 1.25
            t1 = time.time()
            repeater.reset()
            metrics_log_thread = threading.Thread(
                target=periodically_log_metrics, args=())
            try:
                metrics_log_thread.start()
                repeater.await_progress(soak_await_msgs, soak_timeout)
            except TimeoutError:
                # Progress is determined by a certain number of total messages produces and
                # consumed arriving after a predetermined amount of time, in this case 2 minutes.
                # If that does not occur the TimeoutError is thrown by repeater.await_progress

                result_set = make_result_set(t1, repeater)
                expected_produce_mbps = scale.expect_bandwidth / (1024 *
                                                                  1024.0)
                raise TimeoutError(
                    f"Expected throughput {expected_produce_mbps:.2f}, got throughput {result_set.produce_mbps:.2f}MB/s"
                )
            finally:
                stop_thread = True
                metrics_log_thread.join()

            # If the estimated progress was made, the results are returned to the caller
            # to me able to assert on other properties of the test run.
            return make_result_set(t1, repeater)

    @cluster(num_nodes=5)
    def test_audit_log(self):
        """
        This test attempts to create a worst-case-scenario for audit logging -
        what exactly would that be? It would be a case where many events are distinct
        from eachother which would prevent the system from reducing the total amount
        of messages that are needed to be produced onto the audit topic.

        The best way to abuse this would be via the produce path, to produce messages
        with a high cardinality. Messages are determined to be equivalent if they:

        1. Have the same API type (so kafka::<produce>, or admin_api::<endpoint>)
        2. Have the same authorization result (so user, ip, credentials, etc)
        3. Have the same request parameters

        For point (3) the detail of request parameters is usually not so exhaustive, for
        example in a produce request only the topics not the partitions are part of the
        audit payload. This means increasing the number of partitions will introduce more
        traffic only up until a certain point. Only when partitions for the same topic
        reside on different cores will they produce distinct auditable events.

        Due to the above this test will use many clients with many topics (and a light
        number of partitions, enough to have 1 per shard) to generate a many
        unique auditable messages from the produce, consume and authentication
        APIs.
        """

        # Scale tests are not run on debug builds
        assert not self.debug_mode

        # This user will be passed to the kgo-repeater program
        self._create_audit_test_sasl_user()

        # Message cardinality formula:
        # distinct_events = number_of_workers * number_of_cores * number_of_topics
        #
        # Adding more partitions past the number of cores won't matter because
        # the partition_id isn't part of the audit message payload. We will want only the
        # number of partitions up until there is one per core in the entire cluster. To
        # scale further then that we want to increase the number of topics.
        scale = ScaleParameters(
            self.redpanda,
            replication_factor=3,
            mib_per_partition=DEFAULT_MIB_PER_PARTITION,
            topic_partitions_per_shard=PARTITIONS_PER_SHARD)
        partitions_per_topic = self.redpanda.get_node_cpu_count() * len(
            self.redpanda.nodes)
        total_topics = scale.partition_limit // partitions_per_topic
        assert total_topics >= 1

        # Create the topics as configured with the scale definition above stopping when
        # the partition limit has been exhausted. The partition limit must be spread across
        # a number of topics so that no topic has more then partitions_per_topic partitions.
        topics = [
            TopicSpec(partition_count=partitions_per_topic)
            for _ in range(0, total_topics)
        ]
        extra_partitions = scale.partition_limit % partitions_per_topic
        if extra_partitions > 0:
            # If partition_limit doesn't evenly divide into the total number of shards,
            # create an extra topic with the number of partitions containing the diff
            topics = topics + [TopicSpec(partition_count=extra_partitions)]
        self.redpanda.logger.info(
            f"Creating {len(topics)} topics almost all with {partitions_per_topic} partitions with options: segment.bytes {scale.segment_size} topic retention.bytes {scale.retention_bytes}"
        )
        for topic in topics:
            self._user_rpk.create_topic(topic.name,
                                        partitions=topic.partition_count,
                                        replicas=3,
                                        config={
                                            "segment.bytes":
                                            scale.segment_size,
                                            "retention.bytes":
                                            scale.retention_bytes
                                        })

        # Use the kgo-repeater to generate traffic for 2 minutes
        # Then assert that the traffic is within an expected range
        topic_names = [topic.name for topic in topics]

        self._disable_auditing()
        audit_disabled_results = self._run_repeater(topic_names, scale)

        # Re-run the test and compare results
        self._enable_auditing()
        audit_enabled_results = self._run_repeater(topic_names, scale)

        # Assert that there is no more then a x% difference in observed results
        allowable_threshold = 10.0

        def pct_chg(new, orig):
            return ((orig - new) / abs(orig)) * 100

        self.redpanda.logger.info(
            f"audit_disabled_results: {audit_disabled_results}")
        self.redpanda.logger.info(
            f"audit_enabled_results: {audit_enabled_results}")

        assert pct_chg(
            audit_enabled_results.produce_mbps,
            audit_disabled_results.produce_mbps) < allowable_threshold
        assert pct_chg(
            audit_enabled_results.consume_mbps,
            audit_disabled_results.consume_mbps) < allowable_threshold
        assert pct_chg(audit_enabled_results.p90,
                       audit_disabled_results.p90) > (allowable_threshold * -1)
        assert pct_chg(audit_enabled_results.p99,
                       audit_disabled_results.p99) > (allowable_threshold * -1)
