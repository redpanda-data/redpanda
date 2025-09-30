# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from math import floor
from typing import Optional
from rptest.services.redpanda import RedpandaService, ResourceSettings, SISettings


class ScaleParameters:
    """ScaleParameters helps tests size their workload as appropriate for the current environment.
    Test will in general run on dedicated nodes (in CDT or cloud tests), or in docker containers
    (in dev or CI), so the available memory, CPU and disk resources will be very different between
    those environments. Many functional tests which only create a very modest number of partitions,
    do a few operations and check the results will just work in any of these environments, but test
    which are scale or performance tests, or which test some system limits need to be "scale aware"
    and can use ScaleParameters for that purpose."""

    # How much memory is required per partition (from the partition memory pool
    # specified by DEFAULT_PARTITIONS_MEMORY_ALLOCATION_PERCENT).
    # Should be the same as cluster::DEFAULT_TOPIC_MEMORY_PER_PARTITION.
    DEFAULT_MIB_PER_PARTITION = 200 / 1024  # 200 KiB

    # The maximum number of partitions allowed per shard. The effective limit is
    # the lowest of this limit, the partition memory limit, and a few other
    # limits which usually do not apply in tests.
    # Should be the same as topic_partitions_per_shard in configuration.cc
    DEFAULT_PARTITIONS_PER_SHARD = 5000

    # How much memory is reserved for partitions
    # Should be the same as the default value for
    # topic_partitions_memory_allocation_percent in configuration.cc
    DEFAULT_PARTITIONS_MEMORY_ALLOCATION_PERCENT = 10

    # Number of partitions to create when running in docker (i.e.
    # when dedicated_nodes=false).  This is independent of the
    # amount of RAM or CPU that the nodes claim to have, because
    # we know they are liable to be oversubscribed.
    # This is _not_ for running on oversubscribed CI environments: it's for
    # running on a reasonably powerful developer machine while they work
    # on the test.
    DOCKER_PARTITION_LIMIT = 128

    def __init__(
        self,
        redpanda: RedpandaService,
        replication_factor: int,
        mib_per_partition=DEFAULT_MIB_PER_PARTITION,
        topic_replicas_per_shard=DEFAULT_PARTITIONS_PER_SHARD,
        tiered_storage_enabled=False,
        partition_memory_reserve_percentage=DEFAULT_PARTITIONS_MEMORY_ALLOCATION_PERCENT,
    ):
        self.partition_limit: int
        self.redpanda = redpanda
        self.tiered_storage_enabled = tiered_storage_enabled
        self.partition_memory_reserve_percentage = partition_memory_reserve_percentage

        self.node_count = node_count = len(self.redpanda.nodes)

        node_memory_mib = self.redpanda.get_node_memory_mb()
        self.node_cpus = self.redpanda.get_node_cpu_count()
        node_disk_free = self.redpanda.get_node_disk_free()

        if self.redpanda.dedicated_nodes:
            # Emulate seastar's policy for default reserved memory
            reserved_memory = max(1536, int(0.07 * node_memory_mib) + 1)
            effective_node_memory = node_memory_mib - reserved_memory
        else:
            reserved_memory = 0
            # when in docker, we always end up passing --memory=node_memory
            # explicitly, and this amount does not have the reserve subtracted
            # from it, so we use the full amount. For details see:
            # https://github.com/scylladb/seastar/issues/375#issuecomment-2530012089
            effective_node_memory = node_memory_mib

        self.logger.info(
            f"Nodes have {self.node_cpus} cores, {node_memory_mib}MB memory, {effective_node_memory}MB available memory, {node_disk_free / (1024 * 1024)}MB free disk"
        )

        # On large nodes, reserve half of shard 0 to minimize interference
        # between data and control plane, as control plane messages become
        # very large.
        shard0_reserve: Optional[int] = None
        if self.node_cpus >= 8:
            shard0_reserve = floor(topic_replicas_per_shard / 2)

        # Reserve a few slots for internal partitions. This is a count of
        # partitions and we assume the replication factor is 'replication_factor'
        # in order to calculate the associated number of partition replicas.
        #
        # The way we calculate internal partitions in practice is complicated
        # and this value is an over-simplification. See generate-tiers.py for
        # additional details on internal partition calculations.
        internal_partition_slack = 32

        # Calculate how many partitions we will aim to create, based
        # on the size & count of nodes.  This enables running the
        # test on various instance sizes without explicitly adjusting.
        shard_replicas_from_memory = (
            partition_memory_reserve_percentage
            / 100
            * effective_node_memory
            // self.node_cpus
            // mib_per_partition
        )

        # the per-shard limit is the less of the "per shard" and "per memory" limits
        shard_replicas_effective = min(
            shard_replicas_from_memory, topic_replicas_per_shard
        )

        self.logger.info(
            f"shard_replicas_effective: {shard_replicas_effective} = "
            f"min(topic_replicas_per_shard {topic_replicas_per_shard}, "
            f"shard_replicas_from_memory {shard_replicas_from_memory})"
        )

        self.partition_limit = floor(
            node_count
            * (
                self.node_cpus * (shard_replicas_effective // replication_factor)
                - internal_partition_slack
            )
        )

        self.logger.info(
            f"Cluster partition limit is {self.partition_limit} = "
            f"{node_count} * ({self.node_cpus} * ({shard_replicas_effective} // {replication_factor}) - {internal_partition_slack})"
        )

        if shard0_reserve:
            self.partition_limit -= node_count * shard0_reserve

        if not self.redpanda.dedicated_nodes:
            docker_limit = ScaleParameters.DOCKER_PARTITION_LIMIT
            _pl = min(docker_limit, self.partition_limit)
            self.logger.info(
                f"Selected partition limit {_pl} min({docker_limit}, {self.partition_limit})"
            )
            self.partition_limit = _pl
        else:
            self.logger.info(
                f"Cluster partition limit is {self.partition_limit} = "
                f"{node_count} * {self.node_cpus} * ({shard_replicas_effective} // {replication_factor}) "
                f"- {node_count} * {shard0_reserve}"
            )

        partition_replicas_per_node = int(
            self.partition_limit * replication_factor // node_count
        )

        # Aim to use about half the disk space: set retention limits
        # to enforce that.  This enables traffic tests to run as long
        # as they like without risking filling the disk.
        self.retention_bytes = int((node_disk_free / 2) / partition_replicas_per_node)
        self.local_retention_bytes = None

        # Choose an appropriate segment size to enable retention
        # rules to kick in promptly.
        # TODO: redpanda should figure this out automatically by
        #       rolling segments pre-emptively if low on disk space
        self.segment_size = int(self.retention_bytes / 4)

        # Tiered storage will have a warmup period where it will set the
        # segment size and local retention lower to ensure a large number of
        # segments.
        self.segment_size_after_warmup = self.segment_size

        # NOTE: using retention_bytes that is aimed at occupying disk space.
        self.local_retention_after_warmup = self.retention_bytes

        if tiered_storage_enabled:
            # When testing with tiered storage, the tuning goals of the test
            # parameters are different: we want to stress the number of
            # uploaded segments.

            # Locally retain as many segments as a full day.
            self.segment_size = 32 * 1024

            # Retain as much data in cloud as one big batch of data.
            # NOTE: we consider the existing `retention_bytes` (computed above)
            # so the test doesn't take too much space on disk.
            self.local_retention_bytes = min(
                self.retention_bytes, self.segment_size * 24
            )

            # One of the goals of this test with tiered storage enabled is to
            # test with a large number of managed cloud segments.
            # TODO: consider a variant of this test (or another test) that
            # tests cloud retention.
            self.retention_bytes = -1

            # Set a max upload interval such that won't swamp S3 -- we should
            # already be uploading somewhat frequently given the segment size.
            cloud_storage_segment_max_upload_interval_sec = 300
            cloud_storage_housekeeping_interval_ms = (
                cloud_storage_segment_max_upload_interval_sec * 1000
            )

            self.si_settings = SISettings(
                redpanda._context,
                log_segment_size=self.segment_size,
                cloud_storage_segment_max_upload_interval_sec=cloud_storage_segment_max_upload_interval_sec,
                cloud_storage_housekeeping_interval_ms=cloud_storage_housekeeping_interval_ms,
                use_bucket_cleanup_policy=True,
                skip_end_of_test_scrubbing=True,
            )
        else:
            self.si_settings = None

        # The expect_bandwidth is just for calculating sensible
        # timeouts when waiting for traffic: it is not a scientific
        # success condition for the tests.
        if self.redpanda.dedicated_nodes:
            # A 24 core i3en.6xlarge has about 1GB/s disk write
            # bandwidth.  Divide by 2 to give comfortable room for variation.
            # This is total bandwidth from a group of producers.
            self.expect_bandwidth = (
                (node_count / replication_factor) * (self.node_cpus / 24.0) * 1e9 * 0.5
            )

            # Single-producer tests are slower, bottlenecked on the
            # client side.
            self.expect_single_bandwidth = 200e6

            if tiered_storage_enabled:
                # We read very tiny segments 32KiB over high latency link
                # (10ms to 100ms) with a concurrency limit of cloud_storage_max_connections=20`.
                # Using Little's Law we can derive the arrival rate as `20/0.1`
                # which is to say 200 segments per second. 200 * 32KiB = 6.25MiB
                # per node. This is if we ignore all other sources of latency,
                # contention, and S3 rate limiting or instabilities.
                self.expect_bandwidth = node_count * 6 * 1e6
                # Minimum of server and client bottlenecks.
                self.expect_single_bandwidth = min(
                    self.expect_bandwidth, self.expect_single_bandwidth
                )
        else:
            # Docker environment: curb your expectations.  Not only is storage
            # liable to be slow, we have many nodes sharing the same drive.
            self.expect_bandwidth = 5 * 1024 * 1024
            self.expect_single_bandwidth = 10e6

        # Clamp the node memory to exercise the partition limit.
        # Not all internal partitions have rf=replication_factor so this
        # over-allocates but making it more accurate would be complicated.
        per_node_slack = internal_partition_slack * replication_factor / node_count
        required_node_memory = (
            mib_per_partition
            * (partition_replicas_per_node + per_node_slack)
            / (self.partition_memory_reserve_percentage / 100.0)
        )

        rnm_message = (
            f"required_node_memory:  {required_node_memory} MiB = {mib_per_partition} * ("
            f"{partition_replicas_per_node} + {per_node_slack}) / "
            f"({self.partition_memory_reserve_percentage} / 100.)"
        )

        self.logger.info(rnm_message)

        resource_settings_args = {}
        if not self.redpanda.dedicated_nodes:
            # In docker, assume we're on a laptop drive and not doing
            # real testing, so disable fsync to make test run faster.
            resource_settings_args["bypass_fsync"] = True

            required_node_memory = max(
                required_node_memory, ResourceSettings.DEFAULT_MEMORY_MB
            )
        else:
            # On dedicated nodes we will use an explicit reactor stall threshold
            # as a success condition.
            resource_settings_args["reactor_stall_threshold"] = 100

        resource_settings_args["memory_mb"] = int(required_node_memory)

        self.redpanda.set_resource_settings(ResourceSettings(**resource_settings_args))

        self.logger.info(
            f"Selected retention.bytes={self.retention_bytes}, retention.local.target.bytes={self.local_retention_bytes}, segment.bytes={self.segment_size}"
        )

        # Should not happen on the expected EC2 instance types where
        # the cores-RAM ratio is sufficient to meet our shards-per-core
        if effective_node_memory < required_node_memory:
            raise RuntimeError(
                f"Node memory is too small. Effective memory: {effective_node_memory}MB "
                f"({node_memory_mib}MB node - {reserved_memory}MB reserved), "
                f"required memory: {required_node_memory}MB, ({rnm_message})"
            )

    @property
    def logger(self):
        return self.redpanda.logger
