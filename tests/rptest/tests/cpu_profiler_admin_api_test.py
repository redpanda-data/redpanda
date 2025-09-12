# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
from typing import Any

import requests

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_repeater_service import repeater_traffic
from rptest.services.redpanda import LoggingConfig
from rptest.tests.redpanda_test import RedpandaTest

VERSION_REGEX = re.compile(r".+ - .+")


def assert_profile_good_v2(profile: dict[str, Any], wait_ms: int | None = None):
    """Check that a profile looks normal."""

    assert profile

    def get(key: str):
        assert key in profile, (
            f"profile did not contain expected top-level attribute '{key}'"
        )
        return profile[key]

    assert get("arch") in ("amd64", "arm64"), f"bad arch: {profile['arch']}"
    assert get("schema") == 2
    if wait_ms is not None:
        assert get("wait_ms") == wait_ms
    assert int(get("sample_period_ms")) > 0
    assert VERSION_REGEX.match(get("version"))
    profile_attr = get("profile")
    assert len(profile_attr) > 0, "At least one shard should exist"
    samples = profile_attr[0]["samples"]
    assert len(samples) > 0, "At least one cpu profile should've been collected."


class CPUProfilerAdminAPITest(RedpandaTest):
    topics = (TopicSpec(partition_count=30, replication_factor=1),)

    def __init__(self, test_context):
        super(CPUProfilerAdminAPITest, self).__init__(
            test_context=test_context,
            num_brokers=1,
            log_config=LoggingConfig("info", logger_levels={"resources": "trace"}),
            extra_rp_conf={
                "cpu_profiler_enabled": False,
                "cpu_profiler_sample_period_ms": 50,
            },
        )

        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=2)
    def test_get_cpu_profile_with_override(self):
        # Provide traffic so there is something to sample.
        with repeater_traffic(
            context=self.test_context,
            redpanda=self.redpanda,
            topics=[self.topic],
            msg_size=4096,
            workers=1,
        ) as repeater:
            repeater.await_group_ready()
            profile = self.admin.get_cpu_profile(wait_ms=30 * 1_000)
            assert_profile_good_v2(profile)

    @cluster(num_nodes=1)
    def test_get_cpu_profile_with_override_limits(self):
        try:
            self.admin.get_cpu_profile(wait_ms=16 * 60 * 1_000)
        except requests.exceptions.HTTPError:
            pass
        else:
            assert False, "call with wait_ms > 15min should have failed"

        try:
            self.admin.get_cpu_profile(wait_ms=0)
        except requests.exceptions.HTTPError:
            pass
        else:
            assert False, "call with wait_ms < 1ms should have failed"

    @cluster(num_nodes=1)
    def test_cpu_profile_stress(self):
        self.redpanda.set_cluster_config(
            {
                "cpu_profiler_enabled": True,
                "cpu_profiler_sample_period_ms": 1,
            }
        )

        node0 = self.redpanda.nodes[0]

        self.admin.stress_fiber_start(
            num_fibers=1,
            node=node0,
            min_spins_per_scheduling_point=1000,
            max_spins_per_scheduling_point=1000,
            stack_depth=64,
        )

        # There is currently effectively a max sample count of ~1280 samples
        # due to a limit of 10 sample buffers * 128 samples per buffer, so
        # waiting 1,500 ms is enough for a worst-case.
        wait_ms = 1500
        profile = self.admin.get_cpu_profile(wait_ms=wait_ms, node=node0)
        assert_profile_good_v2(profile)

        max_frames = -1
        total_dropped = 0
        for shard_sample in profile["profile"]:
            shard = shard_sample["shard_id"]
            samples = shard_sample["samples"]
            total_samples = 0
            total_dropped += shard_sample["dropped_samples"]
            for sample in samples:
                sc = sample["user_backtrace"].count(" ") + 1
                max_frames = max(max_frames, sc)
                total_samples += sample["occurrences"]
                # self.logger.warn(f"{shard} stack had {sc} samples")
            self.logger.info(
                f"Shard {shard} had {len(samples)} unique samples, {total_samples} total, {shard_sample['dropped_samples']} dropped"
            )

        # CPU profiler has a max stack depth of 64 per sample, which we should
        # hit as we set the stress fiber to use that deep of a stack (plus there
        # are some other frames outside of the recursive ones added by the stress
        # tool)
        assert max_frames == 64, f"max samples too low: {max_frames}"
