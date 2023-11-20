# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.constraints import DEFAULT_SEGMENT_SIZE_CONSTRAINT, DEFAULT_SEGMENT_MS_CONSTRAINT
from rptest.services.admin import Admin
from rptest.services.workload_protocol import PWorkload

PROP_NAME = 'log_segment_ms_min'
PROP_NEW_VALUE = 9999999


class SetLogSegmentMsMinConfig(PWorkload):
    def __init__(self, ctx) -> None:
        self.ctx = ctx
        self.admin = Admin(self.ctx.redpanda)

    def get_earliest_applicable_release(self):
        # version before updating log_segment_ms_min from 60s to 10min
        return (23, 2)

    def begin(self):
        # set log_segment_ms_min, to check later that this value did not update to something else

        default_value = self.admin.get_cluster_config()[PROP_NAME]
        assert default_value != PROP_NEW_VALUE, f"sanity check failed: we want to set a different value than the default for easier debugging {default_value=} {PROP_NEW_VALUE=}"

        self.admin.patch_cluster_config(
            upsert={PROP_NAME: str(PROP_NEW_VALUE)})

    def on_cluster_upgraded(self, version: tuple[int, int, int]) -> int:
        prop_val = self.admin.get_cluster_config()[PROP_NAME]
        assert prop_val == PROP_NEW_VALUE, f"{PROP_NAME} did not maintain the value {PROP_NEW_VALUE} that was set before upgrading. {version=}"
        return PWorkload.DONE


class DefaultConfigConstraints(PWorkload):
    def __init__(self, ctx) -> None:
        self.ctx = ctx
        self.admin = Admin(self.ctx.redpanda)

    def _check_segment_clamps(self, res: dict, expected_segment_ms_min: int):
        segment_size_min = res['log_segment_size_min']
        expected_segment_size_min = DEFAULT_SEGMENT_SIZE_CONSTRAINT['min']
        assert segment_size_min == expected_segment_size_min, f'expected log_segment_size_min={expected_segment_size_min}, {segment_size_min=}'
        segment_size_max = res['log_segment_size_max']
        assert segment_size_max == None, f'expected undefined log_segment_size_max, {segment_size_max=}'
        segment_ms_min = res['log_segment_ms_min']
        assert segment_ms_min == expected_segment_ms_min, f'expected log_segment_ms_min={expected_segment_ms_min}, {segment_ms_min=}'
        segment_ms_max = res['log_segment_ms_max']
        expected_segment_ms_max = DEFAULT_SEGMENT_MS_CONSTRAINT['max']
        assert segment_ms_max == expected_segment_ms_max, f'expected log_segment_ms_max={expected_segment_ms_max}, {segment_ms_max=}'

    def get_earliest_applicable_release(self):
        # version before configuration constraint support
        return (23, 2)

    def begin(self):
        # Before cluster upgrade, there is no constraints config and the legacy clamps exist

        res = self.admin.get_cluster_config()
        self._check_segment_clamps(res, expected_segment_ms_min=60000)
        assert 'constraints' not in res, 'expected constraints config not found'

    def on_cluster_upgraded(self, version: tuple[int, int, int]) -> int:
        # After cluster upgrade, expect constraints as a cluster config and it is pre-populated with
        # legacy clamps for segment size and lifetime. NOTE: log_segment_ms_min was updated to 10min for 23.3.x
        # legacy clamps are deprected but still in use.
        res = self.admin.get_cluster_config()
        self._check_segment_clamps(
            res, expected_segment_ms_min=DEFAULT_SEGMENT_MS_CONSTRAINT['min'])
        assert 'constraints' in res, 'expected to find constraints config'
        assert type(res['constraints']
                    ) == list, 'expected constraint config to be a list'
        assert len(res['constraints']) == 0, 'expected empty constraints list'

        return PWorkload.DONE
