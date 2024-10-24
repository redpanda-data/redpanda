# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

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
