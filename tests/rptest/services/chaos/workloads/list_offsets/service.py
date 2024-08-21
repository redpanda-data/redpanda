# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ..service_base import WorkloadServiceBase
from . import consistency, stat


class ListOffsetsWorkload(WorkloadServiceBase):
    def __init__(self, ctx, brokers_str, topic, concurrency=2):
        super().__init__(ctx, brokers_str, num_nodes=1)
        self._topic = topic
        self._concurrency = concurrency

    @property
    def java_module_name(self):
        return "list_offsets"

    def extra_config(self, node):
        return {
            "topic": self._topic,
            "settings": {
                "concurrency": self._concurrency,
            }
        }

    def validate_consistency(self):
        consistency.validate(
            self._results_dir(),
            [n.name for n in self.nodes],
            brokers_str=self._brokers_str,
            topic=self._topic,
        )

    def collect_stats(self):
        return stat.collect(self.context.test_id, self._results_dir(),
                            [n.name for n in self.nodes])
