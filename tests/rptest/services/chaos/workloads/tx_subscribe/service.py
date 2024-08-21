# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass

from ..service_base import WorkloadServiceBase
from . import consistency, stat


class TxSubscribeWorkload(WorkloadServiceBase):
    @dataclass
    class Setup:
        # source topic
        source: str
        source_partitions: int
        # target topic
        target: str
        # consumer group id
        group_id: str

    def __init__(self,
                 ctx,
                 brokers_str,
                 setup: Setup,
                 fail_consistency_on_interruption=False,
                 retries=5):
        super().__init__(ctx, brokers_str, num_nodes=2)
        self._setup = setup
        self._fail_consistency_on_interruption = fail_consistency_on_interruption
        self._retries = retries

    @property
    def java_module_name(self):
        return "tx_subscribe"

    def extra_config(self, node):
        return {
            "idx": self.nodes.index(node),
            "source": self._setup.source,
            "partitions": self._setup.source_partitions,
            "target": self._setup.target,
            "group_id": self._setup.group_id,
            "settings": {
                "retries": self._retries,
            }
        }

    def validate_consistency(self):
        consistency.validate(
            self._results_dir(), [n.name for n in self.nodes],
            fail_on_interruption=self._fail_consistency_on_interruption)

    def collect_stats(self):
        return stat.collect(self.context.test_id,
                            self._results_dir(), [n.name for n in self.nodes],
                            source_partitions=self._setup.source_partitions)
