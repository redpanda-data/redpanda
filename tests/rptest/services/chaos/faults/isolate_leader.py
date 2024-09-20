# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.admin import Admin
from rptest.services.failure_injector import FailureInjector

from .fault import RecoverableFault


class IsolateLeaderFault(RecoverableFault):
    def __init__(self,
                 redpanda,
                 topic: str,
                 partition: int = 0,
                 namespace: str = "kafka",
                 name: str = "isolate_leader"):
        self._name = name
        self.redpanda = redpanda
        self.topic = topic
        self.partition = partition
        self.namespace = namespace

        self._leader = None

    @property
    def name(self):
        return self._name

    def inject(self):
        leader_id = Admin(self.redpanda).await_stable_leader(
            self.topic, partition=self.partition, namespace=self.namespace)
        self._leader = self.redpanda.get_node_by_id(leader_id)
        FailureInjector(self.redpanda)._isolate(self._leader)

    def heal(self):
        FailureInjector(self.redpanda)._heal(self._leader)
