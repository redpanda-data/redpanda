# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json

from collections import ChainMap
from enum import Enum
from dataclasses import dataclass
from typing import Optional
from functools import reduce


class BatchType(str, Enum):
    raft_data = "batch_type::raft_data"
    raft_configuration = "batch_type::raft_configuration"
    controller = "batch_type::controller"
    kvstore = "batch_type::kvstore"
    checkpoint = "batch_type::checkpoint"
    topic_management_cmd = "batch_type::topic_management_cmd"
    ghost_batch = "batch_type::ghost_batch"
    id_allocator = "batch_type::id_allocator"
    tx_prepare = "batch_type::tx_prepare"
    tx_fence = "batch_type::tx_fence"
    tm_update = "batch_type::tm_update"
    user_management_cmd = "batch_type::user_management_cmd"
    acl_management_cmd = "batch_type::acl_management_cmd"
    group_prepare_tx = "batch_type::group_prepare_tx"
    group_commit_tx = "batch_type::group_commit_tx"
    group_abort_tx = "batch_type::group_abort_tx"
    node_management_cmd = "batch_type::node_management_cmd"
    data_policy_management_cmd = "batch_type::data_policy_management_cmd"
    archival_metadata = "batch_type::archival_metadata"
    cluster_config_cmd = "batch_type::cluster_config_cmd"
    feature_update = "batch_type::feature_update"
    cluster_bootstrap_cmd = "batch_type::cluster_boostrap_cmd"
    version_fence = "batch_type::version_fence"
    tx_tm_hosted_trasactions = "batch_type::tx_tm_hosted_trasactions"
    prefix_truncate = "batch_type::prefix_truncate"


class Operation(str, Enum):
    write = "write"
    falloc = "falloc"
    flush = "flush"
    truncate = "truncate"
    close = "close"


@dataclass
class NTP:
    topic: str
    partition: int
    namespace: str = "kafka"


@dataclass
class FailureConfig:
    operation: Operation
    batch_type: Optional[BatchType] = None
    failure_probability: Optional[float] = None
    delay_probability: Optional[float] = None
    min_delay_ms: Optional[int] = None
    max_delay_ms: Optional[int] = None

    def to_dict(self):
        op_config = dict()
        if self.batch_type:
            op_config["batch_type"] = self.batch_type.value
        if self.failure_probability:
            op_config["failure_probability"] = self.failure_probability
        if self.delay_probability:
            op_config["delay_probability"] = self.delay_probability
        if self.min_delay_ms:
            op_config["min_delay_ms"] = self.min_delay_ms
        if self.max_delay_ms:
            op_config["max_delay_ms"] = self.max_delay_ms

        return {self.operation.value: op_config}


@dataclass
class NTPFailureInjectionConfig:
    ntp: NTP
    failure_configs: list[FailureConfig]

    def to_dict(self):
        return {
            "namespace":
            self.ntp.namespace,
            "topic":
            self.ntp.topic,
            "partition":
            self.ntp.partition,
            "failure_configs":
            reduce(lambda a, b: {
                **a,
                **b
            }, [cfg.to_dict() for cfg in self.failure_configs])
        }


@dataclass
class FailureInjectionConfig:
    seed: int
    ntp_failure_configs: list[NTPFailureInjectionConfig]

    def to_dict(self):
        return {
            "seed": self.seed,
            "ntps":
            [ntp_cfg.to_dict() for ntp_cfg in self.ntp_failure_configs]
        }

    def write_to_file(self, path):
        with open(path, "w") as f:
            json.dump(self.to_dict(), f)
