/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/protocol/describe_log_dirs.h"
#include "kafka/server/handlers/handler.h"

#include <seastar/core/future.hh>

namespace kafka {

using describe_log_dirs_handler
  = single_stage_handler<describe_log_dirs_api, 0, 2>;

namespace describe_log_dirs::detail {

struct log_partition_data {
    describe_log_dirs_partition local;
    std::optional<describe_log_dirs_partition> remote;
};

using partition_dir_set
  = chunked_hash_map<model::topic, chunked_vector<log_partition_data>>;

ss::future<log_partition_data>
describe_partition(kafka::partition_proxy& p, bool include_remote);

/// Merges per-shard partition_dir_sets produced by collect_mapper into a
/// single accumulator. Topics present in update have their partitions
/// appended to acc[topic]; new topics are inserted. Used as the reducer
/// in the map_reduce0 fan-out across shards.
partition_dir_set merge_partition_dir_sets(
  partition_dir_set acc, const partition_dir_set& update);

} // namespace describe_log_dirs::detail

} // namespace kafka
