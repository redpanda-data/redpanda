/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "kafka/server/group_data_parser.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/group_stm.h"
#include "model/fundamental.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <absl/container/node_hash_map.h>

namespace kafka {

struct group_recovery_consumer_state {
    absl::node_hash_map<kafka::group_id, group_stm> groups;
    /*
     * recovered committed offsets are by default non-reclaimable, and marked as
     * reclaimable if this flag is set to true. the flag is set to true if and
     * when the fence is observed during recovery. after recovery, if the fence
     * has not been observed, then the fence will be written once the offset
     * retention feature is activated. see group::offset_metadata for more info.
     */
    bool has_offset_retention_feature_fence{false};
    model::offset last_read_offset;
};

class group_recovery_consumer
  : public group_data_parser<group_recovery_consumer> {
    using base_t = group_data_parser<group_recovery_consumer>;

public:
    /*
     * This batch consumer is used during partition recovery to read, index, and
     * deduplicate both group and commit metadata snapshots.
     */

    explicit group_recovery_consumer(
      group_metadata_serializer serializer, ss::abort_source& as)
      : _serializer(std::move(serializer))
      , _as(as) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch batch);

    group_recovery_consumer_state end_of_stream() { return std::move(_state); }

    ss::future<> handle_raft_data(model::record_batch);
    ss::future<> handle_tx_offsets(
      model::record_batch_header, kafka::group_tx::offsets_metadata);
    ss::future<> handle_fence_v0(
      model::record_batch_header, kafka::group_tx::fence_metadata_v0);
    ss::future<> handle_fence_v1(
      model::record_batch_header, kafka::group_tx::fence_metadata_v1);
    ss::future<>
      handle_fence(model::record_batch_header, kafka::group_tx::fence_metadata);
    ss::future<>
      handle_abort(model::record_batch_header, kafka::group_tx::abort_metadata);
    ss::future<> handle_commit(
      model::record_batch_header, kafka::group_tx::commit_metadata);
    ss::future<> handle_version_fence(features::feature_table::version_fence);

private:
    void handle_record(model::record);
    void handle_group_metadata(group_metadata_kv);
    void handle_offset_metadata(offset_metadata_kv);
    group_recovery_consumer_state _state;
    model::offset _batch_base_offset;
    group_metadata_serializer _serializer;
    ss::abort_source& _as;
};
} // namespace kafka
