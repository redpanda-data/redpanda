/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/admin/services/datalake/datalake.h"

#include "container/chunked_hash_map.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/types.h"

namespace {
proto::admin::data_file to_proto(const datalake::coordinator::data_file& df) {
    proto::admin::data_file pb_file;
    pb_file.set_remote_path(ss::sstring(df.remote_path));
    pb_file.set_row_count(df.row_count);
    pb_file.set_file_size_bytes(df.file_size_bytes);
    pb_file.set_table_schema_id(df.table_schema_id);
    pb_file.set_partition_spec_id(df.partition_spec_id);
    chunked_vector<iobuf> pb_partition_key;
    for (const auto& key : df.partition_key) {
        if (key.has_value()) {
            auto key_buf = bytes_to_iobuf(key.value());
            pb_partition_key.emplace_back(std::move(key_buf));
        } else {
            pb_partition_key.emplace_back(iobuf{});
        }
    }
    pb_file.set_partition_key(std::move(pb_partition_key));
    return pb_file;
}

proto::admin::translated_offset_range
to_proto(const datalake::coordinator::translated_offset_range& range) {
    proto::admin::translated_offset_range pb_range;
    pb_range.set_start_offset(range.start_offset());
    pb_range.set_last_offset(range.last_offset());
    chunked_vector<proto::admin::data_file> pb_data_files;
    for (const auto& file : range.files) {
        pb_data_files.emplace_back(to_proto(file));
    }
    pb_range.set_data_files(std::move(pb_data_files));
    chunked_vector<proto::admin::data_file> pb_dlq_files;
    for (const auto& dlq_file : range.dlq_files) {
        pb_dlq_files.emplace_back(to_proto(dlq_file));
    }
    pb_range.set_dlq_files(std::move(pb_dlq_files));
    pb_range.set_kafka_processed_bytes(range.kafka_bytes_processed);
    return pb_range;
}

proto::admin::pending_entry
to_proto(const datalake::coordinator::pending_entry& entry) {
    proto::admin::pending_entry pb_entry;
    pb_entry.set_data(to_proto(entry.data));
    pb_entry.set_added_pending_at(entry.added_pending_at());
    return pb_entry;
}

proto::admin::partition_state
to_proto(const datalake::coordinator::partition_state& state) {
    proto::admin::partition_state pb_state;
    chunked_vector<proto::admin::pending_entry> pb_entries;
    for (const auto& entry : state.pending_entries) {
        pb_entries.emplace_back(to_proto(entry));
    }
    pb_state.set_pending_entries(std::move(pb_entries));
    if (state.last_committed.has_value()) {
        pb_state.set_last_committed(state.last_committed.value()());
    }
    return pb_state;
}

proto::admin::lifecycle_state
to_proto(datalake::coordinator::topic_state::lifecycle_state_t state) {
    switch (state) {
    case datalake::coordinator::topic_state::lifecycle_state_t::live:
        return proto::admin::lifecycle_state::live;
    case datalake::coordinator::topic_state::lifecycle_state_t::closed:
        return proto::admin::lifecycle_state::closed;
    case datalake::coordinator::topic_state::lifecycle_state_t::purged:
        return proto::admin::lifecycle_state::purged;
    }
    return proto::admin::lifecycle_state::unspecified;
}

proto::admin::topic_state
to_proto(const datalake::coordinator::topic_state& state) {
    proto::admin::topic_state pb_state;
    pb_state.set_revision(state.revision());
    chunked_hash_map<int32_t, proto::admin::partition_state>
      pb_partition_states;
    for (const auto& [pid, pstate] : state.pid_to_pending_files) {
        pb_partition_states.emplace(pid(), to_proto(pstate));
    }
    pb_state.set_partition_states(std::move(pb_partition_states));
    pb_state.set_lifecycle_state(to_proto(state.lifecycle_state));
    pb_state.set_total_kafka_processed_bytes(state.total_kafka_bytes_processed);
    pb_state.set_last_committed_snapshot_id(
      state.last_committed_snapshot_id.value_or(iceberg::invalid_snapshot_id));
    return pb_state;
}

} // anonymous namespace

namespace admin {

datalake_service_impl::datalake_service_impl(
  admin::proxy::client proxy_client,
  ss::sharded<datalake::coordinator::frontend>* coordinator_fe)
  : _proxy_client(std::move(proxy_client))
  , _coordinator_fe(coordinator_fe) {}

ss::future<proto::admin::get_coordinator_state_response>
datalake_service_impl::get_coordinator_state(
  serde::pb::rpc::context, proto::admin::get_coordinator_state_request req) {
    if (!_coordinator_fe->local_is_initialized()) {
        throw serde::pb::rpc::unavailable_exception(
          "Datalake coordinator frontend not initialized");
    }

    // Group topics by coordinator partition.
    chunked_hash_map<model::partition_id, chunked_vector<model::topic>>
      topics_filter_by_partition;
    if (req.get_topics_filter().empty()) {
        auto partition_count_opt
          = _coordinator_fe->local().coordinator_partition_count();
        if (!partition_count_opt.has_value()) {
            throw serde::pb::rpc::unavailable_exception(
              fmt::format(
                "Datalake coordinator couldn't get coordinator partition "
                "count"));
        }
        // There is no topics filter, make a request for every partition.
        for (auto p = 0; p < partition_count_opt.value(); ++p) {
            topics_filter_by_partition.emplace(
              model::partition_id{p}, chunked_vector<model::topic>());
        }
    } else {
        for (const auto& topic_name : req.get_topics_filter()) {
            model::topic topic{topic_name};
            auto partition_opt = _coordinator_fe->local().coordinator_partition(
              topic);
            if (!partition_opt.has_value()) {
                throw serde::pb::rpc::unavailable_exception(
                  fmt::format(
                    "Datalake coordinator couldn't get coordinator partition "
                    "for {}",
                    topic));
            }
            topics_filter_by_partition[partition_opt.value()].emplace_back(
              std::move(topic));
        }
    }

    // Send out the RPCs.
    chunked_hash_map<model::topic, datalake::coordinator::topic_state>
      topic_states;
    for (auto& [partition_id, topics_filter] : topics_filter_by_partition) {
        datalake::coordinator::get_topic_state_request fe_req{
          partition_id, std::move(topics_filter)};
        auto fe_res = co_await _coordinator_fe->local().get_topic_state(
          std::move(fe_req));

        if (fe_res.errc != datalake::coordinator::errc::ok) {
            throw serde::pb::rpc::internal_exception(
              fmt::format(
                "Datalake coordinator error for partition {}: {}",
                partition_id,
                fe_res.errc));
        }

        for (auto& [topic, state] : fe_res.topic_states) {
            topic_states.insert({topic, std::move(state)});
        }
    }

    // Convert to protobuf response.
    proto::admin::get_coordinator_state_response response;
    chunked_hash_map<ss::sstring, proto::admin::topic_state> pb_topic_states;
    for (const auto& [topic, state] : topic_states) {
        pb_topic_states.emplace(topic(), to_proto(state));
    }
    proto::admin::coordinator_state state;
    state.set_topic_states(std::move(pb_topic_states));
    response.set_state(std::move(state));

    co_return response;
}

} // namespace admin
