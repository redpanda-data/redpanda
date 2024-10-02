/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "compat/check.h"
#include "compat/raft_generator.h"
#include "compat/raft_json.h"
#include "raft/types.h"

#include <type_traits>

namespace compat {

/*
 * raft::timeout_now_request
 */
template<>
struct compat_check<raft::timeout_now_request> {
    static constexpr std::string_view name = "raft::timeout_now_request";

    static std::vector<raft::timeout_now_request> create_test_cases() {
        return generate_instances<raft::timeout_now_request>();
    }

    static void to_json(
      raft::timeout_now_request obj, json::Writer<json::StringBuffer>& wr) {
        json_write(target_node_id);
        json_write(node_id);
        json_write(group);
        json_write(term);
    }

    static raft::timeout_now_request from_json(json::Value& rd) {
        raft::timeout_now_request obj;
        json_read(target_node_id);
        json_read(node_id);
        json_read(group);
        json_read(term);
        return obj;
    }

    static std::vector<compat_binary> to_binary(raft::timeout_now_request obj) {
        return {compat_binary::serde(obj)};
    }

    static void check(raft::timeout_now_request obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

/*
 * raft::timeout_now_reply
 */
template<>
struct compat_check<raft::timeout_now_reply> {
    static constexpr std::string_view name = "raft::timeout_now_reply";

    static std::vector<raft::timeout_now_reply> create_test_cases() {
        return generate_instances<raft::timeout_now_reply>();
    }

    static void
    to_json(raft::timeout_now_reply obj, json::Writer<json::StringBuffer>& wr) {
        json_write(target_node_id);
        json_write(term);
        json_write(result);
    }

    static raft::timeout_now_reply from_json(json::Value& rd) {
        raft::timeout_now_reply obj;
        json_read(target_node_id);
        json_read(term);
        auto result = json::read_enum_ut(rd, "result", obj.result);
        switch (result) {
        case 0:
            obj.result = raft::timeout_now_reply::status::success;
            break;
        case 1:
            obj.result = raft::timeout_now_reply::status::failure;
            break;
        default:
            vassert(false, "invalid status: {}", result);
        }
        return obj;
    }

    static std::vector<compat_binary> to_binary(raft::timeout_now_reply obj) {
        return {compat_binary::serde(obj)};
    }

    static void check(raft::timeout_now_reply obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

/*
 * raft::transfer_leadership_request
 */
template<>
struct compat_check<raft::transfer_leadership_request> {
    static constexpr std::string_view name
      = "raft::transfer_leadership_request";

    static std::vector<raft::transfer_leadership_request> create_test_cases() {
        return generate_instances<raft::transfer_leadership_request>();
    }

    static void to_json(
      raft::transfer_leadership_request obj,
      json::Writer<json::StringBuffer>& wr) {
        json_write(group);
        json_write(target);
        json_write(timeout);
    }

    static raft::transfer_leadership_request from_json(json::Value& rd) {
        raft::transfer_leadership_request obj;
        json_read(group);
        json_read(target);
        json_read(timeout);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(raft::transfer_leadership_request obj) {
        return {compat_binary::serde(obj)};
    }

    static void
    check(raft::transfer_leadership_request obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

/*
 * raft::transfer_leadership_reply
 */
template<>
struct compat_check<raft::transfer_leadership_reply> {
    static constexpr std::string_view name = "raft::transfer_leadership_reply";

    static std::vector<raft::transfer_leadership_reply> create_test_cases() {
        return generate_instances<raft::transfer_leadership_reply>();
    }

    static void to_json(
      raft::transfer_leadership_reply obj,
      json::Writer<json::StringBuffer>& wr) {
        json_write(success);
        json_write(result);
    }

    static raft::transfer_leadership_reply from_json(json::Value& rd) {
        raft::transfer_leadership_reply obj;
        json_read(success);
        auto result = json::read_enum_ut(rd, "result", obj.result);
        switch (result) {
        case 0:
            obj.result = raft::errc::success;
            break;
        case 1:
            obj.result = raft::errc::disconnected_endpoint;
            break;
        case 2:
            obj.result = raft::errc::exponential_backoff;
            break;
        case 3:
            obj.result = raft::errc::non_majority_replication;
            break;
        case 4:
            obj.result = raft::errc::not_leader;
            break;
        case 5: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::vote_dispatch_error;
            break;
        case 6: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::append_entries_dispatch_error;
            break;
        case 7: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::replicated_entry_truncated;
            break;
        case 8: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::leader_flush_failed;
            break;
        case 9: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::leader_append_failed;
            break;
        case 10: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::timeout;
            break;
        case 11: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::configuration_change_in_progress;
            break;
        case 12: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::node_does_not_exists;
            break;
        case 13: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::leadership_transfer_in_progress;
            break;
        case 14: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::transfer_to_current_leader;
            break;
        case 15: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::node_already_exists;
            break;
        case 16: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::invalid_configuration_update;
            break;
        case 17: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::not_voter;
            break;
        case 18: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::invalid_target_node;
            break;
        case 19: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::shutting_down;
            break;
        case 20: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::replicate_batcher_cache_error;
            break;
        case 21: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::group_not_exists;
            break;
        case 22: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
            obj.result = raft::errc::replicate_first_stage_exception;
            break;
        default:
            vassert(false, "invalid raft::errc: {}", result);
        }
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(raft::transfer_leadership_reply obj) {
        return {compat_binary::serde(obj)};
    }

    static void check(raft::transfer_leadership_reply obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

/*
 * raft::install_snapshot_request
 */
template<>
struct compat_check<raft::install_snapshot_request> {
    static constexpr std::string_view name = "raft::install_snapshot_request";

    static std::vector<raft::install_snapshot_request> create_test_cases() {
        return generate_instances<raft::install_snapshot_request>();
    }

    static void to_json(
      raft::install_snapshot_request obj,
      json::Writer<json::StringBuffer>& wr) {
        json_write(target_node_id);
        json_write(term);
        json_write(group);
        json_write(node_id);
        json_write(last_included_index);
        json_write(file_offset);
        json_write(chunk);
        json_write(done);
        json_write(dirty_offset);
    }

    static raft::install_snapshot_request from_json(json::Value& rd) {
        raft::install_snapshot_request obj;
        json_read(target_node_id);
        json_read(term);
        json_read(group);
        json_read(node_id);
        json_read(last_included_index);
        json_read(file_offset);
        json_read(chunk);
        json_read(done);
        json_read(dirty_offset);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(raft::install_snapshot_request obj) {
        return {compat_binary::serde(std::move(obj))};
    }

    static void check(raft::install_snapshot_request obj, compat_binary test) {
        verify_serde_only(std::move(obj), std::move(test));
    }
};

template<>
inline std::pair<raft::install_snapshot_request, raft::install_snapshot_request>
compat_copy(raft::install_snapshot_request obj) {
    auto f = [&obj] {
        return raft::install_snapshot_request{
          .target_node_id = obj.target_node_id,
          .term = obj.term,
          .group = obj.group,
          .node_id = obj.node_id,
          .last_included_index = obj.last_included_index,
          .file_offset = obj.file_offset,
          .chunk = obj.chunk.copy(),
          .done = obj.done,
          .dirty_offset = obj.dirty_offset,
        };
    };
    return std::make_pair(f(), f());
}

/*
 * raft::install_snapshot_reply
 */
template<>
struct compat_check<raft::install_snapshot_reply> {
    static constexpr std::string_view name = "raft::install_snapshot_reply";

    static std::vector<raft::install_snapshot_reply> create_test_cases() {
        return generate_instances<raft::install_snapshot_reply>();
    }

    static void to_json(
      raft::install_snapshot_reply obj, json::Writer<json::StringBuffer>& wr) {
        json_write(target_node_id);
        json_write(term);
        json_write(bytes_stored);
        json_write(success);
    }

    static raft::install_snapshot_reply from_json(json::Value& rd) {
        raft::install_snapshot_reply obj;
        json_read(target_node_id);
        json_read(term);
        json_read(bytes_stored);
        json_read(success);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(raft::install_snapshot_reply obj) {
        return {compat_binary::serde(obj)};
    }

    static void check(raft::install_snapshot_reply obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

/*
 * raft::vote_request
 */
template<>
struct compat_check<raft::vote_request> {
    static constexpr std::string_view name = "raft::vote_request";

    static std::vector<raft::vote_request> create_test_cases() {
        return generate_instances<raft::vote_request>();
    }

    static void
    to_json(raft::vote_request obj, json::Writer<json::StringBuffer>& wr) {
        json_write(node_id);
        json_write(target_node_id);
        json_write(group);
        json_write(term);
        json_write(prev_log_index);
        json_write(prev_log_term);
        json_write(leadership_transfer);
    }

    static raft::vote_request from_json(json::Value& rd) {
        raft::vote_request obj;
        json_read(node_id);
        json_read(target_node_id);
        json_read(group);
        json_read(term);
        json_read(prev_log_index);
        json_read(prev_log_term);
        json_read(leadership_transfer);
        return obj;
    }

    static std::vector<compat_binary> to_binary(raft::vote_request obj) {
        return {compat_binary::serde(obj)};
    }

    static void check(raft::vote_request obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

/*
 * raft::vote_reply
 */
template<>
struct compat_check<raft::vote_reply> {
    static constexpr std::string_view name = "raft::vote_reply";

    static std::vector<raft::vote_reply> create_test_cases() {
        return generate_instances<raft::vote_reply>();
    }

    static void
    to_json(raft::vote_reply obj, json::Writer<json::StringBuffer>& wr) {
        json_write(target_node_id);
        json_write(term);
        json_write(granted);
        json_write(log_ok);
    }

    static raft::vote_reply from_json(json::Value& rd) {
        raft::vote_reply obj;
        json_read(target_node_id);
        json_read(term);
        json_read(granted);
        json_read(log_ok);
        return obj;
    }

    static std::vector<compat_binary> to_binary(raft::vote_reply obj) {
        return {compat_binary::serde(obj)};
    }

    static void check(raft::vote_reply obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};

/*
 * raft::heartbeat_request
 */
template<>
struct compat_check<raft::heartbeat_request> {
    static constexpr std::string_view name = "raft::heartbeat_request";

    static std::vector<raft::heartbeat_request> create_test_cases() {
        return generate_instances<raft::heartbeat_request>();
    }

    static void to_json(
      const raft::heartbeat_request& obj,
      json::Writer<json::StringBuffer>& wr) {
        json_write(heartbeats);
    }

    static raft::heartbeat_request from_json(json::Value& rd) {
        raft::heartbeat_request obj;
        json_read(heartbeats);
        return obj;
    }

    static std::vector<compat_binary> to_binary(raft::heartbeat_request obj) {
        return {compat_binary::serde(std::move(obj))};
    }

    static void check(raft::heartbeat_request expected, compat_binary test) {
        verify_serde_only(std::move(expected), std::move(test));
    }
};

/*
 * raft::heartbeat_reply
 */
template<>
struct compat_check<raft::heartbeat_reply> {
    static constexpr std::string_view name = "raft::heartbeat_reply";

    static std::vector<raft::heartbeat_reply> create_test_cases() {
        return generate_instances<raft::heartbeat_reply>();
    }

    static void to_json(
      const raft::heartbeat_reply& obj, json::Writer<json::StringBuffer>& wr) {
        // temporary workaround for serialization of append_entries_reply not
        // needing the caller to start/end the object. will fix with a refactor.
        wr.Key("meta");
        wr.StartArray();
        for (const auto& e : obj.meta) {
            wr.StartObject();
            rjson_serialize(wr, e);
            wr.EndObject();
        }
        wr.EndArray();
    }

    static raft::heartbeat_reply from_json(json::Value& rd) {
        raft::heartbeat_reply obj;
        json_read(meta);
        return obj;
    }

    static std::vector<compat_binary> to_binary(raft::heartbeat_reply obj) {
        return {compat_binary::serde(std::move(obj))};
    }

    static void check(raft::heartbeat_reply expected, compat_binary test) {
        verify_serde_only(std::move(expected), std::move(test));
    }
};

/*
 * reading from the input request's batch reader destroys the data itself. so we
 * need to read out the batches, make a copy, and then reconstruct two copies.
 */
template<>
inline std::pair<raft::append_entries_request, raft::append_entries_request>
compat_copy(raft::append_entries_request r) {
    auto metadata = r.metadata();
    auto target_node = r.target_node();
    auto source_node = r.source_node();
    auto flush = r.is_flush_required();

    auto a_batches = model::consume_reader_to_memory(
                       std::move(r).release_batches(), model::no_timeout)
                       .get();

    ss::circular_buffer<model::record_batch> b_batches;
    for (const auto& batch : a_batches) {
        b_batches.emplace_back(batch.copy());
    }

    raft::append_entries_request a(
      source_node,
      target_node,
      metadata,
      model::make_memory_record_batch_reader(std::move(a_batches)),
      flush);

    raft::append_entries_request b(
      source_node,
      target_node,
      metadata,
      model::make_memory_record_batch_reader(std::move(b_batches)),
      flush);

    return {std::move(a), std::move(b)};
}

/*
 * raft::append_entries_request
 */
template<>
struct compat_check<raft::append_entries_request> {
    static constexpr std::string_view name = "raft::append_entries_request";

    static std::vector<raft::append_entries_request> create_test_cases() {
        return generate_instances<raft::append_entries_request>();
    }

    static void to_json(
      raft::append_entries_request obj, json::Writer<json::StringBuffer>& wr) {
        json::write_member(wr, "node_id", obj.source_node());
        json::write_member(wr, "target_node_id", obj.target_node());
        json::write_member(wr, "meta", obj.metadata());
        json::write_member(wr, "flush", obj.is_flush_required());
        auto batches = model::consume_reader_to_memory(
                         std::move(obj).release_batches(), model::no_timeout)
                         .get();
        json::write_member(wr, "batches", batches);
    }

    static raft::append_entries_request from_json(json::Value& rd) {
        raft::vnode node;
        raft::vnode target;
        raft::protocol_metadata meta;
        raft::flush_after_append flush;
        model::record_batch_reader::data_t batches;

        json::read_member(rd, "node_id", node);
        json::read_member(rd, "target_node_id", target);
        json::read_member(rd, "meta", meta);
        json::read_member(rd, "flush", flush);
        json::read_member(rd, "batches", batches);

        return {
          node,
          target,
          meta,
          model::make_memory_record_batch_reader(std::move(batches)),
          flush};
    }

    static std::vector<compat_binary>
    to_binary(raft::append_entries_request obj) {
        return {compat_binary::serde(std::move(obj))};
    }

    static void
    check(raft::append_entries_request expected, compat_binary test) {
        auto decoded = decode_serde_only<raft::append_entries_request>(
          std::move(test));

        if (decoded.source_node() != expected.source_node()) {
            throw compat_error(fmt::format(
              "Expected node_id {} got {}",
              expected.source_node(),
              decoded.source_node()));
        }

        if (decoded.target_node() != expected.target_node()) {
            throw compat_error(fmt::format(
              "Expected target_node_id {} got {}",
              expected.target_node(),
              decoded.target_node()));
        }

        if (decoded.metadata() != expected.metadata()) {
            throw compat_error(fmt::format(
              "Expected meta {} got {}",
              expected.metadata(),
              decoded.metadata()));
        }

        if (decoded.is_flush_required() != expected.is_flush_required()) {
            throw compat_error(fmt::format(
              "Expected flush {} got {}",
              expected.is_flush_required(),
              decoded.is_flush_required()));
        }

        auto decoded_batches = model::consume_reader_to_memory(
                                 std::move(decoded).release_batches(),
                                 model::no_timeout)
                                 .get();

        auto expected_batches = model::consume_reader_to_memory(
                                  std::move(expected).release_batches(),
                                  model::no_timeout)
                                  .get();

        if (decoded_batches.size() != expected_batches.size()) {
            throw compat_error(fmt::format(
              "Expected {} batches got {}",
              expected_batches.size(),
              decoded_batches.size()));
        }

        vassert(
          decoded_batches.size() > 0,
          "Invalid batches size for test {}",
          decoded_batches.size());

        for (size_t i = 0; i < decoded_batches.size(); i++) {
            if (decoded_batches[i] != expected_batches[i]) {
                throw compat_error(fmt::format(
                  "Expected batch {}\n decoded batch {}",
                  expected_batches[i],
                  decoded_batches[i]));
            }
        }
    }
};

/*
 * raft::append_entries_reply
 */
template<>
struct compat_check<raft::append_entries_reply> {
    static constexpr std::string_view name = "raft::append_entries_reply";

    static std::vector<raft::append_entries_reply> create_test_cases() {
        return generate_instances<raft::append_entries_reply>();
    }

    static void to_json(
      raft::append_entries_reply obj, json::Writer<json::StringBuffer>& wr) {
        rjson_serialize(wr, obj);
    }

    static raft::append_entries_reply from_json(json::Value& rd) {
        raft::append_entries_reply obj;
        read_value(rd, obj);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(raft::append_entries_reply obj) {
        return {compat_binary::serde(obj)};
    }

    static void check(raft::append_entries_reply obj, compat_binary test) {
        verify_serde_only(obj, std::move(test));
    }
};
} // namespace compat
