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
        return compat_binary::serde_and_adl(obj);
    }

    static bool check(raft::timeout_now_request obj, compat_binary test) {
        return verify_adl_or_serde(obj, std::move(test));
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
        auto result = json::read_member_enum(rd, "result", obj.result);
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
        return compat_binary::serde_and_adl(obj);
    }

    static bool check(raft::timeout_now_reply obj, compat_binary test) {
        return verify_adl_or_serde(obj, std::move(test));
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
    }

    static raft::transfer_leadership_request from_json(json::Value& rd) {
        raft::transfer_leadership_request obj;
        json_read(group);
        json_read(target);
        return obj;
    }

    static std::vector<compat_binary>
    to_binary(raft::transfer_leadership_request obj) {
        return compat_binary::serde_and_adl(obj);
    }

    static bool
    check(raft::transfer_leadership_request obj, compat_binary test) {
        return verify_adl_or_serde(obj, std::move(test));
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
        auto result = json::read_member_enum(rd, "result", obj.result);
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
        return compat_binary::serde_and_adl(obj);
    }

    static bool check(raft::transfer_leadership_reply obj, compat_binary test) {
        return verify_adl_or_serde(obj, std::move(test));
    }
};
} // namespace compat
