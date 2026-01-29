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

#include <system_error>

namespace raft {

enum class errc : int16_t {
    success = 0, // must be 0
    disconnected_endpoint,
    exponential_backoff,
    non_majority_replication,
    not_leader,
    vote_dispatch_error,
    append_entries_dispatch_error,
    replicated_entry_truncated,
    leader_flush_failed,
    leader_append_failed,
    timeout,
    configuration_change_in_progress,
    node_does_not_exists,
    leadership_transfer_in_progress,
    transfer_to_current_leader,
    node_already_exists,
    invalid_configuration_update,
    not_voter,
    invalid_target_node,
    shutting_down,
    replicate_batcher_cache_error,
    group_not_exists,
    replicate_first_stage_exception,
    invalid_input_records,
};
struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "raft::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "raft::errc::success";
        case errc::disconnected_endpoint:
            return "raft::errc::disconnected_endpoint(node down)";
        case errc::exponential_backoff:
            return "raft::errc::exponential_backoff";
        case errc::non_majority_replication:
            return "raft::errc::non_majority_replication";
        case errc::not_leader:
            return "raft::errc::not_leader";
        case errc::vote_dispatch_error:
            return "raft::errc::vote_dispatch_error";
        case errc::append_entries_dispatch_error:
            return "raft::errc::append_entries_dispatch_error";
        case errc::replicated_entry_truncated:
            return "raft::errc::replicated_entry_truncated";
        case errc::leader_flush_failed:
            return "raft::errc::leader_flush_failed";
        case errc::leader_append_failed:
            return "raft::errc::leader_append_failed";
        case errc::timeout:
            return "raft::errc::timeout";
        case errc::configuration_change_in_progress:
            return "raft::errc::configuration_change_in_progress";
        case errc::node_does_not_exists:
            return "Node does not exists in configuration";
        case errc::leadership_transfer_in_progress:
            return "Node is currently transferring leadership";
        case errc::transfer_to_current_leader:
            return "Target for leadership transfer is current leader";
        case errc::node_already_exists:
            return "Node does already exists in configuration";
        case errc::invalid_configuration_update:
            return "Configuration resulting from the update is invalid";
        case errc::not_voter:
            return "Requested node is not a voter";
        case errc::invalid_target_node:
            return "Node that received the request is not the one that the "
                   "request was addressed to";
        case errc::shutting_down:
            return "raft protocol shutting down";
        case errc::replicate_batcher_cache_error:
            return "unable to append batch to replicate batcher cache";
        case errc::group_not_exists:
            return "raft group does not exist on target broker";
        case errc::replicate_first_stage_exception:
            return "unable to finish replicate since exception was thrown in "
                   "first phase";
        case errc::invalid_input_records:
            return "attempt to replicate invalid input records";
        }
        return "cluster::errc::unknown";
    }
};
inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}
} // namespace raft
namespace std {
template<>
struct is_error_code_enum<raft::errc> : true_type {};
} // namespace std
