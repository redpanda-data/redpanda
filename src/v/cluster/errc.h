/*
 * Copyright 2020 Vectorized, Inc.
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

namespace cluster {

enum class errc : int16_t {
    success = 0, // must be 0
    notification_wait_timeout,
    topic_invalid_partitions,
    topic_invalid_replication_factor,
    topic_invalid_config,
    not_leader_controller,
    topic_already_exists,
    replication_error,
    shutting_down,
    no_leader_controller,
    join_request_dispatch_error,
    seed_servers_exhausted,
    auto_create_topics_exception,
    timeout,
    topic_not_exists,
    invalid_topic_name,
    partition_not_exists,
    not_leader,
    partition_already_exists,
    waiting_for_recovery,
    update_in_progress,
    user_exists,
    user_does_not_exist,
    invalid_producer_epoch,
    sequence_out_of_order,
    generic_tx_error,
    node_does_not_exists,
    invalid_node_opeartion,
    topic_operation_error,

};
struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "cluster::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "Success";
        case errc::notification_wait_timeout:
            return "Timeout waiting for append entries notification comming "
                   "from raft 0";
        case errc::topic_invalid_partitions:
            return "Unable to assign topic partitions to current cluster "
                   "resources";
        case errc::topic_invalid_replication_factor:
            return "Unable to allocate topic with given replication factor";
        case errc::topic_invalid_config:
            return "Topic configuration is either bogus or not supported";
        case errc::not_leader_controller:
            return "This node is not raft-0 leader. i.e is not leader "
                   "controller";
        case errc::topic_already_exists:
            return "The topic has already been created";
        case errc::replication_error:
            return "Controller was unable to replicate given state across "
                   "cluster nodes";
        case errc::shutting_down:
            return "Application is shutting down";
        case errc::no_leader_controller:
            return "Currently there is no leader controller elected in the "
                   "cluster";
        case errc::join_request_dispatch_error:
            return "Error occurred when controller tried to join the cluster";
        case errc::seed_servers_exhausted:
            return "There are no seed servers left to try in this round, will "
                   "retry after delay ";
        case errc::auto_create_topics_exception:
            return "An exception was thrown while auto creating topics";
        case errc::timeout:
            return "Timeout occurred while processing request";
        case errc::topic_not_exists:
            return "Topic does not exists";
        case errc::invalid_topic_name:
            return "Invalid topic name";
        case errc::partition_not_exists:
            return "Requested partition does not exists";
        case errc::not_leader:
            return "Current node is not a leader for partition";
        case errc::partition_already_exists:
            return "Requested partition already exists";
        case errc::waiting_for_recovery:
            return "Waiting for partition to recover";
        case errc::update_in_progress:
            return "Partition configuration update in progress";
        case errc::user_exists:
            return "User already exists";
        case errc::user_does_not_exist:
            return "User does not exist";
        case errc::invalid_producer_epoch:
            return "Invalid idempotent producer epoch";
        case errc::sequence_out_of_order:
            return "Producer sequence ID out of order";
        case errc::generic_tx_error:
            return "Generic error when processing transactional requests";
        case errc::node_does_not_exists:
            return "Requested node does not exists";
        case errc::invalid_node_opeartion:
            return "Requested node opeartion is invalid";
        case errc::topic_operation_error:
            return "Unable to perform requested topic operation ";
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
} // namespace cluster
namespace std {
template<>
struct is_error_code_enum<cluster::errc> : true_type {};
} // namespace std
