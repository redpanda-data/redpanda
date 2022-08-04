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

#include "cluster/errc.h"
#include "cluster/types.h"
#include "compat/generator.h"
#include "model/tests/randoms.h"
#include "test_utils/randoms.h"

namespace compat {

template<>
struct instance_generator<cluster::errc> {
    static cluster::errc random() {
        return random_generators::random_choice(
          {cluster::errc::success,
           cluster::errc::notification_wait_timeout,
           cluster::errc::topic_invalid_partitions,
           cluster::errc::topic_invalid_replication_factor,
           cluster::errc::topic_invalid_config,
           cluster::errc::not_leader_controller,
           cluster::errc::topic_already_exists,
           cluster::errc::replication_error,
           cluster::errc::shutting_down,
           cluster::errc::no_leader_controller,
           cluster::errc::join_request_dispatch_error,
           cluster::errc::seed_servers_exhausted,
           cluster::errc::auto_create_topics_exception,
           cluster::errc::timeout,
           cluster::errc::topic_not_exists,
           cluster::errc::invalid_topic_name,
           cluster::errc::partition_not_exists,
           cluster::errc::not_leader,
           cluster::errc::partition_already_exists,
           cluster::errc::waiting_for_recovery,
           cluster::errc::update_in_progress,
           cluster::errc::user_exists,
           cluster::errc::user_does_not_exist,
           cluster::errc::invalid_producer_epoch,
           cluster::errc::sequence_out_of_order,
           cluster::errc::generic_tx_error,
           cluster::errc::node_does_not_exists,
           cluster::errc::invalid_node_operation,
           cluster::errc::invalid_configuration_update,
           cluster::errc::topic_operation_error,
           cluster::errc::no_eligible_allocation_nodes,
           cluster::errc::allocation_error,
           cluster::errc::partition_configuration_revision_not_updated,
           cluster::errc::partition_configuration_in_joint_mode,
           cluster::errc::partition_configuration_leader_config_not_committed,
           cluster::errc::partition_configuration_differs,
           cluster::errc::data_policy_already_exists,
           cluster::errc::data_policy_not_exists,
           cluster::errc::source_topic_not_exists,
           cluster::errc::source_topic_still_in_use,
           cluster::errc::waiting_for_partition_shutdown,
           cluster::errc::error_collecting_health_report,
           cluster::errc::leadership_changed,
           cluster::errc::feature_disabled,
           cluster::errc::invalid_request,
           cluster::errc::no_update_in_progress,
           cluster::errc::unknown_update_interruption_error});
    }

    static std::vector<cluster::errc> limits() { return {}; }
};

template<>
struct instance_generator<cluster::config_status> {
    static cluster::config_status random() {
        return cluster::config_status{
          .node = tests::random_named_int<model::node_id>(),
          .version = tests::random_named_int<cluster::config_version>(),
          .restart = tests::random_bool(),
          .unknown = tests::random_sstrings(),
          .invalid = tests::random_sstrings()};
    }

    static std::vector<cluster::config_status> limits() { return {}; }
};

template<>
struct instance_generator<cluster::cluster_property_kv> {
    static cluster::cluster_property_kv random() {
        return {
          tests::random_named_string<ss::sstring>(),
          tests::random_named_string<ss::sstring>()};
    }

    static std::vector<cluster::cluster_property_kv> limits() {
        return {{"", ""}};
    }
};

template<>
struct instance_generator<cluster::config_update_request> {
    static cluster::config_update_request random() {
        return cluster::config_update_request(
          {.upsert = tests::random_vector([] {
               return instance_generator<
                 cluster::cluster_property_kv>::random();
           }),
           .remove = tests::random_sstrings()});
    }

    static std::vector<cluster::config_update_request> limits() {
        return {{{}, {}}};
    }
};

template<>
struct instance_generator<cluster::config_update_reply> {
    static cluster::config_update_reply random() {
        return cluster::config_update_reply{
          .error = instance_generator<cluster::errc>::random(),
          .latest_version = tests::random_named_int<cluster::config_version>()};
    }

    static std::vector<cluster::config_update_reply> limits() {
        return {
          cluster::config_update_reply{
            .error = cluster::errc::success,
            .latest_version = cluster::config_version(
              std::numeric_limits<cluster::config_version::type>::max())},
          cluster::config_update_reply{
            .error = cluster::errc::success,
            .latest_version = cluster::config_version(
              std::numeric_limits<cluster::config_version::type>::min())}};
    }
};

template<>
struct instance_generator<cluster::hello_request> {
    static cluster::hello_request random() {
        return cluster::hello_request{
          .peer = tests::random_named_int<model::node_id>(),
          .start_time = tests::random_duration_ms()};
    }

    static std::vector<cluster::hello_request> limits() {
        return {
          cluster::hello_request{
            .peer = model::node_id(
              std::numeric_limits<model::node_id::type>::min()),
            .start_time
            = std::numeric_limits<std::chrono::milliseconds>::min()},
          cluster::hello_request{
            .peer = model::node_id(
              std::numeric_limits<model::node_id::type>::max()),
            .start_time
            = std::numeric_limits<std::chrono::milliseconds>::max()}};
    };
};

template<>
struct instance_generator<cluster::hello_reply> {
    static cluster::hello_reply random() {
        return cluster::hello_reply{
          .error = instance_generator<cluster::errc>::random()};
    }

    static std::vector<cluster::hello_reply> limits() { return {}; }
};

template<>
struct instance_generator<cluster::feature_update_action> {
    static cluster::feature_update_action random() {
        using action_t = cluster::feature_update_action::action_t;
        return cluster::feature_update_action{
          .feature_name = tests::random_named_string<ss::sstring>(),
          .action = random_generators::random_choice(
            {action_t::complete_preparing,
             action_t::activate,
             action_t::deactivate})};
    }

    static std::vector<cluster::feature_update_action> limits() { return {}; }
};

template<>
struct instance_generator<cluster::feature_action_request> {
    static cluster::feature_action_request random() {
        return cluster::feature_action_request{
          .action
          = instance_generator<cluster::feature_update_action>::random()};
    }

    static std::vector<cluster::feature_action_request> limits() { return {}; }
};

template<>
struct instance_generator<cluster::feature_action_response> {
    static cluster::feature_action_response random() {
        return cluster::feature_action_response{
          .error = instance_generator<cluster::errc>::random()};
    }

    static std::vector<cluster::feature_action_response> limits() { return {}; }
};

template<>
struct instance_generator<cluster::feature_barrier_request> {
    static cluster::feature_barrier_request random() {
        return cluster::feature_barrier_request{
          .tag = tests::random_named_string<cluster::feature_barrier_tag>(),
          .peer = tests::random_named_int<model::node_id>(),
          .entered = tests::random_bool()};
    }

    static std::vector<cluster::feature_barrier_request> limits() { return {}; }
};

template<>
struct instance_generator<cluster::feature_barrier_response> {
    static cluster::feature_barrier_response random() {
        return cluster::feature_barrier_response{
          .entered = tests::random_bool(), .complete = tests::random_bool()};
    }

    static std::vector<cluster::feature_barrier_response> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::join_request> {
    static cluster::join_request random() {
        return cluster::join_request{model::random_broker()};
    }

    static std::vector<cluster::join_request> limits() { return {}; }
};

template<>
struct instance_generator<cluster::join_reply> {
    static cluster::join_reply random() {
        return cluster::join_reply{tests::random_bool()};
    }

    static std::vector<cluster::join_reply> limits() { return {}; }
};

template<>
struct instance_generator<cluster::join_node_request> {
    static cluster::join_node_request random() {
        return cluster::join_node_request{
          tests::random_named_int<cluster::cluster_version>(),
          tests::random_vector(
            [] { return random_generators::get_int<uint8_t>(); }),
          model::random_broker()};
    }

    static std::vector<cluster::join_node_request> limits() { return {}; }
};

template<>
struct instance_generator<cluster::join_node_reply> {
    static cluster::join_node_reply random() {
        return cluster::join_node_reply{
          tests::random_bool(), tests::random_named_int<model::node_id>()};
    }

    static std::vector<cluster::join_node_reply> limits() { return {}; }
};

template<>
struct instance_generator<cluster::decommission_node_request> {
    static cluster::decommission_node_request random() {
        return {.id = tests::random_named_int<model::node_id>()};
    }

    static std::vector<cluster::decommission_node_request> limits() {
        return {};
    }
};

} // namespace compat
