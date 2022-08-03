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

} // namespace compat
