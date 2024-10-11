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
#include "compat/model_generator.h"
#include "model/tests/randoms.h"
#include "random/generators.h"
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
           cluster::errc::unknown_update_interruption_error,
           cluster::errc::topic_invalid_partitions_core_limit,
           cluster::errc::topic_invalid_partitions_memory_limit,
           cluster::errc::topic_invalid_partitions_fd_limit,
           cluster::errc::topic_invalid_partitions_decreased});
    }

    static std::vector<cluster::errc> limits() { return {}; }
};

template<>
struct instance_generator<cluster::partition_move_direction> {
    static cluster::partition_move_direction random() {
        return random_generators::random_choice(
          {cluster::partition_move_direction::to_node,
           cluster::partition_move_direction::from_node,
           cluster::partition_move_direction::all});
    }

    static std::vector<cluster::errc> limits() { return {}; }
};

template<>
struct instance_generator<cluster::move_cancellation_result> {
    static cluster::move_cancellation_result random() {
        return {
          model::random_ntp(),
          instance_generator<cluster::errc>::random(),
        };
    }

    static std::vector<cluster::errc> limits() { return {}; }
};

template<>
struct instance_generator<cluster::topic_result> {
    static cluster::topic_result random() {
        return cluster::topic_result(
          model::random_topic_namespace(),
          instance_generator<cluster::errc>::random());
    }

    static std::vector<cluster::topic_result> limits() { return {}; }
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
struct instance_generator<cluster::config_status_request> {
    static cluster::config_status_request random() {
        return {.status = instance_generator<cluster::config_status>::random()};
    }

    static std::vector<cluster::config_status_request> limits() { return {}; }
};

template<>
struct instance_generator<cluster::config_status_reply> {
    static cluster::config_status_reply random() {
        return {.error = instance_generator<cluster::errc>::random()};
    }

    static std::vector<cluster::config_status_reply> limits() { return {}; }
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
struct instance_generator<cluster::join_node_request> {
    static cluster::join_node_request random() {
        return cluster::join_node_request{
          tests::random_named_int<cluster::cluster_version>(),
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
          tests::random_bool() ? cluster::join_node_reply::status_code::success
                               : cluster::join_node_reply::status_code::error,
          tests::random_named_int<model::node_id>()};
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

template<>
struct instance_generator<cluster::decommission_node_reply> {
    static cluster::decommission_node_reply random() {
        return {.error = instance_generator<cluster::errc>::random()};
    }

    static std::vector<cluster::decommission_node_reply> limits() { return {}; }
};

template<>
struct instance_generator<cluster::recommission_node_request> {
    static cluster::recommission_node_request random() {
        return {.id = tests::random_named_int<model::node_id>()};
    }

    static std::vector<cluster::recommission_node_request> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::recommission_node_reply> {
    static cluster::recommission_node_reply random() {
        return {.error = instance_generator<cluster::errc>::random()};
    }

    static std::vector<cluster::recommission_node_reply> limits() { return {}; }
};

template<>
struct instance_generator<cluster::finish_reallocation_request> {
    static cluster::finish_reallocation_request random() {
        return {.id = tests::random_named_int<model::node_id>()};
    }

    static std::vector<cluster::finish_reallocation_request> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::finish_reallocation_reply> {
    static cluster::finish_reallocation_reply random() {
        return {.error = instance_generator<cluster::errc>::random()};
    }

    static std::vector<cluster::finish_reallocation_reply> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::set_maintenance_mode_request> {
    static cluster::set_maintenance_mode_request random() {
        return {
          .id = tests::random_named_int<model::node_id>(),
          .enabled = tests::random_bool()};
    }

    static std::vector<cluster::set_maintenance_mode_request> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::set_maintenance_mode_reply> {
    static cluster::set_maintenance_mode_reply random() {
        return {.error = instance_generator<cluster::errc>::random()};
    }

    static std::vector<cluster::set_maintenance_mode_reply> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::reconciliation_state_request> {
    static cluster::reconciliation_state_request random() {
        auto f = []() { return model::random_ntp(); };
        return {.ntps = tests::random_vector(std::move(f))};
    }

    static std::vector<cluster::reconciliation_state_request> limits() {
        return {{}};
    }
};

template<>
struct instance_generator<cluster::finish_partition_update_request> {
    static cluster::finish_partition_update_request random() {
        return {
          .ntp = model::random_ntp(),
          .new_replica_set = tests::random_vector(
            [] { return model::random_broker_shard(); }),
        };
    }

    static std::vector<cluster::finish_partition_update_request> limits() {
        return {{.ntp = model::random_ntp(), .new_replica_set = {}}};
    }
};

template<>
struct instance_generator<cluster::finish_partition_update_reply> {
    static cluster::finish_partition_update_reply random() {
        return {
          .result = instance_generator<cluster::errc>::random(),
        };
    }

    static std::vector<cluster::finish_partition_update_reply> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::configuration_update_request> {
    static cluster::configuration_update_request random() {
        return cluster::configuration_update_request(
          model::random_broker(), tests::random_named_int<model::node_id>());
    }

    static std::vector<cluster::configuration_update_request> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::cancel_node_partition_movements_request> {
    static cluster::cancel_node_partition_movements_request random() {
        return {
          .node_id = tests::random_named_int<model::node_id>(),
          .direction
          = instance_generator<cluster::partition_move_direction>::random(),
        };
    }

    static std::vector<cluster::cancel_node_partition_movements_request>
    limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::configuration_update_reply> {
    static cluster::configuration_update_reply random() {
        return cluster::configuration_update_reply(tests::random_bool());
    }

    static std::vector<cluster::configuration_update_reply> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::cancel_partition_movements_reply> {
    static cluster::cancel_partition_movements_reply random() {
        return {
          .general_error = instance_generator<cluster::errc>::random(),
          .partition_results = tests::random_vector([] {
              return instance_generator<
                cluster::move_cancellation_result>::random();
          }),
        };
    }

    static std::vector<cluster::cancel_partition_movements_reply> limits() {
        return {{
          .general_error = instance_generator<cluster::errc>::random(),
          .partition_results = {},
        }};
    }
};

EMPTY_COMPAT_GENERATOR(cluster::cancel_all_partition_movements_request);

template<>
struct instance_generator<cluster::partition_assignment> {
    static cluster::partition_assignment random() {
        return {
          tests::random_named_int<raft::group_id>(),
          tests::random_named_int<model::partition_id>(),
          tests::random_vector([] { return model::random_broker_shard(); })};
    }

    static std::vector<cluster::partition_assignment> limits() { return {{}}; }
};

template<>
struct instance_generator<cluster::backend_operation> {
    static cluster::backend_operation random() {
        return {
          .source_shard = random_generators::get_int<unsigned>(),
          .p_as = instance_generator<cluster::partition_assignment>::random(),
          .type = random_generators::random_choice(
            {cluster::partition_operation_type::add,
             cluster::partition_operation_type::add_non_replicable,
             cluster::partition_operation_type::del_non_replicable,
             cluster::partition_operation_type::cancel_update,
             cluster::partition_operation_type::force_cancel_update,
             cluster::partition_operation_type::update,
             cluster::partition_operation_type::finish_update,
             cluster::partition_operation_type::update_properties,
             cluster::partition_operation_type::remove}),
        };
    }

    static std::vector<cluster::backend_operation> limits() { return {{}}; }
};

template<>
struct instance_generator<cluster::ntp_reconciliation_state> {
    static cluster::ntp_reconciliation_state random() {
        return {
          model::random_ntp(),
          tests::random_chunked_fifo([] {
              return instance_generator<cluster::backend_operation>::random();
          }),
          random_generators::random_choice(
            {cluster::reconciliation_status::done,
             cluster::reconciliation_status::error,
             cluster::reconciliation_status::in_progress}),
          instance_generator<cluster::errc>::random()};
    }

    static std::vector<cluster::ntp_reconciliation_state> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::reconciliation_state_reply> {
    static cluster::reconciliation_state_reply random() {
        return {
          .results = tests::random_vector([] {
              return instance_generator<
                cluster::ntp_reconciliation_state>::random();
          }),
        };
    }

    static std::vector<cluster::reconciliation_state_reply> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::remote_topic_properties> {
    static cluster::remote_topic_properties random() {
        return cluster::remote_topic_properties(
          tests::random_named_int<model::initial_revision_id>(),
          random_generators::get_int<int32_t>());
    }

    static std::vector<cluster::remote_topic_properties> limits() { return {}; }
};

template<>
struct instance_generator<cluster::topic_properties> {
    static xid random_xid() {
        auto data = random_generators::get_bytes(12);
        xid::data_t array;
        std::copy(std::begin(data), std::end(data), array.begin());

        return xid{array};
    }

    static cluster::topic_properties random() {
        return {
          tests::random_optional(
            [] { return instance_generator<model::compression>::random(); }),
          tests::random_optional([] {
              return instance_generator<
                model::cleanup_policy_bitflags>::random();
          }),
          tests::random_optional([] {
              return instance_generator<model::compaction_strategy>::random();
          }),
          tests::random_optional(
            [] { return instance_generator<model::timestamp_type>::random(); }),
          tests::random_optional(
            [] { return random_generators::get_int<size_t>(); }),
          tests::random_tristate(
            [] { return random_generators::get_int<size_t>(); }),
          tests::random_tristate([] { return tests::random_duration_ms(); }),
          tests::random_optional([] { return tests::random_bool(); }),
          tests::random_optional(
            [] { return model::shadow_indexing_mode::archival; }),
          tests::random_optional([] { return tests::random_bool(); }),
          tests::random_optional(
            [] { return tests::random_named_string<ss::sstring>(); }),
          model::random_topic_namespace(),
          instance_generator<cluster::remote_topic_properties>::random(),
          tests::random_optional(
            [] { return random_generators::get_int<uint32_t>(1024 * 1024); }),
          tests::random_tristate(
            [] { return random_generators::get_int<size_t>(); }),
          tests::random_tristate([] { return tests::random_duration_ms(); }),
          // Remote delete always false to enable ADL roundtrip (ADL
          // always decodes to false for legacy topics)
          false,
          tests::random_tristate([] { return tests::random_duration_ms(); }),
          std::nullopt,
          std::nullopt,
          std::nullopt,
          std::nullopt,
          std::nullopt,
          std::nullopt,
          std::nullopt,
          std::nullopt,
          tests::random_tristate(
            [] { return random_generators::get_int<size_t>(); }),
          tests::random_tristate([] { return tests::random_duration_ms(); }),
          tests::random_optional(
            [] { return model::vcluster_id(random_xid()); }),
          tests::random_optional([] {
              return random_generators::random_choice(
                {model::write_caching_mode::default_true,
                 model::write_caching_mode::default_false,
                 model::write_caching_mode::disabled});
          }),
          tests::random_optional([] { return tests::random_duration_ms(); }),
          tests::random_optional(
            [] { return random_generators::get_int<size_t>(); }),
          false,
          std::nullopt,
          false};
    }

    static std::vector<cluster::topic_properties> limits() { return {}; }
};

template<>
struct instance_generator<cluster::topic_configuration> {
    static cluster::topic_configuration random() {
        cluster::topic_configuration tc;
        tc.tp_ns = model::random_topic_namespace();
        tc.partition_count = random_generators::get_int<int32_t>();
        tc.replication_factor = random_generators::get_int<int16_t>();
        tc.is_migrated = tests::random_bool();
        tc.properties = instance_generator<cluster::topic_properties>::random();
        return tc;
    }

    static std::vector<cluster::topic_configuration> limits() {
        return {
          {model::ns(""),
           model::topic(""),
           std::numeric_limits<int32_t>::max(),
           std::numeric_limits<int16_t>::max(),
           std::numeric_limits<bool>::max()},
          {model::ns(""),
           model::topic(""),
           std::numeric_limits<int32_t>::min(),
           std::numeric_limits<int16_t>::min(),
           std::numeric_limits<bool>::min()}};
    }
};

template<>
struct instance_generator<cluster::create_topics_request> {
    static cluster::create_topics_request random() {
        return {
          .topics = tests::random_chunked_vector(
            [] {
                return instance_generator<
                  cluster::topic_configuration>::random();
            },
            5),
          .timeout = tests::random_duration_ms()};
    }

    static std::vector<cluster::create_topics_request> limits() { return {}; }
};

template<>
struct instance_generator<cluster::create_topics_reply> {
    static cluster::create_topics_reply random() {
        return {
          tests::random_vector(
            [] { return instance_generator<cluster::topic_result>::random(); }),
          tests::random_vector(
            [] { return instance_generator<model::topic_metadata>::random(); }),
          tests::random_chunked_vector([] {
              return instance_generator<cluster::topic_configuration>::random();
          })};
    }

    static std::vector<cluster::create_topics_reply> limits() { return {}; }
};

template<>
struct instance_generator<v8_engine::data_policy> {
    static v8_engine::data_policy random() {
        return {
          tests::random_named_string<ss::sstring>(),
          tests::random_named_string<ss::sstring>()};
    }

    static std::vector<v8_engine::data_policy> limits() { return {}; }
};

template<typename Func>
auto random_property_update(Func f) {
    using T = decltype(f());
    return tests::random_bool()
             ? cluster::property_update<T>()
             : cluster::property_update<T>(
                 f(),
                 random_generators::random_choice(
                   {cluster::incremental_update_operation::none,
                    cluster::incremental_update_operation::set,
                    cluster::incremental_update_operation::remove}));
}

template<>
struct instance_generator<cluster::incremental_topic_custom_updates> {
    static cluster::incremental_topic_custom_updates random() {
        return {
          .data_policy = random_property_update([] {
              return tests::random_optional([] {
                  return instance_generator<v8_engine::data_policy>::random();
              });
          })};
    }

    static std::vector<cluster::incremental_topic_custom_updates> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::incremental_topic_updates> {
    static cluster::incremental_topic_updates random() {
        cluster::incremental_topic_updates updates;
        updates.compression = random_property_update([] {
            return tests::random_optional(
              [] { return instance_generator<model::compression>::random(); });
        });
        updates.cleanup_policy_bitflags = random_property_update([] {
            return tests::random_optional([] {
                return instance_generator<
                  model::cleanup_policy_bitflags>::random();
            });
        });
        updates.compaction_strategy = random_property_update([] {
            return tests::random_optional([] {
                return instance_generator<model::compaction_strategy>::random();
            });
        });
        updates.timestamp_type = random_property_update([] {
            return tests::random_optional([] {
                return instance_generator<model::timestamp_type>::random();
            });
        });
        updates.segment_size = random_property_update([] {
            return tests::random_optional(
              [] { return random_generators::get_int<size_t>(); });
        });
        updates.retention_bytes = random_property_update([] {
            return tests::random_tristate(
              [] { return random_generators::get_int<size_t>(); });
        });
        updates.retention_duration = random_property_update([] {
            return tests::random_tristate(
              [] { return tests::random_duration_ms(); });
        });
        updates.get_shadow_indexing() = random_property_update([] {
            return tests::random_optional([] {
                return instance_generator<
                  model::shadow_indexing_mode>::random();
            });
        });
        updates.remote_delete = random_property_update([] {
            // Enable ADL roundtrip, which always decodes as false
            // for legacy topics
            return false;
        });

        return updates;
    }

    static std::vector<cluster::incremental_topic_updates> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::topic_properties_update> {
    static cluster::topic_properties_update random() {
        return {
          model::random_topic_namespace(),
          instance_generator<cluster::incremental_topic_updates>::random(),
          instance_generator<
            cluster::incremental_topic_custom_updates>::random()};
    }

    static std::vector<cluster::topic_properties_update> limits() { return {}; }
};

template<>
struct instance_generator<cluster::update_topic_properties_request> {
    static cluster::update_topic_properties_request random() {
        return {.updates = tests::random_chunked_vector([] {
                    return instance_generator<
                      cluster::topic_properties_update>::random();
                })};
    }

    static std::vector<cluster::update_topic_properties_request> limits() {
        return {};
    }
};

template<>
struct instance_generator<cluster::update_topic_properties_reply> {
    static cluster::update_topic_properties_reply random() {
        return {.results = tests::random_vector([] {
                    return instance_generator<cluster::topic_result>::random();
                })};
    }

    static std::vector<cluster::update_topic_properties_reply> limits() {
        return {};
    }
};

} // namespace compat
