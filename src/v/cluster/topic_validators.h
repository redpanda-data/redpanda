/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"

#include <concepts>
#include <optional>

namespace cluster {

struct schema_id_validation_validator {
    static constexpr const char* error_message
      = "Mismatch between redpanda.* and confluent.* schema id validation "
        "properties ";
    static constexpr errc ec = errc::topic_invalid_config;

    template<typename T>
    static bool
    compatible(const std::optional<T>& lhs, const std::optional<T>& rhs) {
        // If both are specified, they must match
        return !lhs || !rhs || lhs == rhs;
    }

    static bool is_valid(cluster::topic_properties& p) {
        return compatible(
                 p.record_key_schema_id_validation,
                 p.record_key_schema_id_validation_compat)
               && compatible(
                 p.record_key_subject_name_strategy,
                 p.record_key_subject_name_strategy_compat)
               && compatible(
                 p.record_value_schema_id_validation,
                 p.record_value_schema_id_validation_compat)
               && compatible(
                 p.record_value_subject_name_strategy,
                 p.record_value_subject_name_strategy_compat);
    }
};

template<typename T>
concept TopicPropertyValidator = requires(const topic_properties& p) {
    { T::is_valid(p) } -> std::same_as<bool>;
    { T::ec } -> std::convertible_to<const errc&>;
    { T::error_message } -> std::convertible_to<const char*>;
};

template<typename... Ts>
struct topic_property_validator_list {};

template<typename... Validators>
requires(TopicPropertyValidator<Validators> && ...)
using make_topic_property_validators
  = topic_property_validator_list<Validators...>;

struct validation_failure {
    errc ec;
    const char* error_message;
};

/// Runs each validator in declaration order against `p`. Returns the first
/// failure or std::nullopt if all validators pass.
template<typename... Validators>
std::optional<validation_failure> validate_topic_properties(
  const topic_properties& p, topic_property_validator_list<Validators...>) {
    std::optional<validation_failure> fail;
    (
      [&] {
          if (!fail.has_value() && !Validators::is_valid(p)) {
              fail = validation_failure{
                Validators::ec, Validators::error_message};
          }
      }(),
      ...);
    return fail;
}

/// Returns true if the topic's cloud-storage-related properties (tiered
/// storage, shadow indexing, recovery, read replica) are compatible with the
/// local node's cloud_storage_enabled setting.
struct cloud_storage_supported_validator {
    static constexpr const char* error_message
      = "Topic requires cloud storage but cloud_storage_enabled is disabled "
        "on this node";
    static constexpr errc ec = errc::feature_disabled;

    static bool is_valid(const cluster::topic_properties& p) {
        const bool requires_cloud_storage = p.is_archival_enabled()
                                            || p.is_remote_fetch_enabled();
        return !requires_cloud_storage
               || config::shard_local_cfg().cloud_storage_enabled();
    }
};

/// Returns true if the topic's iceberg_mode is compatible with the local
/// node's iceberg_enabled setting.
struct iceberg_supported_validator {
    static constexpr const char* error_message
      = "Topic requires iceberg but iceberg_enabled is disabled on this node";
    static constexpr errc ec = errc::feature_disabled;

    static bool is_valid(const cluster::topic_properties& p) {
        return p.iceberg_mode == model::iceberg_mode::disabled
               || config::shard_local_cfg().iceberg_enabled();
    }
};

/// Returns true if the topic's storage_mode (cloud or tiered_cloud) is
/// compatible with the local node's cloud_topics_enabled setting.
struct cloud_topics_supported_validator {
    static constexpr const char* error_message
      = "Topic requires cloud topics but cloud_topics_enabled is disabled on "
        "this node";
    static constexpr errc ec = errc::feature_disabled;

    static bool is_valid(const cluster::topic_properties& p) {
        return !p.is_cloud_topic()
               || config::shard_local_cfg().cloud_topics_enabled();
    }
};

/// Validators a node runs on each partition replica it is asked to host. If
/// any fails, the partition is not created on this node and the reconciliation
/// loop will retry, healing automatically once the local config is fixed.
using node_can_host_partition_validators = make_topic_property_validators<
  cloud_storage_supported_validator,
  iceberg_supported_validator,
  cloud_topics_supported_validator>;

} // namespace cluster
