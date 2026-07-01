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

#include "cluster_link/utils/topic_properties_utils.h"

#include "base/type_traits.h"
#include "features/feature_table.h"
#include "kafka/server/handlers/configs/config_utils.h"
#include "pandaproxy/schema_registry/types.h"
#include "reflection/type_traits.h"

namespace {

template<typename T, typename Validator = kafka::noop_validator<T>>
bool parse_and_set(
  model::topic_namespace_view tn,
  cluster::property_update<T>& prop,
  const std::optional<ss::sstring>& raw_value,
  const T& curr_value,
  Validator validator = kafka::noop_validator<T>{}) {
    if constexpr (std::is_same_v<T, bool>) {
        kafka::parse_and_set_bool(
          tn,
          prop,
          raw_value,
          kafka::config_resource_operation::set,
          false /* ignored */);
    } else if constexpr (reflection::is_tristate<T>) {
        kafka::parse_and_set_tristate(
          prop, raw_value, kafka::config_resource_operation::set, validator);
    } else if constexpr (reflection::is_std_optional<T>) {
        using ValT = typename T::value_type;
        if constexpr (std::is_same_v<ValT, bool>) {
            kafka::parse_and_set_optional_bool_alpha(
              prop, raw_value, kafka::config_resource_operation::set);
        } else if constexpr (detail::is_duration_v<ValT>) {
            kafka::parse_and_set_optional_duration(
              prop,
              raw_value,
              kafka::config_resource_operation::set,
              validator,
              true);
        } else if constexpr (std::is_same_v<ValT, ss::sstring>) {
            kafka::parse_and_set_optional(
              prop,
              raw_value,
              kafka::config_resource_operation::set,
              validator,
              std::identity{});
        } else {
            kafka::parse_and_set_optional(
              prop,
              raw_value,
              kafka::config_resource_operation::set,
              validator);
        }
    } else {
        kafka::parse_and_set_property(
          tn,
          prop,
          raw_value,
          kafka::config_resource_operation::set,
          validator);
    }
    return prop.value != curr_value;
}

} // namespace

namespace cluster_link::utils {
bool maybe_append_update(
  cluster::topic_properties_update& update,
  const ss::sstring& config_name,
  const ss::sstring& config_value,
  const cluster::topic_configuration& topic_config) {
    if (config_name == kafka::topic_property_cleanup_policy) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.cleanup_policy_bitflags,
          config_value,
          topic_config.properties.cleanup_policy_bitflags);
    }
    if (config_name == kafka::topic_property_compaction_strategy) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.compaction_strategy,
          config_value,
          topic_config.properties.compaction_strategy);
    }
    if (config_name == kafka::topic_property_compression) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.compression,
          config_value,
          topic_config.properties.compression);
    }
    if (config_name == kafka::topic_property_segment_size) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.segment_size,
          config_value,
          topic_config.properties.segment_size,
          kafka::segment_size_validator{});
    }
    if (config_name == kafka::topic_property_timestamp_type) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.timestamp_type,
          config_value,
          topic_config.properties.timestamp_type);
    }
    if (config_name == kafka::topic_property_retention_bytes) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.retention_bytes,
          config_value,
          topic_config.properties.retention_bytes);
    }
    if (config_name == kafka::topic_property_retention_duration) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.retention_duration,
          config_value,
          topic_config.properties.retention_duration);
    }
    if (config_name == kafka::topic_property_retention_local_target_bytes) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.retention_local_target_bytes,
          config_value,
          topic_config.properties.retention_local_target_bytes);
    }
    if (config_name == kafka::topic_property_retention_local_target_ms) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.retention_local_target_ms,
          config_value,
          topic_config.properties.retention_local_target_ms);
    }
    if (config_name == kafka::topic_property_remote_read) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.remote_read,
          config_value,
          topic_config.properties.is_remote_fetch_enabled());
    }
    if (config_name == kafka::topic_property_remote_write) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.remote_write,
          config_value,
          topic_config.properties.is_archival_enabled());
    }
    if (config_name == kafka::topic_property_remote_delete) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.remote_delete,
          config_value,
          topic_config.properties.remote_delete);
    }
    if (config_name == kafka::topic_property_max_message_bytes) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.batch_max_bytes,
          config_value,
          topic_config.properties.batch_max_bytes);
    }
    if (config_name == kafka::topic_property_segment_ms) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.segment_ms,
          config_value,
          topic_config.properties.segment_ms);
    }
    if (
      config_name
      == kafka::topic_property_initial_retention_local_target_bytes) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.initial_retention_local_target_bytes,
          config_value,
          topic_config.properties.initial_retention_local_target_bytes);
    }
    if (
      config_name == kafka::topic_property_initial_retention_local_target_ms) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.initial_retention_local_target_ms,
          config_value,
          topic_config.properties.initial_retention_local_target_ms);
    }
    /// XXX: Not sure if we want to support mirroring schema ID validation
    // if (
    //   config::shard_local_cfg().enable_schema_id_validation()
    //   != pandaproxy::schema_registry::schema_id_validation_mode::none) {
    //     if (schema_id_validation_config_parser(cfg)) {
    //     }
    // }
    if (config_name == kafka::topic_property_write_caching) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.write_caching,
          config_value,
          topic_config.properties.write_caching,
          kafka::write_caching_config_validator{});
    }
    if (config_name == kafka::topic_property_flush_ms) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.flush_ms,
          config_value,
          topic_config.properties.flush_ms,
          kafka::flush_ms_validator);
    }
    if (config_name == kafka::topic_property_flush_bytes) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.flush_bytes,
          config_value,
          topic_config.properties.flush_bytes,
          kafka::flush_bytes_validator{});
    }
    if (config_name == kafka::topic_property_iceberg_mode) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.iceberg_mode,
          config_value,
          topic_config.properties.iceberg_mode,
          kafka::iceberg_config_validator{});
    }
    if (config_name == kafka::topic_property_leaders_preference) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.leaders_preference,
          config_value,
          topic_config.properties.leaders_preference,
          kafka::noop_validator<config::leaders_preference>{});
    }
    if (config_name == kafka::topic_property_delete_retention_ms) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.delete_retention_ms,
          config_value,
          topic_config.properties.delete_retention_ms);
    }
    if (config_name == kafka::topic_property_iceberg_delete) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.iceberg_delete,
          config_value,
          topic_config.properties.iceberg_delete);
    }
    if (config_name == kafka::topic_property_iceberg_partition_spec) {
        // Use std::identity as the "parser function" (i.e. pass through
        // the raw string) because boost::lexical_cast<ss::sstring> (the
        // default) doesn't allow spaces in the config value.
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.iceberg_partition_spec,
          config_value,
          topic_config.properties.iceberg_partition_spec,
          kafka::iceberg_partition_spec_validator{});
    }
    if (config_name == kafka::topic_property_iceberg_invalid_record_action) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.iceberg_invalid_record_action,
          config_value,
          topic_config.properties.iceberg_invalid_record_action);
    }
    if (config_name == kafka::topic_property_remote_allow_gaps) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.remote_allow_gaps,
          config_value,
          topic_config.properties.remote_topic_allow_gaps);
    }
    if (config_name == kafka::topic_property_iceberg_target_lag_ms) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.iceberg_target_lag_ms,
          config_value,
          topic_config.properties.iceberg_target_lag_ms,
          kafka::iceberg_target_lag_ms_validator);
    }
    if (config_name == kafka::topic_property_schema_registry_context) {
        // context is named_type<ss::sstring>, not ss::sstring, so we need an
        // explicit parse function.
        kafka::parse_and_set_property(
          topic_config.tp_ns,
          update.properties.schema_registry_context,
          config_value,
          kafka::config_resource_operation::set,
          kafka::schema_registry_context_validator{},
          [](const ss::sstring& s) {
              return pandaproxy::schema_registry::context{s};
          });
        return update.properties.schema_registry_context.value
               != topic_config.properties.schema_registry_context;
    }

    if (config_name == kafka::topic_property_min_cleanable_dirty_ratio) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.min_cleanable_dirty_ratio,
          config_value,
          topic_config.properties.min_cleanable_dirty_ratio,
          kafka::min_cleanable_dirty_ratio_validator{});
    }

    if (config_name == kafka::topic_property_min_compaction_lag_ms) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.min_compaction_lag_ms,
          config_value,
          topic_config.properties.min_compaction_lag_ms,
          kafka::min_compaction_lag_ms_validator);
    }

    if (config_name == kafka::topic_property_max_compaction_lag_ms) {
        return parse_and_set(
          topic_config.tp_ns,
          update.properties.max_compaction_lag_ms,
          config_value,
          topic_config.properties.max_compaction_lag_ms,
          kafka::max_compaction_lag_ms_validator);
    }

    return false;
}

bool maybe_append_storage_mode_update(
  cluster::topic_properties_update& update,
  const std::optional<ss::sstring>& mode_value,
  const std::optional<ss::sstring>& impl_value,
  const cluster::topic_configuration& topic_config,
  const features::feature_table& feature_table) {
    std::optional<::model::redpanda_storage_mode> mode;
    if (impl_value.has_value()) {
        // The impl property is exact and always present on sources that
        // emit it, so it takes precedence over the ambiguous mode value.
        mode = ::model::redpanda_storage_mode_from_impl_string(*impl_value);
        if (!mode.has_value()) {
            throw kafka::validation_error(
              fmt::format("Unrecognized storage mode impl '{}'", *impl_value));
        }
    } else if (mode_value.has_value()) {
        // Sources that predate the impl property sync only the mode, where
        // 'tiered' meant the classic tiered storage.
        mode = ::model::redpanda_storage_mode_from_string(*mode_value);
        if (!mode.has_value()) {
            throw kafka::validation_error(
              fmt::format("Unrecognized storage mode '{}'", *mode_value));
        }
    } else {
        return false;
    }
    auto current = topic_config.properties.storage_mode;
    if (*mode == current) {
        return false;
    }
    if (
      *mode == ::model::redpanda_storage_mode::tiered_cloud
      && !feature_table.is_active(features::feature::tiered_cloud_topics)) {
        throw kafka::validation_error(
          "Cannot use the tiered_v2 storage mode until the cluster is fully "
          "upgraded to at least v26.2.1");
    }
    if (!kafka::is_storage_mode_transition_permitted(
          current, *mode, /*migration_enabled=*/false)) {
        throw kafka::validation_error(
          fmt::format(
            "Storage mode transition from {} to {} is not permitted",
            current,
            *mode));
    }
    update.properties.storage_mode.op
      = cluster::incremental_update_operation::set;
    update.properties.storage_mode.value = *mode;
    return true;
}
} // namespace cluster_link::utils
