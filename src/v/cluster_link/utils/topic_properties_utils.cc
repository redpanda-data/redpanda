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

namespace cluster_link::utils {

bool parse_and_set_optional_bool_alpha(
  cluster::property_update<std::optional<bool>>& property,
  const std::optional<ss::sstring>& value,
  const std::optional<bool>& current_value) {
    // set property value if preset, otherwise do nothing
    if (value) {
        try {
            bool v = string_switch<bool>(*value)
                       .match("true", true)
                       .match("false", false);
            if (v != current_value) {
                property.op = cluster::incremental_update_operation::set;
                property.value = v;
                return true;
            }
        } catch (const std::runtime_error&) {
            // Our callers expect this exception type on malformed values
            throw boost::bad_lexical_cast();
        }
    }
    return false;
}

bool maybe_append_update(
  cluster::topic_properties_update& update,
  const ss::sstring& config_name,
  const ss::sstring& config_value,
  const cluster::topic_configuration& topic_config) {
    if (config_name == kafka::topic_property_cleanup_policy) {
        return parse_and_set_optional(
          update.properties.cleanup_policy_bitflags,
          config_value,
          topic_config.properties.cleanup_policy_bitflags);
    }
    if (config_name == kafka::topic_property_compaction_strategy) {
        return parse_and_set_optional(
          update.properties.compaction_strategy,
          config_value,
          topic_config.properties.compaction_strategy);
    }
    if (config_name == kafka::topic_property_compression) {
        return parse_and_set_optional(
          update.properties.compression,
          config_value,
          topic_config.properties.compression);
    }
    if (config_name == kafka::topic_property_segment_size) {
        return parse_and_set_optional(
          update.properties.segment_size,
          config_value,
          topic_config.properties.segment_size,
          kafka::segment_size_validator{});
    }
    if (config_name == kafka::topic_property_timestamp_type) {
        return parse_and_set_optional(
          update.properties.timestamp_type,
          config_value,
          topic_config.properties.timestamp_type);
    }
    if (config_name == kafka::topic_property_retention_bytes) {
        return parse_and_set_tristate(
          update.properties.retention_bytes,
          config_value,
          topic_config.properties.retention_bytes);
    }
    if (config_name == kafka::topic_property_retention_duration) {
        return parse_and_set_tristate(
          update.properties.retention_duration,
          config_value,
          topic_config.properties.retention_duration);
    }
    if (config_name == kafka::topic_property_retention_local_target_bytes) {
        return parse_and_set_tristate(
          update.properties.retention_local_target_bytes,
          config_value,
          topic_config.properties.retention_local_target_bytes);
    }
    if (config_name == kafka::topic_property_retention_local_target_ms) {
        return parse_and_set_tristate(
          update.properties.retention_local_target_ms,
          config_value,
          topic_config.properties.retention_local_target_ms);
    }
    if (config_name == kafka::topic_property_remote_read) {
        return parse_and_set_bool(
          topic_config.tp_ns,
          update.properties.remote_read,
          config_value,
          topic_config.properties.shadow_indexing.has_value()
            ? ::model::is_fetch_enabled(
                topic_config.properties.shadow_indexing.value())
            : false);
    }
    if (config_name == kafka::topic_property_remote_write) {
        return parse_and_set_bool(
          topic_config.tp_ns,
          update.properties.remote_write,
          config_value,
          topic_config.properties.shadow_indexing.has_value()
            ? ::model::is_fetch_enabled(
                topic_config.properties.shadow_indexing.value())
            : false);
    }
    if (config_name == kafka::topic_property_remote_delete) {
        return parse_and_set_bool(
          topic_config.tp_ns,
          update.properties.remote_delete,
          config_value,
          topic_config.properties.remote_delete);
    }
    if (config_name == kafka::topic_property_max_message_bytes) {
        return parse_and_set_optional(
          update.properties.batch_max_bytes,
          config_value,
          topic_config.properties.batch_max_bytes);
    }
    if (config_name == kafka::topic_property_segment_ms) {
        return parse_and_set_tristate(
          update.properties.segment_ms,
          config_value,
          topic_config.properties.segment_ms);
    }
    if (
      config_name
      == kafka::topic_property_initial_retention_local_target_bytes) {
        return parse_and_set_tristate(
          update.properties.initial_retention_local_target_bytes,
          config_value,
          topic_config.properties.initial_retention_local_target_bytes);
    }
    if (
      config_name == kafka::topic_property_initial_retention_local_target_ms) {
        return parse_and_set_tristate(
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
    if (
      std::find(
        std::begin(kafka::allowlist_topic_noop_confs),
        std::end(kafka::allowlist_topic_noop_confs),
        config_name)
      != std::end(kafka::allowlist_topic_noop_confs)) {
        return false;
    }
    if (config_name == kafka::topic_property_write_caching) {
        return parse_and_set_optional(
          update.properties.write_caching,
          config_value,
          topic_config.properties.write_caching,
          kafka::write_caching_config_validator{});
    }
    if (config_name == kafka::topic_property_flush_ms) {
        return parse_and_set_optional_duration(
          update.properties.flush_ms,
          config_value,
          topic_config.properties.flush_ms,
          kafka::flush_ms_validator);
    }
    if (config_name == kafka::topic_property_flush_bytes) {
        return parse_and_set_optional(
          update.properties.flush_bytes,
          config_value,
          topic_config.properties.flush_bytes,
          kafka::flush_bytes_validator{});
    }
    if (config_name == kafka::topic_property_iceberg_mode) {
        return parse_and_set_property(
          topic_config.tp_ns,
          update.properties.iceberg_mode,
          config_value,
          topic_config.properties.iceberg_mode,
          kafka::iceberg_config_validator{});
    }
    if (config_name == kafka::topic_property_leaders_preference) {
        return parse_and_set_optional(
          update.properties.leaders_preference,
          config_value,
          topic_config.properties.leaders_preference,
          noop_validator<config::leaders_preference>{},
          config::leaders_preference::parse);
    }
    if (config_name == kafka::topic_property_cloud_topic_enabled) {
        if (config::shard_local_cfg().development_enable_cloud_topics()) {
            throw kafka::validation_error(
              "Cloud topics property cannot be changed");
        }
        throw kafka::validation_error("Cloud topics is not enabled");
    }
    if (config_name == kafka::topic_property_delete_retention_ms) {
        return parse_and_set_tristate(
          update.properties.delete_retention_ms,
          config_value,
          topic_config.properties.delete_retention_ms);
    }
    if (config_name == kafka::topic_property_iceberg_delete) {
        return parse_and_set_optional_bool_alpha(
          update.properties.iceberg_delete,
          config_value,
          topic_config.properties.iceberg_delete);
    }
    if (config_name == kafka::topic_property_iceberg_partition_spec) {
        // Use std::identity as the "parser function" (i.e. pass through
        // the raw string) because boost::lexical_cast<ss::sstring> (the
        // default) doesn't allow spaces in the config value.
        return parse_and_set_optional(
          update.properties.iceberg_partition_spec,
          config_value,
          topic_config.properties.iceberg_partition_spec,
          kafka::iceberg_partition_spec_validator{},
          std::identity{});
    }
    if (config_name == kafka::topic_property_iceberg_invalid_record_action) {
        return parse_and_set_optional(
          update.properties.iceberg_invalid_record_action,
          config_value,
          topic_config.properties.iceberg_invalid_record_action);
    }
    if (config_name == kafka::topic_property_remote_allow_gaps) {
        return parse_and_set_optional_bool_alpha(
          update.properties.remote_allow_gaps,
          config_value,
          topic_config.properties.remote_topic_allow_gaps);
    }
    if (config_name == kafka::topic_property_iceberg_target_lag_ms) {
        return parse_and_set_optional_duration(
          update.properties.iceberg_target_lag_ms,
          config_value,
          topic_config.properties.iceberg_target_lag_ms,
          kafka::iceberg_target_lag_ms_validator);
    }

    if (config_name == kafka::topic_property_min_cleanable_dirty_ratio) {
        return parse_and_set_tristate(
          update.properties.min_cleanable_dirty_ratio,
          config_value,
          topic_config.properties.min_cleanable_dirty_ratio,
          kafka::min_cleanable_dirty_ratio_validator{});
    }

    if (config_name == kafka::topic_property_min_compaction_lag_ms) {
        return parse_and_set_optional_duration(
          update.properties.min_compaction_lag_ms,
          config_value,
          topic_config.properties.min_compaction_lag_ms,
          kafka::min_compaction_lag_ms_validator);
    }

    if (config_name == kafka::topic_property_max_compaction_lag_ms) {
        return parse_and_set_optional_duration(
          update.properties.max_compaction_lag_ms,
          config_value,
          topic_config.properties.max_compaction_lag_ms,
          kafka::max_compaction_lag_ms_validator);
    }

    return false;
}
} // namespace cluster_link::utils
