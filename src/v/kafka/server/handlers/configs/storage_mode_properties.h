/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/protocol/topic_properties.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/metadata.h"

#include <seastar/core/sstring.hh>

#include <array>
#include <string_view>

namespace kafka {

// Bitmask for storage modes a property is valid for
enum class storage_mode_mask : uint8_t {
    none = 0,
    local = 1 << 0,
    tiered = 1 << 1,
    cloud = 1 << 2,
    tiered_cloud = 1 << 3,
    unset = 1 << 4,
    // Common combinations
    local_tiered = local | tiered,
    tiered_and_cloud = tiered | cloud,
    cloud_family = cloud | tiered_cloud,
    all = local | tiered | cloud | tiered_cloud | unset,
};

constexpr storage_mode_mask
operator|(storage_mode_mask a, storage_mode_mask b) {
    return static_cast<storage_mode_mask>(
      static_cast<uint8_t>(a) | static_cast<uint8_t>(b));
}

constexpr storage_mode_mask
operator&(storage_mode_mask a, storage_mode_mask b) {
    return static_cast<storage_mode_mask>(
      static_cast<uint8_t>(a) & static_cast<uint8_t>(b));
}

// Entry mapping a property name to its valid storage modes
struct property_storage_mode_entry {
    std::string_view property_name;
    storage_mode_mask valid_modes;
};

// Complete mapping of all topic properties to their valid storage modes.
// Every topic property must be listed here.
inline constexpr auto storage_mode_properties
  = std::to_array<property_storage_mode_entry>({
    // Properties valid for all storage modes
    {topic_property_compression, storage_mode_mask::all},
    {topic_property_cleanup_policy, storage_mode_mask::all},
    {topic_property_retention_duration, storage_mode_mask::all},
    {topic_property_retention_bytes, storage_mode_mask::all},
    {topic_property_timestamp_type, storage_mode_mask::all},
    {topic_property_max_message_bytes, storage_mode_mask::all},
    {topic_property_delete_retention_ms, storage_mode_mask::all},
    {topic_property_min_cleanable_dirty_ratio, storage_mode_mask::all},
    {topic_property_min_compaction_lag_ms, storage_mode_mask::all},
    {topic_property_max_compaction_lag_ms, storage_mode_mask::all},
    {topic_property_message_timestamp_before_max_ms, storage_mode_mask::all},
    {topic_property_message_timestamp_after_max_ms, storage_mode_mask::all},
    {topic_property_leaders_preference, storage_mode_mask::all},
    {topic_property_mpx_virtual_cluster_id, storage_mode_mask::all},
    {topic_property_recovery, storage_mode_mask::all},
    {topic_property_compaction_strategy, storage_mode_mask::all},
    {topic_property_redpanda_storage_mode, storage_mode_mask::all},
    // Schema validation properties - valid for all modes
    {topic_property_record_key_schema_id_validation, storage_mode_mask::all},
    {topic_property_record_key_subject_name_strategy, storage_mode_mask::all},
    {topic_property_record_value_schema_id_validation, storage_mode_mask::all},
    {topic_property_record_value_subject_name_strategy, storage_mode_mask::all},
    {topic_property_record_key_schema_id_validation_compat,
     storage_mode_mask::all},
    {topic_property_record_key_subject_name_strategy_compat,
     storage_mode_mask::all},
    {topic_property_record_value_schema_id_validation_compat,
     storage_mode_mask::all},
    {topic_property_record_value_subject_name_strategy_compat,
     storage_mode_mask::all},

    // Properties valid for local, tiered, and tiered_cloud (NOT cloud).
    // In tiered_cloud mode data goes through raft and local storage,
    // so segment and flush settings apply.
    {topic_property_flush_bytes,
     storage_mode_mask::local_tiered | storage_mode_mask::tiered_cloud},
    {topic_property_flush_ms,
     storage_mode_mask::local_tiered | storage_mode_mask::tiered_cloud},
    {topic_property_segment_size,
     storage_mode_mask::local_tiered | storage_mode_mask::tiered_cloud},
    {topic_property_segment_ms,
     storage_mode_mask::local_tiered | storage_mode_mask::tiered_cloud},
    {topic_property_write_caching,
     storage_mode_mask::local_tiered | storage_mode_mask::tiered_cloud},

    // Properties valid for tiered only
    {topic_property_initial_retention_local_target_bytes,
     storage_mode_mask::tiered},
    {topic_property_initial_retention_local_target_ms,
     storage_mode_mask::tiered},
    {topic_property_remote_read, storage_mode_mask::tiered},
    {topic_property_remote_write, storage_mode_mask::tiered},
    {topic_property_retention_local_target_bytes, storage_mode_mask::tiered},
    {topic_property_retention_local_target_ms, storage_mode_mask::tiered},
    {topic_property_remote_allow_gaps, storage_mode_mask::tiered},

    // Properties valid for tiered and cloud only (NOT local)
    {topic_property_iceberg_mode,
     storage_mode_mask::tiered_and_cloud | storage_mode_mask::tiered_cloud},
    {topic_property_iceberg_delete,
     storage_mode_mask::tiered_and_cloud | storage_mode_mask::tiered_cloud},
    {topic_property_iceberg_partition_spec,
     storage_mode_mask::tiered_and_cloud | storage_mode_mask::tiered_cloud},
    {topic_property_iceberg_invalid_record_action,
     storage_mode_mask::tiered_and_cloud | storage_mode_mask::tiered_cloud},
    {topic_property_iceberg_target_lag_ms,
     storage_mode_mask::tiered_and_cloud | storage_mode_mask::tiered_cloud},
    {topic_property_remote_delete,
     storage_mode_mask::tiered_and_cloud | storage_mode_mask::tiered_cloud},
    {topic_property_read_replica,
     storage_mode_mask::tiered_and_cloud | storage_mode_mask::tiered_cloud},
  });

// Convert a storage mode enum to its corresponding mask bit
constexpr storage_mode_mask
storage_mode_to_mask(model::redpanda_storage_mode mode) {
    switch (mode) {
    case model::redpanda_storage_mode::local:
        return storage_mode_mask::local;
    case model::redpanda_storage_mode::tiered:
        return storage_mode_mask::tiered;
    case model::redpanda_storage_mode::cloud:
        return storage_mode_mask::cloud;
    case model::redpanda_storage_mode::tiered_cloud:
        return storage_mode_mask::tiered_cloud;
    case model::redpanda_storage_mode::unset:
        return storage_mode_mask::unset;
    }
    return storage_mode_mask::none;
}

// Check if a property is valid for a given storage mode.
// All properties must be in the storage_mode_properties list.
// When storage_mode is unset, all properties are considered valid since
// the actual mode depends on legacy shadow_indexing configuration.
inline bool is_property_valid_for_storage_mode(
  std::string_view property_name, model::redpanda_storage_mode mode) {
    // When unset, allow all properties since the actual mode depends on
    // legacy shadow_indexing configuration
    if (mode == model::redpanda_storage_mode::unset) {
        return true;
    }

    const auto mode_mask = storage_mode_to_mask(mode);

    for (const auto& entry : storage_mode_properties) {
        if (entry.property_name == property_name) {
            return (entry.valid_modes & mode_mask) != storage_mode_mask::none;
        }
    }

    return true;
}

// Get the valid storage modes for a property as a human-readable string.
// Returns a string like "local, tiered" or "cloud".
inline ss::sstring
get_valid_storage_modes_string(std::string_view property_name) {
    for (const auto& entry : storage_mode_properties) {
        if (entry.property_name == property_name) {
            ss::sstring result;
            bool first = true;
            if (
              (entry.valid_modes & storage_mode_mask::local)
              != storage_mode_mask::none) {
                result += "local";
                first = false;
            }
            if (
              (entry.valid_modes & storage_mode_mask::tiered)
              != storage_mode_mask::none) {
                if (!first) {
                    result += ", ";
                }
                result += "tiered";
                first = false;
            }
            if (
              (entry.valid_modes & storage_mode_mask::cloud)
              != storage_mode_mask::none) {
                if (!first) {
                    result += ", ";
                }
                result += "cloud";
                first = false;
            }
            if (
              (entry.valid_modes & storage_mode_mask::tiered_cloud)
              != storage_mode_mask::none) {
                if (!first) {
                    result += ", ";
                }
                result += "tiered_cloud";
                first = false;
            }
            return result;
        }
    }
    return "unknown";
}

} // namespace kafka
