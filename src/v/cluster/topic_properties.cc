// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_properties.h"

#include "model/adl_serde.h"
#include "reflection/adl.h"

namespace cluster {

std::ostream& operator<<(std::ostream& o, const topic_properties& properties) {
    fmt::print(
      o,
      "{{compression: {}, cleanup_policy_bitflags: {}, compaction_strategy: "
      "{}, retention_bytes: {}, retention_duration_ms: {}, segment_size: {}, "
      "timestamp_type: {}, recovery_enabled: {}, shadow_indexing: {}, "
      "read_replica: {}, read_replica_bucket: {}, "
      "remote_topic_namespace_override: {}, "
      "remote_topic_properties: {}, "
      "batch_max_bytes: {}, retention_local_target_bytes: {}, "
      "retention_local_target_ms: {}, remote_delete: {}, segment_ms: {}, "
      "record_key_schema_id_validation: {}, "
      "record_key_schema_id_validation_compat: {}, "
      "record_key_subject_name_strategy: {}, "
      "record_key_subject_name_strategy_compat: {}, "
      "record_value_schema_id_validation: {}, "
      "record_value_schema_id_validation_compat: {}, "
      "record_value_subject_name_strategy: {}, "
      "record_value_subject_name_strategy_compat: {}, "
      "initial_retention_local_target_bytes: {}, "
      "initial_retention_local_target_ms: {}, "
      "mpx_virtual_cluster_id: {}, "
      "write_caching: {}, "
      "flush_ms: {}, "
      "flush_bytes: {}, "
      "remote_label: {}, iceberg_enabled: {}, "
      "leaders_preference: {}",
      properties.compression,
      properties.cleanup_policy_bitflags,
      properties.compaction_strategy,
      properties.retention_bytes,
      properties.retention_duration,
      properties.segment_size,
      properties.timestamp_type,
      properties.recovery,
      properties.shadow_indexing,
      properties.read_replica,
      properties.read_replica_bucket,
      properties.remote_topic_namespace_override,
      properties.remote_topic_properties,
      properties.batch_max_bytes,
      properties.retention_local_target_bytes,
      properties.retention_local_target_ms,
      properties.remote_delete,
      properties.segment_ms,
      properties.record_key_schema_id_validation,
      properties.record_key_schema_id_validation_compat,
      properties.record_key_subject_name_strategy,
      properties.record_key_subject_name_strategy_compat,
      properties.record_value_schema_id_validation,
      properties.record_value_schema_id_validation_compat,
      properties.record_value_subject_name_strategy,
      properties.record_value_subject_name_strategy_compat,
      properties.initial_retention_local_target_bytes,
      properties.initial_retention_local_target_ms,
      properties.mpx_virtual_cluster_id,
      properties.write_caching,
      properties.flush_ms,
      properties.flush_bytes,
      properties.remote_label,
      properties.iceberg_enabled,
      properties.leaders_preference);

    if (config::shard_local_cfg().development_enable_cloud_topics()) {
        fmt::print(
          o, ", cloud_topic_enabled: {}", properties.cloud_topic_enabled);
    }

    o << "}";

    return o;
}

bool topic_properties::is_compacted() const {
    if (!cleanup_policy_bitflags) {
        return false;
    }
    return (cleanup_policy_bitflags.value()
            & model::cleanup_policy_bitflags::compaction)
           == model::cleanup_policy_bitflags::compaction;
}

bool topic_properties::has_overrides() const {
    const auto overrides
      = cleanup_policy_bitflags || compaction_strategy || segment_size
        || retention_bytes.is_engaged() || retention_duration.is_engaged()
        || recovery.has_value() || shadow_indexing.has_value()
        || read_replica.has_value()
        || remote_topic_namespace_override.has_value()
        || batch_max_bytes.has_value()
        || retention_local_target_bytes.is_engaged()
        || retention_local_target_ms.is_engaged()
        || remote_delete != storage::ntp_config::default_remote_delete
        || segment_ms.is_engaged()
        || record_key_schema_id_validation.has_value()
        || record_key_schema_id_validation_compat.has_value()
        || record_key_subject_name_strategy.has_value()
        || record_key_subject_name_strategy_compat.has_value()
        || record_value_schema_id_validation.has_value()
        || record_value_schema_id_validation_compat.has_value()
        || record_value_subject_name_strategy.has_value()
        || record_value_subject_name_strategy_compat.has_value()
        || initial_retention_local_target_bytes.is_engaged()
        || initial_retention_local_target_ms.is_engaged()
        || write_caching.has_value() || flush_ms.has_value()
        || flush_bytes.has_value() || remote_label.has_value()
        || (iceberg_enabled != storage::ntp_config::default_iceberg_enabled)
        || leaders_preference.has_value();

    if (config::shard_local_cfg().development_enable_cloud_topics()) {
        return overrides
               || (cloud_topic_enabled != storage::ntp_config::default_cloud_topic_enabled);
    }

    return overrides;
}

bool topic_properties::requires_remote_erase() const {
    // A topic requires remote erase if it matches all of:
    // * Using tiered storage
    // * Not a read replica
    // * Has redpanda.remote.delete=true
    auto mode = shadow_indexing.value_or(model::shadow_indexing_mode::disabled);
    return mode != model::shadow_indexing_mode::disabled
           && !read_replica.value_or(false) && remote_delete;
}

storage::ntp_config::default_overrides
topic_properties::get_ntp_cfg_overrides() const {
    storage::ntp_config::default_overrides ret;
    ret.cleanup_policy_bitflags = cleanup_policy_bitflags;
    ret.compaction_strategy = compaction_strategy;
    ret.retention_bytes = retention_bytes;
    ret.retention_time = retention_duration;
    ret.segment_size = segment_size;
    ret.shadow_indexing_mode = shadow_indexing;
    ret.read_replica = read_replica;
    ret.retention_local_target_bytes = retention_local_target_bytes;
    ret.retention_local_target_ms = retention_local_target_ms;
    ret.remote_delete = remote_delete;
    ret.segment_ms = segment_ms;
    ret.initial_retention_local_target_bytes
      = initial_retention_local_target_bytes;
    ret.initial_retention_local_target_ms = initial_retention_local_target_ms;
    ret.write_caching = write_caching;
    ret.flush_ms = flush_ms;
    ret.flush_bytes = flush_bytes;
    ret.iceberg_enabled = iceberg_enabled;
    ret.cloud_topic_enabled = cloud_topic_enabled;
    return ret;
}

} // namespace cluster

namespace reflection {

// adl is no longer used for serializing new topic properties
void adl<cluster::topic_properties>::to(
  iobuf& out, cluster::topic_properties&& p) {
    reflection::serialize(
      out,
      p.compression,
      p.cleanup_policy_bitflags,
      p.compaction_strategy,
      p.timestamp_type,
      p.segment_size,
      p.retention_bytes,
      p.retention_duration,
      p.recovery,
      p.shadow_indexing,
      p.read_replica,
      p.read_replica_bucket,
      p.remote_topic_properties);
}

cluster::topic_properties
adl<cluster::topic_properties>::from(iobuf_parser& parser) {
    auto compression
      = reflection::adl<std::optional<model::compression>>{}.from(parser);
    auto cleanup_policy_bitflags
      = reflection::adl<std::optional<model::cleanup_policy_bitflags>>{}.from(
        parser);
    auto compaction_strategy
      = reflection::adl<std::optional<model::compaction_strategy>>{}.from(
        parser);
    auto timestamp_type
      = reflection::adl<std::optional<model::timestamp_type>>{}.from(parser);
    auto segment_size = reflection::adl<std::optional<size_t>>{}.from(parser);
    auto retention_bytes = reflection::adl<tristate<size_t>>{}.from(parser);
    auto retention_duration
      = reflection::adl<tristate<std::chrono::milliseconds>>{}.from(parser);
    auto recovery = reflection::adl<std::optional<bool>>{}.from(parser);
    auto shadow_indexing
      = reflection::adl<std::optional<model::shadow_indexing_mode>>{}.from(
        parser);
    auto read_replica = reflection::adl<std::optional<bool>>{}.from(parser);
    auto read_replica_bucket
      = reflection::adl<std::optional<ss::sstring>>{}.from(parser);
    auto remote_topic_properties
      = reflection::adl<std::optional<cluster::remote_topic_properties>>{}.from(
        parser);

    return {
      compression,
      cleanup_policy_bitflags,
      compaction_strategy,
      timestamp_type,
      segment_size,
      retention_bytes,
      retention_duration,
      recovery,
      shadow_indexing,
      read_replica,
      read_replica_bucket,
      std::nullopt,
      remote_topic_properties,
      std::nullopt,
      tristate<size_t>{std::nullopt},
      tristate<std::chrono::milliseconds>{std::nullopt},
      // Backward compat: ADL-generation (pre-22.3) topics use legacy tiered
      // storage mode in which topic deletion does not delete objects in S3
      false,
      tristate<std::chrono::milliseconds>{std::nullopt},
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      tristate<size_t>{std::nullopt},
      tristate<std::chrono::milliseconds>{std::nullopt},
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      false,
      std::nullopt,
      false};
}

} // namespace reflection
