// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cloud_storage/remote_label.h"
#include "cluster/remote_topic_properties.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"
#include "reflection/adl.h"
#include "serde/rw/chrono.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "serde/rw/scalar.h"
#include "serde/rw/tristate_rw.h"
#include "storage/ntp_config.h"
#include "utils/tristate.h"

namespace cluster {

/**
 * Structure holding topic properties overrides, empty values will be replaced
 * with defaults
 */
struct topic_properties
  : serde::
      envelope<topic_properties, serde::version<13>, serde::compat_version<0>> {
    topic_properties() noexcept = default;
    topic_properties(
      std::optional<model::compression> compression,
      std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags,
      std::optional<model::compaction_strategy> compaction_strategy,
      std::optional<model::timestamp_type> timestamp_type,
      std::optional<size_t> segment_size,
      tristate<size_t> retention_bytes,
      tristate<std::chrono::milliseconds> retention_duration,
      std::optional<bool> recovery,
      std::optional<model::shadow_indexing_mode> shadow_indexing,
      std::optional<bool> read_replica,
      std::optional<ss::sstring> read_replica_bucket,
      std::optional<model::topic_namespace> remote_topic_namespace_override,
      std::optional<remote_topic_properties> remote_topic_properties,
      std::optional<uint32_t> batch_max_bytes,
      tristate<size_t> retention_local_target_bytes,
      tristate<std::chrono::milliseconds> retention_local_target_ms,
      bool remote_delete,
      tristate<std::chrono::milliseconds> segment_ms,
      std::optional<bool> record_key_schema_id_validation,
      std::optional<bool> record_key_schema_id_validation_compat,
      std::optional<pandaproxy::schema_registry::subject_name_strategy>
        record_key_subject_name_strategy,
      std::optional<pandaproxy::schema_registry::subject_name_strategy>
        record_key_subject_name_strategy_compat,
      std::optional<bool> record_value_schema_id_validation,
      std::optional<bool> record_value_schema_id_validation_compat,
      std::optional<pandaproxy::schema_registry::subject_name_strategy>
        record_value_subject_name_strategy,
      std::optional<pandaproxy::schema_registry::subject_name_strategy>
        record_value_subject_name_strategy_compat,
      tristate<size_t> initial_retention_local_target_bytes,
      tristate<std::chrono::milliseconds> initial_retention_local_target_ms,
      std::optional<model::vcluster_id> mpx_virtual_cluster_id,
      std::optional<model::write_caching_mode> write_caching,
      std::optional<std::chrono::milliseconds> flush_ms,
      std::optional<size_t> flush_bytes,
      model::iceberg_mode iceberg_mode,
      std::optional<config::leaders_preference> leaders_preference,
      tristate<std::chrono::milliseconds> delete_retention_ms,
      std::optional<bool> iceberg_delete,
      std::optional<ss::sstring> iceberg_partition_spec,
      std::optional<model::iceberg_invalid_record_action>
        iceberg_invalid_record_action,
      std::optional<std::chrono::milliseconds> iceberg_target_lag_ms,
      tristate<double> min_cleanable_dirty_ratio,
      std::optional<std::chrono::milliseconds> min_compaction_lag_ms,
      std::optional<std::chrono::milliseconds> max_compaction_lag_ms,
      std::optional<bool> remote_topic_allow_gaps,
      std::optional<std::chrono::milliseconds> message_timestamp_before_max_ms,
      std::optional<std::chrono::milliseconds> message_timestamp_after_max_ms,
      model::redpanda_storage_mode storage_mode)
      : compression(compression)
      , cleanup_policy_bitflags(cleanup_policy_bitflags)
      , compaction_strategy(compaction_strategy)
      , timestamp_type(timestamp_type)
      , segment_size(segment_size)
      , retention_bytes(retention_bytes)
      , retention_duration(retention_duration)
      , recovery(recovery)
      , shadow_indexing(shadow_indexing)
      , read_replica(read_replica)
      , read_replica_bucket(std::move(read_replica_bucket))
      , remote_topic_namespace_override(remote_topic_namespace_override)
      , remote_topic_properties(remote_topic_properties)
      , remote_topic_allow_gaps(remote_topic_allow_gaps)
      , batch_max_bytes(batch_max_bytes)
      , retention_local_target_bytes(retention_local_target_bytes)
      , retention_local_target_ms(retention_local_target_ms)
      , remote_delete(remote_delete)
      , segment_ms(segment_ms)
      , record_key_schema_id_validation(record_key_schema_id_validation)
      , record_key_schema_id_validation_compat(
          record_key_schema_id_validation_compat)
      , record_key_subject_name_strategy(record_key_subject_name_strategy)
      , record_key_subject_name_strategy_compat(
          record_key_subject_name_strategy_compat)
      , record_value_schema_id_validation(record_value_schema_id_validation)
      , record_value_schema_id_validation_compat(
          record_value_schema_id_validation_compat)
      , record_value_subject_name_strategy(record_value_subject_name_strategy)
      , record_value_subject_name_strategy_compat(
          record_value_subject_name_strategy_compat)
      , initial_retention_local_target_bytes(
          initial_retention_local_target_bytes)
      , initial_retention_local_target_ms(initial_retention_local_target_ms)
      , mpx_virtual_cluster_id(mpx_virtual_cluster_id)
      , write_caching(write_caching)
      , flush_ms(flush_ms)
      , flush_bytes(flush_bytes)
      , iceberg_mode(iceberg_mode)
      , leaders_preference(std::move(leaders_preference))
      , delete_retention_ms(delete_retention_ms)
      , iceberg_delete(iceberg_delete)
      , iceberg_partition_spec(std::move(iceberg_partition_spec))
      , iceberg_invalid_record_action(iceberg_invalid_record_action)
      , iceberg_target_lag_ms(iceberg_target_lag_ms)
      , min_cleanable_dirty_ratio(min_cleanable_dirty_ratio)
      , min_compaction_lag_ms(min_compaction_lag_ms)
      , max_compaction_lag_ms(max_compaction_lag_ms)
      , message_timestamp_before_max_ms(message_timestamp_before_max_ms)
      , message_timestamp_after_max_ms(message_timestamp_after_max_ms)
      , storage_mode(storage_mode) {}

    std::optional<model::compression> compression;
    std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags;
    std::optional<model::compaction_strategy> compaction_strategy;
    std::optional<model::timestamp_type> timestamp_type;
    std::optional<size_t> segment_size;
    tristate<size_t> retention_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> retention_duration{std::nullopt};
    std::optional<bool> recovery;
    std::optional<model::shadow_indexing_mode> shadow_indexing;
    std::optional<bool> read_replica;
    std::optional<ss::sstring> read_replica_bucket;
    // The ntp override used for tiered storage. In case of a
    // cross-cluster migration, the ntp used for archival subsystems may differ
    // from the ntp used for local storage.
    std::optional<model::topic_namespace> remote_topic_namespace_override;

    // Topic properties for a topic that already has remote data (e.g.
    // recovery topics).
    std::optional<remote_topic_properties> remote_topic_properties;

    // The override that indicates that when tiered-storage is paused the local
    // retention is allowed to work and potentially create a gap in the data.
    std::optional<bool> remote_topic_allow_gaps;

    std::optional<uint32_t> batch_max_bytes;
    tristate<size_t> retention_local_target_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> retention_local_target_ms{std::nullopt};

    // Remote deletes are enabled by default in new tiered storage topics,
    // disabled by default in legacy topics during upgrade.
    // This is intentionally not an optional: all topics have a concrete value
    // one way or another.  There is no "use the cluster default".
    bool remote_delete{storage::ntp_config::default_remote_delete};

    tristate<std::chrono::milliseconds> segment_ms{std::nullopt};

    std::optional<bool> record_key_schema_id_validation;
    std::optional<bool> record_key_schema_id_validation_compat;
    std::optional<pandaproxy::schema_registry::subject_name_strategy>
      record_key_subject_name_strategy;
    std::optional<pandaproxy::schema_registry::subject_name_strategy>
      record_key_subject_name_strategy_compat;
    std::optional<bool> record_value_schema_id_validation;
    std::optional<bool> record_value_schema_id_validation_compat;
    std::optional<pandaproxy::schema_registry::subject_name_strategy>
      record_value_subject_name_strategy;
    std::optional<pandaproxy::schema_registry::subject_name_strategy>
      record_value_subject_name_strategy_compat;

    tristate<size_t> initial_retention_local_target_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> initial_retention_local_target_ms{
      std::nullopt};
    std::optional<model::vcluster_id> mpx_virtual_cluster_id;
    std::optional<model::write_caching_mode> write_caching;
    std::optional<std::chrono::milliseconds> flush_ms;
    std::optional<size_t> flush_bytes;
    model::iceberg_mode iceberg_mode{storage::ntp_config::default_iceberg_mode};

    // Label to be used when generating paths of remote objects (manifests,
    // segments, etc) of this topic.
    //
    // The topic's data is associated with exactly one label: as a topic is
    // removed and recovered across different clusters, its label will be the
    // same, even though the clusters' UUIDs hosting it will be different. This
    // allows recovered topics and read replica topics to download with just
    // one label in mind.
    //
    // std::nullopt indicates this topic was created before labels were
    // supported, in which case objects will use a legacy naming scheme.
    std::optional<cloud_storage::remote_label> remote_label;

    std::optional<config::leaders_preference> leaders_preference;

    tristate<std::chrono::milliseconds> delete_retention_ms{disable_tristate};
    // Should we delete the corresponding iceberg table when deleting the topic.
    std::optional<bool> iceberg_delete;
    // Partition spec expression for the corresponding Iceberg table.
    // std::nullopt means that the cluster default will be used.
    std::optional<ss::sstring> iceberg_partition_spec;

    std::optional<model::iceberg_invalid_record_action>
      iceberg_invalid_record_action;

    std::optional<std::chrono::milliseconds> iceberg_target_lag_ms{};

    tristate<double> min_cleanable_dirty_ratio{std::nullopt};
    std::optional<std::chrono::milliseconds> min_compaction_lag_ms{};
    std::optional<std::chrono::milliseconds> max_compaction_lag_ms{};

    std::optional<std::chrono::milliseconds> message_timestamp_before_max_ms{};
    std::optional<std::chrono::milliseconds> message_timestamp_after_max_ms{};

    // Storage mode for the topic: local, tiered, or cloud
    model::redpanda_storage_mode storage_mode{
      storage::ntp_config::default_storage_mode};

    bool is_cloud_topic() const {
        return storage_mode == model::redpanda_storage_mode::cloud
               || storage_mode == model::redpanda_storage_mode::tiered_cloud;
    }

    bool is_compacted() const;
    bool has_overrides() const;
    // Returns true if this topic is a tiered topic that requires
    // deletion of Redpanda data in cloud storage.
    bool requires_tiered_remote_erase() const;
    // Returns true if this topic is a cloud topic that requires
    // deletion of Redpanda data in cloud storage.
    bool requires_cloud_topic_remote_erase() const;
    // Returns true if this topic is an iceberg-enabled topic that requires
    // deletion of Iceberg data in cloud storage.
    bool requires_iceberg_remote_erase() const;

    // Returns true if the topic has archival (remote write on a tiered topic)
    // enabled. This checks both storage_mode and shadow_indexing to ensure the
    // topic is configured for tiered storage with archival.
    bool is_archival_enabled() const;

    // Returns true if the topic has remote fetch (remote read on a tiered
    // topic) enabled. This checks both storage_mode and shadow_indexing to
    // ensure the topic is configured for tiered storage with remote fetch.
    bool is_remote_fetch_enabled() const;

    storage::ntp_config::default_overrides get_ntp_cfg_overrides() const;

    friend std::ostream& operator<<(std::ostream&, const topic_properties&);
    auto serde_fields() {
        return std::tie(
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
          remote_topic_properties,
          batch_max_bytes,
          retention_local_target_bytes,
          retention_local_target_ms,
          remote_delete,
          segment_ms,
          record_key_schema_id_validation,
          record_key_schema_id_validation_compat,
          record_key_subject_name_strategy,
          record_key_subject_name_strategy_compat,
          record_value_schema_id_validation,
          record_value_schema_id_validation_compat,
          record_value_subject_name_strategy,
          record_value_subject_name_strategy_compat,
          initial_retention_local_target_bytes,
          initial_retention_local_target_ms,
          mpx_virtual_cluster_id,
          write_caching,
          flush_ms,
          flush_bytes,
          remote_label,
          remote_topic_namespace_override,
          iceberg_mode,
          leaders_preference,
          deprecated_cloud_topic_enabled,
          delete_retention_ms,
          iceberg_delete,
          iceberg_partition_spec,
          iceberg_invalid_record_action,
          iceberg_target_lag_ms,
          min_cleanable_dirty_ratio,
          remote_topic_allow_gaps,
          min_compaction_lag_ms,
          max_compaction_lag_ms,
          message_timestamp_before_max_ms,
          message_timestamp_after_max_ms,
          storage_mode);
    }

    friend bool
    operator==(const topic_properties&, const topic_properties&) = default;

private:
    // This was deprecated in favour of redpanda.storage.mode, but is kept here
    // for backwards compatible serde purposes.
    bool deprecated_cloud_topic_enabled{false};
};

} // namespace cluster

namespace reflection {

template<>
struct adl<cluster::topic_properties> {
    void to(iobuf&, cluster::topic_properties&&);
    cluster::topic_properties from(iobuf_parser&);
};

} // namespace reflection
