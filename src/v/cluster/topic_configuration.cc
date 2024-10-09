// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_configuration.h"

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "reflection/adl.h"
#include "storage/ntp_config.h"

#include <seastar/core/sstring.hh>

namespace cluster {

storage::ntp_config topic_configuration::make_ntp_config(
  const ss::sstring& work_dir,
  model::partition_id p_id,
  model::revision_id rev,
  model::initial_revision_id init_rev) const {
    auto has_overrides = properties.has_overrides() || is_internal();
    std::unique_ptr<storage::ntp_config::default_overrides> overrides = nullptr;

    if (has_overrides) {
        overrides = std::make_unique<storage::ntp_config::default_overrides>(
          storage::ntp_config::default_overrides{
            .cleanup_policy_bitflags = properties.cleanup_policy_bitflags,
            .compaction_strategy = properties.compaction_strategy,
            .segment_size = properties.segment_size,
            .retention_bytes = properties.retention_bytes,
            .retention_time = properties.retention_duration,
            // we disable cache for internal topics as they are read only once
            // during bootstrap.
            .cache_enabled = storage::with_cache(
              !is_internal() || tp_ns.tp == model::tx_manager_topic),
            .recovery_enabled = storage::topic_recovery_enabled(
              properties.recovery ? *properties.recovery : false),
            .shadow_indexing_mode = properties.shadow_indexing,
            .read_replica = properties.read_replica,
            .retention_local_target_bytes
            = properties.retention_local_target_bytes,
            .retention_local_target_ms = properties.retention_local_target_ms,
            .remote_delete = properties.remote_delete,
            .segment_ms = properties.segment_ms,
            .initial_retention_local_target_bytes
            = properties.initial_retention_local_target_bytes,
            .initial_retention_local_target_ms
            = properties.initial_retention_local_target_ms,
            .write_caching = properties.write_caching,
            .flush_ms = properties.flush_ms,
            .flush_bytes = properties.flush_bytes,
            .iceberg_enabled = properties.iceberg_enabled,
            .cloud_topic_enabled = properties.cloud_topic_enabled,
          });
    }
    return {
      model::ntp(tp_ns.ns, tp_ns.tp, p_id),
      work_dir,
      std::move(overrides),
      rev,
      init_rev};
}

void topic_configuration::serde_write(iobuf& out) {
    using serde::write;
    write(out, tp_ns);
    write(out, partition_count);
    write(out, replication_factor);
    write(out, properties);
    write(out, is_migrated);
}

void topic_configuration::serde_read(iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;

    tp_ns = read_nested<model::topic_namespace>(in, h._bytes_left_limit);
    partition_count = read_nested<int32_t>(in, h._bytes_left_limit);
    replication_factor = read_nested<int16_t>(in, h._bytes_left_limit);
    properties = read_nested<topic_properties>(in, h._bytes_left_limit);
    if (h._version >= 2) {
        is_migrated = read_nested<bool>(in, h._bytes_left_limit);
    } else {
        is_migrated = false;
    }

    if (h._version < 1) {
        // Legacy tiered storage topics do not delete data on
        // topic deletion.
        properties.remote_delete = storage::ntp_config::legacy_remote_delete;
    }
}

std::ostream& operator<<(std::ostream& o, const topic_configuration& cfg) {
    fmt::print(
      o,
      "{{ topic: {}, partition_count: {}, replication_factor: {}, is_migrated: "
      "{}, properties: {}}}",
      cfg.tp_ns,
      cfg.partition_count,
      cfg.replication_factor,
      cfg.is_migrated,
      cfg.properties);

    return o;
}

} // namespace cluster

namespace reflection {

// note: adl serialization doesn't support read replica fields since serde
// should be used for new versions.
void adl<cluster::topic_configuration>::to(
  iobuf& out, cluster::topic_configuration&& t) {
    int32_t version = -1;
    reflection::serialize(
      out,
      version,
      t.tp_ns,
      t.partition_count,
      t.replication_factor,
      t.properties.compression,
      t.properties.cleanup_policy_bitflags,
      t.properties.compaction_strategy,
      t.properties.timestamp_type,
      t.properties.segment_size,
      t.properties.retention_bytes,
      t.properties.retention_duration,
      t.properties.recovery,
      t.properties.shadow_indexing);
}

// note: adl deserialization doesn't support read replica or migration fields
// since serde should be used for new versions.
cluster::topic_configuration
adl<cluster::topic_configuration>::from(iobuf_parser& in) {
    // NOTE: The first field of the topic_configuration is a
    // model::ns which has length prefix which is always
    // positive.
    // We're using negative length value to encode version. So if
    // the first int32_t value is positive then we're dealing with
    // the old format. The negative value means that the new format
    // was used.
    auto version = adl<int32_t>{}.from(in.peek(4));
    if (version < 0) {
        // Consume version from stream
        in.skip(4);
        vassert(
          -1 == version,
          "topic_configuration version {} is not supported",
          version);
    } else {
        version = 0;
    }
    auto ns = model::ns(adl<ss::sstring>{}.from(in));
    auto topic = model::topic(adl<ss::sstring>{}.from(in));
    auto partition_count = adl<int32_t>{}.from(in);
    auto rf = adl<int16_t>{}.from(in);

    auto cfg = cluster::topic_configuration(
      std::move(ns), std::move(topic), partition_count, rf);

    cfg.properties.compression = adl<std::optional<model::compression>>{}.from(
      in);
    cfg.properties.cleanup_policy_bitflags
      = adl<std::optional<model::cleanup_policy_bitflags>>{}.from(in);
    cfg.properties.compaction_strategy
      = adl<std::optional<model::compaction_strategy>>{}.from(in);
    cfg.properties.timestamp_type
      = adl<std::optional<model::timestamp_type>>{}.from(in);
    cfg.properties.segment_size = adl<std::optional<size_t>>{}.from(in);
    cfg.properties.retention_bytes = adl<tristate<size_t>>{}.from(in);
    cfg.properties.retention_duration
      = adl<tristate<std::chrono::milliseconds>>{}.from(in);
    if (version < 0) {
        cfg.properties.recovery = adl<std::optional<bool>>{}.from(in);
        cfg.properties.shadow_indexing
          = adl<std::optional<model::shadow_indexing_mode>>{}.from(in);
    }

    // Legacy topics from pre-22.3 get remote delete disabled.
    cfg.properties.remote_delete = storage::ntp_config::legacy_remote_delete;

    return cfg;
}

} // namespace reflection
