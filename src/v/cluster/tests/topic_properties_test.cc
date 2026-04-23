// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_properties.h"

#include <gtest/gtest.h>

namespace cluster {

// Avoid a common typo where a part of the string format is passed as an
// argument.
// clang-format off
// I.e fmt::format("a: {}", "b: {}", a, b) resulting in "a: b: {}"
// instead of fmt::format("a: {}, b: {}", a, b) which should result in "a: <value> b: <value>"
// clang-format on
TEST(TopicProperties, ostream) {
    topic_properties properties;
    std::ostringstream stream;
    stream << properties;
    auto result = stream.str();
    ASSERT_FALSE(result.contains("{}")) << result;
}

TEST(TopicProperties, serde_roundtrip_with_replicas_preference) {
    topic_properties orig;
    orig.replicas_preference = config::replicas_preference::parse(
      "racks: A, {B, C}, D");

    iobuf buf;
    serde::write(buf, std::move(orig));

    auto parser = iobuf_parser(std::move(buf));
    auto decoded = serde::read<topic_properties>(parser);

    ASSERT_TRUE(decoded.replicas_preference.has_value());
    EXPECT_EQ(decoded.replicas_preference->num_groups(), 3);
    EXPECT_EQ(
      decoded.replicas_preference->group_index_for(model::rack_id{"A"}).value(),
      0);
    EXPECT_EQ(
      decoded.replicas_preference->group_index_for(model::rack_id{"B"}).value(),
      1);
    EXPECT_EQ(
      decoded.replicas_preference->group_index_for(model::rack_id{"C"}).value(),
      1);
    EXPECT_EQ(
      decoded.replicas_preference->group_index_for(model::rack_id{"D"}).value(),
      2);
}

TEST(TopicProperties, serde_roundtrip_without_replicas_preference) {
    topic_properties orig;
    // replicas_preference defaults to nullopt

    iobuf buf;
    serde::write(buf, std::move(orig));

    auto parser = iobuf_parser(std::move(buf));
    auto decoded = serde::read<topic_properties>(parser);

    EXPECT_FALSE(decoded.replicas_preference.has_value());
}

/// A v13 topic_properties struct (before replicas_preference was added).
/// Used to verify backward compatibility: deserializing a v13 payload as
/// v14 should leave replicas_preference as nullopt.
struct topic_properties_v13
  : serde::envelope<
      topic_properties_v13,
      serde::version<13>,
      serde::compat_version<0>> {
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
    std::optional<remote_topic_properties> remote_topic_properties;
    std::optional<uint32_t> batch_max_bytes;
    tristate<size_t> retention_local_target_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> retention_local_target_ms{std::nullopt};
    bool remote_delete{false};
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
    std::optional<cloud_storage::remote_label> remote_label;
    std::optional<model::topic_namespace> remote_topic_namespace_override;
    model::iceberg_mode iceberg_mode{model::iceberg_mode::disabled};
    std::optional<config::leaders_preference> leaders_preference;
    bool deprecated_cloud_topic_enabled{false};
    tristate<std::chrono::milliseconds> delete_retention_ms{disable_tristate};
    std::optional<bool> iceberg_delete;
    std::optional<ss::sstring> iceberg_partition_spec;
    std::optional<model::iceberg_invalid_record_action>
      iceberg_invalid_record_action;
    std::optional<std::chrono::milliseconds> iceberg_target_lag_ms{};
    tristate<double> min_cleanable_dirty_ratio{std::nullopt};
    std::optional<bool> remote_topic_allow_gaps;
    std::optional<std::chrono::milliseconds> min_compaction_lag_ms{};
    std::optional<std::chrono::milliseconds> max_compaction_lag_ms{};
    std::optional<std::chrono::milliseconds> message_timestamp_before_max_ms{};
    std::optional<std::chrono::milliseconds> message_timestamp_after_max_ms{};
    model::redpanda_storage_mode storage_mode{
      model::redpanda_storage_mode::local};
    // No replicas_preference — this is v13

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
};

TEST(TopicProperties, serde_backward_compat_v13_to_v14) {
    // Serialize a v13 payload (no replicas_preference field).
    topic_properties_v13 v13;
    v13.compression = model::compression::gzip;
    v13.segment_size = 1024;

    iobuf buf;
    serde::write(buf, std::move(v13));

    // Deserialize as v14 topic_properties — replicas_preference should
    // be default-initialized to nullopt.
    auto parser = iobuf_parser(std::move(buf));
    auto decoded = serde::read<topic_properties>(parser);

    EXPECT_EQ(decoded.compression, model::compression::gzip);
    EXPECT_EQ(decoded.segment_size, 1024);
    EXPECT_FALSE(decoded.replicas_preference.has_value());
}

} // namespace cluster
