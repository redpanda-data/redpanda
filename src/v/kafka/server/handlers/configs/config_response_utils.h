/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "container/chunked_vector.h"
#include "kafka/protocol/describe_configs.h"
#include "kafka/protocol/schemata/create_topics_response.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"

#include <iterator>
#include <optional>

namespace kafka {

struct config_response {
    ss::sstring name{};
    std::optional<ss::sstring> value{};
    bool read_only{};
    bool is_default{};
    kafka::describe_configs_source config_source{-1};
    bool is_sensitive{};
    std::vector<describe_configs_synonym> synonyms{};
    kafka::describe_configs_type config_type{0};
    std::optional<ss::sstring> documentation{};

    describe_configs_resource_result to_describe_config();
    creatable_topic_configs to_create_config();
};

using config_response_container_t = chunked_vector<config_response>;
using config_key_t = std::optional<chunked_vector<ss::sstring>>;

/// This abstract interface was added in order to decouple metadata_cache from
/// the make_topic_configs method below.  This allows us to create our own
/// adapters for unit tests
struct metadata_cache_info {
    virtual ~metadata_cache_info() = default;
    virtual model::compression get_default_compression() const = 0;
    virtual model::cleanup_policy_bitflags
    get_default_cleanup_policy_bitflags() const = 0;
    virtual size_t get_default_compacted_topic_segment_size() const = 0;
    virtual size_t get_default_segment_size() const = 0;
    virtual std::optional<std::chrono::milliseconds>
    get_default_retention_duration() const = 0;
    virtual std::optional<size_t> get_default_retention_bytes() const = 0;
    virtual model::timestamp_type get_default_timestamp_type() const = 0;
    virtual uint32_t get_default_batch_max_bytes() const = 0;
    virtual model::shadow_indexing_mode
    get_default_shadow_indexing_mode() const = 0;
    virtual std::optional<size_t>
    get_default_retention_local_target_bytes() const = 0;
    virtual std::chrono::milliseconds
    get_default_retention_local_target_ms() const = 0;
    virtual std::optional<std::chrono::milliseconds>
    get_default_segment_ms() const = 0;
    virtual std::optional<std::chrono::milliseconds>
    get_default_delete_retention_ms() const = 0;
    virtual bool get_default_record_key_schema_id_validation() const = 0;
    virtual pandaproxy::schema_registry::subject_name_strategy
    get_default_record_key_subject_name_strategy() const = 0;
    virtual bool get_default_record_value_schema_id_validation() const = 0;
    virtual pandaproxy::schema_registry::subject_name_strategy
    get_default_record_value_subject_name_strategy() const = 0;
    virtual std::optional<size_t>
    get_default_initial_retention_local_target_bytes() const = 0;
    virtual std::optional<std::chrono::milliseconds>
    get_default_initial_retention_local_target_ms() const = 0;
    virtual std::chrono::milliseconds
    get_default_iceberg_target_lag_ms() const = 0;
    virtual std::optional<double>
    get_default_min_cleanable_dirty_ratio() const = 0;
    virtual std::chrono::milliseconds
    get_default_min_compaction_lag_ms() const = 0;
    virtual std::chrono::milliseconds
    get_default_max_compaction_lag_ms() const = 0;
    virtual std::chrono::milliseconds
    get_default_message_timestamp_before_max_ms() const = 0;
    virtual std::chrono::milliseconds
    get_default_message_timestamp_after_max_ms() const = 0;
};
/// This is the adapter to use cluster::metadata_cache
struct metadata_cache_adapter : public metadata_cache_info {
public:
    explicit metadata_cache_adapter(
      const cluster::metadata_cache& metadata_cache)
      : _metadata_cache(metadata_cache) {}

    model::compression get_default_compression() const override;
    model::cleanup_policy_bitflags
    get_default_cleanup_policy_bitflags() const override;
    size_t get_default_compacted_topic_segment_size() const override;
    size_t get_default_segment_size() const override;
    std::optional<std::chrono::milliseconds>
    get_default_retention_duration() const override;
    std::optional<size_t> get_default_retention_bytes() const override;
    model::timestamp_type get_default_timestamp_type() const override;
    uint32_t get_default_batch_max_bytes() const override;
    model::shadow_indexing_mode
    get_default_shadow_indexing_mode() const override;
    std::optional<size_t>
    get_default_retention_local_target_bytes() const override;
    std::chrono::milliseconds
    get_default_retention_local_target_ms() const override;
    std::optional<std::chrono::milliseconds>
    get_default_segment_ms() const override;
    std::optional<std::chrono::milliseconds>
    get_default_delete_retention_ms() const override;
    bool get_default_record_key_schema_id_validation() const override;
    pandaproxy::schema_registry::subject_name_strategy
    get_default_record_key_subject_name_strategy() const override;
    bool get_default_record_value_schema_id_validation() const override;
    pandaproxy::schema_registry::subject_name_strategy
    get_default_record_value_subject_name_strategy() const override;
    std::optional<size_t>
    get_default_initial_retention_local_target_bytes() const override;
    std::optional<std::chrono::milliseconds>
    get_default_initial_retention_local_target_ms() const override;
    std::chrono::milliseconds
    get_default_iceberg_target_lag_ms() const override;
    std::optional<double>
    get_default_min_cleanable_dirty_ratio() const override;
    std::chrono::milliseconds
    get_default_min_compaction_lag_ms() const override;
    std::chrono::milliseconds
    get_default_max_compaction_lag_ms() const override;
    std::chrono::milliseconds
    get_default_message_timestamp_before_max_ms() const override;
    std::chrono::milliseconds
    get_default_message_timestamp_after_max_ms() const override;

private:
    const cluster::metadata_cache& _metadata_cache;
};

config_response_container_t make_topic_configs(
  const metadata_cache_info& metadata_cache,
  const cluster::topic_properties& topic_properties,
  const config_key_t& config_keys,
  bool include_synonyms,
  bool include_documentation);

inline config_response_container_t make_topic_configs(
  const cluster::metadata_cache& metadata_cache,
  const cluster::topic_properties& topic_properties,
  const config_key_t& config_keys,
  bool include_synonyms,
  bool include_documentation) {
    return make_topic_configs(
      metadata_cache_adapter(metadata_cache),
      topic_properties,
      config_keys,
      include_synonyms,
      include_documentation);
}

config_response_container_t make_broker_configs(
  const config_key_t& config_keys,
  bool include_synonyms,
  bool include_documentation);

} // namespace kafka
