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

#include "kafka/server/handlers/configs/config_response_utils.h"

#include "cluster/metadata_cache.h"
#include "cluster/types.h"
#include "config/node_config.h"
#include "kafka/server/handlers/topics/types.h"

#include <charconv>
#include <chrono>

using namespace std::chrono_literals;

namespace kafka {

static bool config_property_requested(
  const config_key_t& configuration_keys,
  const std::string_view property_name) {
    return !configuration_keys.has_value()
           || std::find(
                configuration_keys->begin(),
                configuration_keys->end(),
                property_name)
                != configuration_keys->end();
}

template<typename T>
static void add_config(
  describe_configs_result& result,
  std::string_view name,
  T value,
  describe_configs_source source) {
    result.configs.push_back(describe_configs_resource_result{
      .name = ss::sstring(name),
      .value = ssx::sformat("{}", value),
      .config_source = source,
    });
}

template<typename T>
static void add_config_if_requested(
  const config_key_t& configuration_keys,
  describe_configs_result& result,
  std::string_view name,
  T value,
  describe_configs_source source) {
    if (config_property_requested(configuration_keys, name)) {
        add_config(result, name, value, source);
    }
}

template<typename T>
ss::sstring describe_as_string(const T& t) {
    return ssx::sformat("{}", t);
}

// Instantiate explicitly for unit testing
template ss::sstring describe_as_string(const int&);

// Kafka protocol defines integral types by sizes. See
// https://kafka.apache.org/protocol.html
// Therefore we should also use type sizes for integrals and use Java type sizes
// as a guideline. See
// https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
template<typename T>
constexpr auto num_bits = CHAR_BIT * sizeof(T);

template<typename T>
constexpr bool is_short = std::is_integral_v<T> && !std::is_same_v<T, bool>
                          && num_bits<T> <= 16;

template<typename T>
constexpr bool is_int = std::is_integral_v<T> && num_bits<T> > 16
                        && num_bits<T> <= 32;

template<typename T>
constexpr bool is_long = std::is_integral_v<T> && num_bits<T> > 32
                         && num_bits<T> <= 64;

// property_config_type maps the datatype for a config property to
// describe_configs_type. Currently class_type and password are not used in
// Redpanda so we do not include checks for those types. You may find a similar
// mapping in Apache Kafka at
// https://github.com/apache/kafka/blob/be032735b39360df1a6de1a7feea8b4336e5bcc0/core/src/main/scala/kafka/server/ConfigHelper.scala
template<typename T>
consteval describe_configs_type property_config_type() {
    // clang-format off
    constexpr auto is_string_type = std::is_same_v<T, ss::sstring> ||
        std::is_same_v<T, model::compression> ||
        std::is_same_v<T, model::cleanup_policy_bitflags> ||
        std::is_same_v<T, model::timestamp_type> ||
        std::is_same_v<T, config::data_directory_path> ||
        std::is_same_v<T, pandaproxy::schema_registry::subject_name_strategy> || 
        std::is_same_v<T, model::vcluster_id> ||
        std::is_same_v<T, model::write_caching_mode> ||
        std::is_same_v<T, config::leaders_preference>;

    constexpr auto is_long_type = is_long<T> ||
        // Long type since seconds is atleast a 35-bit signed integral
        // https://en.cppreference.com/w/cpp/chrono/duration
        std::is_same_v<T, std::chrono::seconds> ||
        // Long type since milliseconds is atleast a 45-bit signed integral
        // https://en.cppreference.com/w/cpp/chrono/duration
        std::is_same_v<T, std::chrono::milliseconds>;
    // clang-format on

    if constexpr (
      reflection::is_std_optional<T> || reflection::is_tristate<T>) {
        return property_config_type<typename T::value_type>();
        return property_config_type<typename T::value_type>();
    } else if constexpr (std::is_same_v<T, bool>) {
        return describe_configs_type::boolean;
    } else if constexpr (is_string_type) {
        return describe_configs_type::string;
    } else if constexpr (is_short<T>) {
        return describe_configs_type::short_type;
    } else if constexpr (is_int<T>) {
        return describe_configs_type::int_type;
    } else if constexpr (is_long_type) {
        return describe_configs_type::long_type;
    } else if constexpr (std::is_floating_point_v<T>) {
        return describe_configs_type::double_type;
    } else if constexpr (reflection::is_std_vector<T>) {
        return describe_configs_type::list;
    } else {
        static_assert(
          base::unsupported_type<T>::value,
          "Type name is not supported in describe_configs_type");
    }
}

template<typename T, typename Func>
static void add_broker_config(
  config_response_container_t& result,
  std::string_view name,
  const config::property<T>& property,
  bool include_synonyms,
  std::optional<ss::sstring> documentation,
  Func&& describe_f) {
    describe_configs_source src
      = property.is_overriden() ? describe_configs_source::static_broker_config
                                : describe_configs_source::default_config;

    std::vector<describe_configs_synonym> synonyms;
    if (include_synonyms) {
        synonyms.reserve(2);
        /**
         * If value was overriden, include override
         */
        if (src == describe_configs_source::static_broker_config) {
            synonyms.push_back(describe_configs_synonym{
              .name = ss::sstring(property.name()),
              .value = describe_f(property.value()),
              .source = static_cast<int8_t>(
                describe_configs_source::static_broker_config),
            });
        }
        /**
         * If property is required it has no default
         */
        if (!property.is_required()) {
            synonyms.push_back(describe_configs_synonym{
              .name = ss::sstring(property.name()),
              .value = describe_f(property.default_value()),
              .source = static_cast<int8_t>(
                describe_configs_source::default_config),
            });
        }
    }

    result.push_back(config_response{
      .name = ss::sstring(name),
      .value = describe_f(property.value()),
      .config_source = src,
      .synonyms = std::move(synonyms),
      .config_type = property_config_type<T>(),
      .documentation = documentation,
    });
}

template<typename T, typename Func>
static void add_broker_config_if_requested(
  const config_key_t& config_keys,
  config_response_container_t& result,
  std::string_view name,
  const config::property<T>& property,
  bool include_synonyms,
  std::optional<ss::sstring> documentation,
  Func&& describe_f) {
    if (config_property_requested(config_keys, name)) {
        add_broker_config(
          result,
          name,
          property,
          include_synonyms,
          documentation,
          std::forward<Func>(describe_f));
    }
}

template<typename T, typename Func>
static void add_topic_config(
  config_response_container_t& result,
  std::string_view default_name,
  const T& default_value,
  std::string_view override_name,
  const std::optional<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation,
  Func&& describe_f) {
    describe_configs_source src = overrides
                                    ? describe_configs_source::topic
                                    : describe_configs_source::default_config;

    std::vector<describe_configs_synonym> synonyms;
    if (include_synonyms) {
        synonyms.reserve(2);
        if (overrides) {
            synonyms.push_back(describe_configs_synonym{
              .name = ss::sstring(override_name),
              .value = describe_f(*overrides),
              .source = static_cast<int8_t>(describe_configs_source::topic),
            });
        }
        synonyms.push_back(describe_configs_synonym{
          .name = ss::sstring(default_name),
          .value = describe_f(default_value),
          .source = static_cast<int8_t>(
            describe_configs_source::default_config),
        });
    }

    result.push_back(config_response{
      .name = ss::sstring(override_name),
      .value = describe_f(overrides.value_or(default_value)),
      .config_source = src,
      .synonyms = std::move(synonyms),
      .config_type = property_config_type<T>(),
      .documentation = documentation,
    });
}
/**
 * Special overload for topic configuration which does not have the default per
 * cluster value
 */
template<typename T, typename Func>
static void add_topic_config(
  config_response_container_t& result,
  std::string_view override_name,
  const std::optional<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation,
  Func&& describe_f) {
    describe_configs_source src = overrides
                                    ? describe_configs_source::topic
                                    : describe_configs_source::default_config;

    std::vector<describe_configs_synonym> synonyms;
    if (include_synonyms) {
        synonyms.reserve(2);
        if (overrides) {
            synonyms.push_back(describe_configs_synonym{
              .name = ss::sstring(override_name),
              .value = describe_f(*overrides),
              .source = static_cast<int8_t>(describe_configs_source::topic),
            });
        }
        synonyms.push_back(describe_configs_synonym{
          .name = ss::sstring(override_name),
          .value = std::nullopt,
          .source = static_cast<int8_t>(
            describe_configs_source::default_config),
        });
    }

    result.push_back(config_response{
      .name = ss::sstring(override_name),
      .value = describe_f(overrides),
      .config_source = src,
      .synonyms = std::move(synonyms),
      .config_type = property_config_type<T>(),
      .documentation = documentation,
    });
}

/**
 * For faking DEFAULT_CONFIG status for properties that are actually
 * topic overrides: cloud storage properties.  We do not support cluster
 * defaults for these, the values are always "sticky" to topics, but
 * some Kafka clients insist that after an AlterConfig RPC, anything
 * they didn't set should be DEFAULT_CONFIG.
 *
 * See https://github.com/redpanda-data/redpanda/issues/7451
 */
template<typename T>
std::optional<T>
override_if_not_default(const std::optional<T>& override, const T& def) {
    if (override && override.value() != def) {
        return override;
    } else {
        return std::nullopt;
    }
}

template<typename T, typename Func>
void add_topic_config_if_requested(
  const config_key_t& config_keys,
  config_response_container_t& result,
  std::string_view default_name,
  const T& default_value,
  std::string_view override_name,
  const std::optional<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation,
  Func&& describe_f,
  bool hide_default_override = false) {
    if (config_property_requested(config_keys, override_name)) {
        std::optional<T> overrides_val;
        if (hide_default_override) {
            overrides_val = override_if_not_default(overrides, default_value);
        } else {
            overrides_val = overrides;
        }

        add_topic_config(
          result,
          default_name,
          default_value,
          override_name,
          overrides_val,
          include_synonyms,
          documentation,
          std::forward<Func>(describe_f));
    }
}

// Instantiate explicitly for unit testing
using describe_int_t = decltype(&describe_as_string<int>);
template void add_topic_config_if_requested<int, describe_int_t>(
  const config_key_t& config_keys,
  config_response_container_t& result,
  std::string_view default_name,
  const int& default_value,
  std::string_view override_name,
  const std::optional<int>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation,
  describe_int_t&& describe_f,
  bool hide_default_override = false);

template<typename T>
static ss::sstring maybe_print_tristate(const tristate<T>& tri) {
    if (tri.is_disabled() || !tri.has_optional_value()) {
        return "-1";
    }
    return ssx::sformat("{}", tri.value());
}

template<typename T>
static void add_topic_config(
  config_response_container_t& result,
  std::string_view default_name,
  const std::optional<T>& default_value,
  std::string_view override_name,
  const tristate<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation) {
    // Wrap overrides in an optional because add_topic_config expects
    // optional<S> where S = tristate<T>
    std::optional<tristate<T>> override_value;
    if (overrides.is_engaged()) {
        override_value = std::make_optional(overrides);
    }

    add_topic_config(
      result,
      default_name,
      tristate<T>{default_value},
      override_name,
      override_value,
      include_synonyms,
      documentation,
      &maybe_print_tristate<T>);
}

template<typename T>
void add_topic_config_if_requested(
  const config_key_t& config_keys,
  config_response_container_t& result,
  std::string_view default_name,
  const std::optional<T>& default_value,
  std::string_view override_name,
  const tristate<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation) {
    if (config_property_requested(config_keys, override_name)) {
        add_topic_config(
          result,
          default_name,
          default_value,
          override_name,
          overrides,
          include_synonyms,
          documentation);
    }
}

// Instantiate explicitly for unit testing
template void add_topic_config_if_requested(
  const config_key_t& config_keys,
  config_response_container_t& result,
  std::string_view default_name,
  const std::optional<int>& default_value,
  std::string_view override_name,
  const tristate<int>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation);

template<typename T, typename Func>
static void add_topic_config_if_requested(
  const config_key_t& config_keys,
  config_response_container_t& result,
  std::string_view override_name,
  const std::optional<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation,
  Func&& describe_f) {
    if (config_property_requested(config_keys, override_name)) {
        add_topic_config(
          result,
          override_name,
          overrides,
          include_synonyms,
          documentation,
          std::forward<Func>(describe_f));
    }
}

static ss::sstring
kafka_endpoint_format(const std::vector<model::broker_endpoint>& endpoints) {
    std::vector<ss::sstring> uris;
    uris.reserve(endpoints.size());
    std::transform(
      endpoints.cbegin(),
      endpoints.cend(),
      std::back_inserter(uris),
      [](const model::broker_endpoint& ep) {
          return ssx::sformat(
            "{}://{}:{}",
            (ep.name.empty() ? "plain" : ep.name),
            ep.address.host(),
            ep.address.port());
      });
    return ssx::sformat("{}", fmt::join(uris, ","));
}

static ss::sstring kafka_authn_endpoint_format(
  const std::vector<config::broker_authn_endpoint>& endpoints) {
    std::vector<ss::sstring> uris;
    uris.reserve(endpoints.size());
    std::transform(
      endpoints.cbegin(),
      endpoints.cend(),
      std::back_inserter(uris),
      [](const config::broker_authn_endpoint& ep) {
          return ssx::sformat(
            "{}://{}:{}",
            (ep.name.empty() ? "plain" : ep.name),
            ep.address.host(),
            ep.address.port());
      });
    return ssx::sformat("{}", fmt::join(uris, ","));
}

static inline std::optional<ss::sstring> maybe_make_documentation(
  bool include_documentation, const std::string_view& docstring) {
    return include_documentation ? std::make_optional(ss::sstring{docstring})
                                 : std::nullopt;
}

config_response_container_t make_topic_configs(
  const cluster::metadata_cache& metadata_cache,
  const cluster::topic_properties& topic_properties,
  const config_key_t& config_keys,
  bool include_synonyms,
  bool include_documentation) {
    config_response_container_t result;

    add_topic_config_if_requested(
      config_keys,
      result,
      config::shard_local_cfg().log_compression_type.name(),
      metadata_cache.get_default_compression(),
      topic_property_compression,
      topic_properties.compression,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().log_compression_type.desc()),
      &describe_as_string<model::compression>);

    add_topic_config_if_requested(
      config_keys,
      result,
      config::shard_local_cfg().log_cleanup_policy.name(),
      metadata_cache.get_default_cleanup_policy_bitflags(),
      topic_property_cleanup_policy,
      topic_properties.cleanup_policy_bitflags,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().log_cleanup_policy.desc()),
      &describe_as_string<model::cleanup_policy_bitflags>);

    const std::string_view docstring{
      topic_properties.is_compacted()
        ? config::shard_local_cfg().compacted_log_segment_size.desc()
        : config::shard_local_cfg().log_segment_size.desc()};
    add_topic_config_if_requested(
      config_keys,
      result,
      topic_properties.is_compacted()
        ? config::shard_local_cfg().compacted_log_segment_size.name()
        : config::shard_local_cfg().log_segment_size.name(),
      topic_properties.is_compacted()
        ? metadata_cache.get_default_compacted_topic_segment_size()
        : metadata_cache.get_default_segment_size(),
      topic_property_segment_size,
      topic_properties.segment_size,
      include_synonyms,
      maybe_make_documentation(include_documentation, docstring),
      &describe_as_string<size_t>);

    add_topic_config_if_requested(
      config_keys,
      result,
      config::shard_local_cfg().log_retention_ms.name(),
      metadata_cache.get_default_retention_duration(),
      topic_property_retention_duration,
      topic_properties.retention_duration,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().log_retention_ms.desc()));

    add_topic_config_if_requested(
      config_keys,
      result,
      config::shard_local_cfg().retention_bytes.name(),
      metadata_cache.get_default_retention_bytes(),
      topic_property_retention_bytes,
      topic_properties.retention_bytes,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().retention_bytes.desc()));

    add_topic_config_if_requested(
      config_keys,
      result,
      config::shard_local_cfg().log_message_timestamp_type.name(),
      metadata_cache.get_default_timestamp_type(),
      topic_property_timestamp_type,
      topic_properties.timestamp_type,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().log_message_timestamp_type.desc()),
      &describe_as_string<model::timestamp_type>);

    add_topic_config_if_requested(
      config_keys,
      result,
      config::shard_local_cfg().kafka_batch_max_bytes.name(),
      metadata_cache.get_default_batch_max_bytes(),
      topic_property_max_message_bytes,
      topic_properties.batch_max_bytes,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().kafka_batch_max_bytes.desc()),
      &describe_as_string<uint32_t>);

    add_topic_config_if_requested(
      config_keys,
      result,
      config::shard_local_cfg().write_caching_default.name(),
      config::shard_local_cfg().write_caching_default(),
      topic_property_write_caching,
      (config::shard_local_cfg().write_caching_default()
       == model::write_caching_mode::disabled)
        ? model::write_caching_mode::disabled
        : topic_properties.write_caching,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().write_caching_default.desc()),
      &describe_as_string<model::write_caching_mode>);

    add_topic_config_if_requested(
      config_keys,
      result,
      config::shard_local_cfg().raft_replica_max_pending_flush_bytes.name(),
      config::shard_local_cfg().raft_replica_max_pending_flush_bytes().value_or(
        std::numeric_limits<size_t>::max()),
      topic_property_flush_bytes,
      topic_properties.flush_bytes,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().raft_replica_max_pending_flush_bytes.desc()),
      &describe_as_string<size_t>);

    add_topic_config_if_requested(
      config_keys,
      result,
      config::shard_local_cfg().raft_replica_max_flush_delay_ms.name(),
      config::shard_local_cfg().raft_replica_max_flush_delay_ms(),
      topic_property_flush_ms,
      topic_properties.flush_ms,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().raft_replica_max_flush_delay_ms.desc()),
      &describe_as_string<std::chrono::milliseconds>);

    // Shadow indexing properties
    add_topic_config_if_requested(
      config_keys,
      result,
      topic_property_remote_read,
      model::is_fetch_enabled(
        metadata_cache.get_default_shadow_indexing_mode()),
      topic_property_remote_read,
      topic_properties.shadow_indexing.has_value()
        ? std::make_optional(
            model::is_fetch_enabled(*topic_properties.shadow_indexing))
        : std::nullopt,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().cloud_storage_enable_remote_read.desc()),
      &describe_as_string<bool>,
      true);

    add_topic_config_if_requested(
      config_keys,
      result,
      topic_property_remote_write,
      model::is_archival_enabled(
        metadata_cache.get_default_shadow_indexing_mode()),
      topic_property_remote_write,
      topic_properties.shadow_indexing.has_value()
        ? std::make_optional(
            model::is_archival_enabled(*topic_properties.shadow_indexing))
        : std::nullopt,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().cloud_storage_enable_remote_write.desc()),
      &describe_as_string<bool>,
      true);

    add_topic_config_if_requested(
      config_keys,
      result,
      topic_property_retention_local_target_bytes,
      metadata_cache.get_default_retention_local_target_bytes(),
      topic_property_retention_local_target_bytes,
      topic_properties.retention_local_target_bytes,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().retention_local_target_bytes_default.desc()));

    add_topic_config_if_requested(
      config_keys,
      result,
      topic_property_retention_local_target_ms,
      std::make_optional(
        metadata_cache.get_default_retention_local_target_ms()),
      topic_property_retention_local_target_ms,
      topic_properties.retention_local_target_ms,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().retention_local_target_ms_default.desc()));

    if (config_property_requested(config_keys, topic_property_remote_delete)) {
        add_topic_config<bool>(
          result,
          topic_property_remote_delete,
          storage::ntp_config::default_remote_delete,
          topic_property_remote_delete,
          override_if_not_default(
            std::make_optional<bool>(topic_properties.remote_delete),
            storage::ntp_config::default_remote_delete),
          true,
          maybe_make_documentation(
            include_documentation,
            "Controls whether topic deletion should imply deletion in "
            "S3"),
          [](const bool& b) { return b ? "true" : "false"; });
    }

    add_topic_config_if_requested(
      config_keys,
      result,
      topic_property_segment_ms,
      metadata_cache.get_default_segment_ms(),
      topic_property_segment_ms,
      topic_properties.segment_ms,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().log_segment_ms.desc()));

    if (config::shard_local_cfg().enable_mpx_extensions()) {
        add_topic_config_if_requested(
          config_keys,
          result,
          topic_property_mpx_virtual_cluster_id,
          topic_properties.mpx_virtual_cluster_id,
          include_synonyms,
          maybe_make_documentation(
            include_documentation,
            "Identifier of virtual cluster that the topic is "
            "affiliated with"),
          [](const std::optional<model::vcluster_id> property) {
              if (!property) {
                  return ss::sstring("-");
              }
              return ssx::sformat("{}", (*property)());
          });
    }

    if (config_property_requested(
          config_keys, topic_property_iceberg_enabled)) {
        add_topic_config<bool>(
          result,
          topic_property_iceberg_enabled,
          storage::ntp_config::default_iceberg_enabled,
          topic_property_iceberg_enabled,
          override_if_not_default(
            std::make_optional<bool>(topic_properties.iceberg_enabled),
            storage::ntp_config::default_iceberg_enabled),
          true,
          maybe_make_documentation(
            include_documentation,
            "Iceberg format translation enabled on this topic if true."),
          [](const bool& b) { return b ? "true" : "false"; });
    }

    if (config::shard_local_cfg().development_enable_cloud_topics()) {
        if (config_property_requested(
              config_keys, topic_property_cloud_topic_enabled)) {
            add_topic_config<bool>(
              result,
              topic_property_cloud_topic_enabled,
              false,
              topic_property_cloud_topic_enabled,
              override_if_not_default(
                std::make_optional<bool>(topic_properties.cloud_topic_enabled),
                false),
              include_synonyms,
              maybe_make_documentation(
                include_documentation,
                "Cloud topic enabled on this topic if ture."),
              [](const bool& b) { return b ? "true" : "false"; });
        }
    }

    constexpr std::string_view key_validation
      = "Enable validation of the schema id for keys on a record";
    constexpr std::string_view val_validation
      = "Enable validation of the schema id for values on a record";
    constexpr bool validation_hide_default_override = true;

    switch (config::shard_local_cfg().enable_schema_id_validation()) {
    case pandaproxy::schema_registry::schema_id_validation_mode::compat: {
        add_topic_config_if_requested(
          config_keys,
          result,
          topic_property_record_key_schema_id_validation_compat,
          metadata_cache.get_default_record_key_schema_id_validation(),
          topic_property_record_key_schema_id_validation_compat,
          topic_properties.record_key_schema_id_validation_compat,
          include_synonyms,
          maybe_make_documentation(include_documentation, key_validation),
          &describe_as_string<bool>,
          validation_hide_default_override);

        add_topic_config_if_requested(
          config_keys,
          result,
          topic_property_record_key_subject_name_strategy_compat,
          metadata_cache.get_default_record_key_subject_name_strategy(),
          topic_property_record_key_subject_name_strategy_compat,
          topic_properties.record_key_subject_name_strategy_compat,
          include_synonyms,
          maybe_make_documentation(
            include_documentation,
            fmt::format(
              "The subject name strategy for keys if {} is enabled",
              topic_property_record_key_schema_id_validation_compat)),
          [](auto sns) { return ss::sstring(to_string_view_compat(sns)); },
          validation_hide_default_override);

        add_topic_config_if_requested(
          config_keys,
          result,
          topic_property_record_value_schema_id_validation_compat,
          metadata_cache.get_default_record_value_schema_id_validation(),
          topic_property_record_value_schema_id_validation_compat,
          topic_properties.record_value_schema_id_validation_compat,
          include_synonyms,
          maybe_make_documentation(include_documentation, val_validation),
          &describe_as_string<bool>,
          validation_hide_default_override);

        add_topic_config_if_requested(
          config_keys,
          result,
          topic_property_record_value_subject_name_strategy_compat,
          metadata_cache.get_default_record_value_subject_name_strategy(),
          topic_property_record_value_subject_name_strategy_compat,
          topic_properties.record_value_subject_name_strategy_compat,
          include_synonyms,
          maybe_make_documentation(
            include_documentation,
            fmt::format(
              "The subject name strategy for values if {} is enabled",
              topic_property_record_value_schema_id_validation_compat)),
          [](auto sns) { return ss::sstring(to_string_view_compat(sns)); },
          validation_hide_default_override);
        [[fallthrough]];
    }
    case pandaproxy::schema_registry::schema_id_validation_mode::redpanda: {
        add_topic_config_if_requested(
          config_keys,
          result,
          topic_property_record_key_schema_id_validation,
          metadata_cache.get_default_record_key_schema_id_validation(),
          topic_property_record_key_schema_id_validation,
          topic_properties.record_key_schema_id_validation,
          include_synonyms,
          maybe_make_documentation(include_documentation, key_validation),
          &describe_as_string<bool>,
          validation_hide_default_override);

        add_topic_config_if_requested(
          config_keys,
          result,
          topic_property_record_key_subject_name_strategy,
          metadata_cache.get_default_record_key_subject_name_strategy(),
          topic_property_record_key_subject_name_strategy,
          topic_properties.record_key_subject_name_strategy,
          include_synonyms,
          maybe_make_documentation(
            include_documentation,
            fmt::format(
              "The subject name strategy for keys if {} is enabled",
              topic_property_record_key_schema_id_validation)),
          &describe_as_string<
            pandaproxy::schema_registry::subject_name_strategy>,
          validation_hide_default_override);

        add_topic_config_if_requested(
          config_keys,
          result,
          topic_property_record_value_schema_id_validation,
          metadata_cache.get_default_record_value_schema_id_validation(),
          topic_property_record_value_schema_id_validation,
          topic_properties.record_value_schema_id_validation,
          include_synonyms,
          maybe_make_documentation(include_documentation, val_validation),
          &describe_as_string<bool>,
          validation_hide_default_override);

        add_topic_config_if_requested(
          config_keys,
          result,
          topic_property_record_value_subject_name_strategy,
          metadata_cache.get_default_record_value_subject_name_strategy(),
          topic_property_record_value_subject_name_strategy,
          topic_properties.record_value_subject_name_strategy,
          include_synonyms,
          maybe_make_documentation(
            include_documentation,
            fmt::format(
              "The subject name strategy for values if {} is enabled",
              topic_property_record_value_schema_id_validation)),
          &describe_as_string<
            pandaproxy::schema_registry::subject_name_strategy>,
          validation_hide_default_override);
        [[fallthrough]];
    }
    case pandaproxy::schema_registry::schema_id_validation_mode::none: {
        break;
    }
    }

    add_topic_config_if_requested(
      config_keys,
      result,
      topic_property_initial_retention_local_target_bytes,
      metadata_cache.get_default_initial_retention_local_target_bytes(),
      topic_property_initial_retention_local_target_bytes,
      topic_properties.initial_retention_local_target_bytes,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg()
          .initial_retention_local_target_bytes_default.desc()));

    add_topic_config_if_requested(
      config_keys,
      result,
      topic_property_initial_retention_local_target_ms,
      metadata_cache.get_default_initial_retention_local_target_ms(),
      topic_property_initial_retention_local_target_ms,
      topic_properties.initial_retention_local_target_ms,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg()
          .initial_retention_local_target_ms_default.desc()));

    add_topic_config_if_requested(
      config_keys,
      result,
      config::shard_local_cfg().default_leaders_preference.name(),
      config::shard_local_cfg().default_leaders_preference(),
      topic_property_leaders_preference,
      topic_properties.leaders_preference,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        "Preferred location (e.g. rack) for partition leaders of this topic."),
      &describe_as_string<config::leaders_preference>);

    return result;
}

config_response_container_t make_broker_configs(
  const config_key_t& config_keys,
  bool include_synonyms,
  bool include_documentation) {
    config_response_container_t result;

    add_broker_config_if_requested(
      config_keys,
      result,
      "listeners",
      config::node().kafka_api,
      include_synonyms,
      maybe_make_documentation(
        include_documentation, config::node().kafka_api.desc()),
      &kafka_authn_endpoint_format);

    add_broker_config_if_requested(
      config_keys,
      result,
      "advertised.listeners",
      config::node().advertised_kafka_api_property(),
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::node().advertised_kafka_api_property().desc()),
      &kafka_endpoint_format);

    add_broker_config_if_requested(
      config_keys,
      result,
      "log.segment.bytes",
      config::shard_local_cfg().log_segment_size,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().log_segment_size.desc()),
      &describe_as_string<size_t>);

    add_broker_config_if_requested(
      config_keys,
      result,
      "log.retention.bytes",
      config::shard_local_cfg().retention_bytes,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().retention_bytes.desc()),
      [](std::optional<size_t> sz) {
          return ssx::sformat("{}", sz ? sz.value() : -1);
      });

    add_broker_config_if_requested(
      config_keys,
      result,
      "log.retention.ms",
      config::shard_local_cfg().log_retention_ms,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().log_retention_ms.desc()),
      [](const std::optional<std::chrono::milliseconds>& ret) {
          return ssx::sformat("{}", ret.value_or(-1ms).count());
      });

    add_broker_config_if_requested(
      config_keys,
      result,
      "num.partitions",
      config::shard_local_cfg().default_topic_partitions,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().default_topic_partitions.desc()),
      &describe_as_string<int32_t>);

    add_broker_config_if_requested(
      config_keys,
      result,
      "default.replication.factor",
      config::shard_local_cfg().default_topic_replication,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().default_topic_replication.desc()),
      &describe_as_string<int16_t>);

    add_broker_config_if_requested(
      config_keys,
      result,
      "log.dirs",
      config::node().data_directory,
      include_synonyms,
      maybe_make_documentation(
        include_documentation, config::node().data_directory.desc()),
      [](const config::data_directory_path& path) {
          return path.as_sstring();
      });

    add_broker_config_if_requested(
      config_keys,
      result,
      "auto.create.topics.enable",
      config::shard_local_cfg().auto_create_topics_enabled,
      include_synonyms,
      maybe_make_documentation(
        include_documentation,
        config::shard_local_cfg().auto_create_topics_enabled.desc()),
      &describe_as_string<bool>);

    return result;
}

describe_configs_resource_result config_response::to_describe_config() {
    return {
      .name = name,
      .value = value,
      .read_only = read_only,
      .is_default = is_default,
      .config_source = config_source,
      .is_sensitive = is_sensitive,
      .synonyms = synonyms,
      .config_type = config_type,
      .documentation = documentation,
    };
};

creatable_topic_configs config_response::to_create_config() {
    return {
      .name = name,
      .value = value,
      .read_only = read_only,
      .config_source = config_source,
      .is_sensitive = is_sensitive,
    };
};

} // namespace kafka
