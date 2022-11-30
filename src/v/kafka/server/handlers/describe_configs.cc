// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/describe_configs.h"

#include "cluster/metadata_cache.h"
#include "config/configuration.h"
#include "config/data_directory_path.h"
#include "config/node_config.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "reflection/type_traits.h"
#include "security/acl.h"
#include "ssx/sformat.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ranges.h>

#include <charconv>
#include <string_view>
#include <type_traits>

namespace kafka {

static bool config_property_requested(
  const std::optional<std::vector<ss::sstring>>& configuration_keys,
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
  const describe_configs_resource& resource,
  describe_configs_result& result,
  std::string_view name,
  T value,
  describe_configs_source source) {
    if (config_property_requested(resource.configuration_keys, name)) {
        add_config(result, name, value, source);
    }
}

template<typename T>
static ss::sstring describe_as_string(const T& t) {
    return ssx::sformat("{}", t);
}

// property_config_type maps the datatype for a config property to
// describe_configs_type. Currently class_type and password are not used in
// Redpanda so we do not include checks for those types. You may find a similar
// mapping in Apache Kafka at
// https://github.com/apache/kafka/blob/be032735b39360df1a6de1a7feea8b4336e5bcc0/core/src/main/scala/kafka/server/ConfigHelper.scala
// Type sizes are based on Java and taken from
// https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
template<typename T>
consteval describe_configs_type property_config_type() {
    if constexpr (reflection::is_std_optional<T>) {
        return property_config_type<typename T::value_type>();
    } else if constexpr (std::is_same_v<T, bool>) {
        return describe_configs_type::boolean;
    } else if constexpr (std::is_same_v<T, ss::sstring>) {
        return describe_configs_type::string;
    } else if constexpr (
      std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t>) {
        return describe_configs_type::int_type;
    } else if constexpr (
      std::is_same_v<T, int16_t> || std::is_same_v<T, uint16_t>) {
        return describe_configs_type::short_type;
    } else if constexpr (
      std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>) {
        return describe_configs_type::long_type;
    } else if constexpr (std::is_floating_point_v<T>) {
        return describe_configs_type::double_type;
    } else if constexpr (reflection::is_std_vector<T>) {
        return describe_configs_type::list;
    } else if constexpr (std::is_same_v<T, model::compression>) {
        return describe_configs_type::string;
    } else if constexpr (std::is_same_v<T, model::cleanup_policy_bitflags>) {
        return describe_configs_type::string;
    } else if constexpr (std::is_same_v<T, std::chrono::seconds>) {
        return describe_configs_type::
          long_type; // Because seconds is atleast a 35-bit signed integral
                     // https://en.cppreference.com/w/cpp/chrono/duration
    } else if constexpr (std::is_same_v<T, std::chrono::milliseconds>) {
        return describe_configs_type::
          long_type; // Because milliseconds is atleast a 45-bit signed integral
                     // https://en.cppreference.com/w/cpp/chrono/duration
    } else if constexpr (std::is_same_v<T, model::timestamp_type>) {
        return describe_configs_type::string;
    } else if constexpr (std::is_same_v<T, config::data_directory_path>) {
        return describe_configs_type::string;
    } else if constexpr (std::is_same_v<T, v8_engine::data_policy>) {
        return describe_configs_type::string;
    } else {
        static_assert(
          config::detail::dependent_false<T>::value,
          "Type name is not supported in describe_configs_type");
    }
}

template<typename T, typename Func>
static void add_broker_config(
  describe_configs_result& result,
  std::string_view name,
  const config::property<T>& property,
  bool include_synonyms,
  bool include_documentation,
  ss::sstring documentation,
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

    describe_configs_type config_type = property_config_type<T>();

    result.configs.push_back(describe_configs_resource_result{
      .name = ss::sstring(name),
      .value = describe_f(property.value()),
      .config_source = src,
      .synonyms = std::move(synonyms),
      .config_type = config_type,
      .documentation = include_documentation ? std::make_optional(documentation)
                                             : std::nullopt,
    });
}

template<typename T, typename Func>
static void add_broker_config_if_requested(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  std::string_view name,
  const config::property<T>& property,
  bool include_synonyms,
  bool include_documentation,
  ss::sstring documentation,
  Func&& describe_f) {
    if (config_property_requested(resource.configuration_keys, name)) {
        add_broker_config(
          result,
          name,
          property,
          include_synonyms,
          include_documentation,
          documentation,
          std::forward<Func>(describe_f));
    }
}

template<typename T, typename Func>
static void add_topic_config(
  describe_configs_result& result,
  std::string_view default_name,
  const T& default_value,
  std::string_view override_name,
  const std::optional<T>& overrides,
  bool include_synonyms,
  bool include_documentation,
  ss::sstring documentation,
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

    describe_configs_type config_type = property_config_type<T>();

    result.configs.push_back(describe_configs_resource_result{
      .name = ss::sstring(override_name),
      .value = describe_f(overrides.value_or(default_value)),
      .config_source = src,
      .synonyms = std::move(synonyms),
      .config_type = config_type,
      .documentation = include_documentation ? std::make_optional(documentation)
                                             : std::nullopt,
    });
}

template<typename T, typename Func>
static void add_topic_config_if_requested(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  std::string_view default_name,
  const T& default_value,
  std::string_view override_name,
  const std::optional<T>& overrides,
  bool include_synonyms,
  bool include_documentation,
  ss::sstring documentation,
  Func&& describe_f) {
    if (config_property_requested(resource.configuration_keys, override_name)) {
        add_topic_config(
          result,
          default_name,
          default_value,
          override_name,
          overrides,
          include_synonyms,
          include_documentation,
          documentation,
          std::forward<Func>(describe_f));
    }
}

template<typename T>
static void add_topic_config(
  describe_configs_result& result,
  std::string_view default_name,
  const std::optional<T>& default_value,
  std::string_view override_name,
  const tristate<T>& overrides,
  bool include_synonyms,
  bool include_documentation,
  ss::sstring documentation) {
    std::optional<ss::sstring> override_value;
    if (overrides.is_disabled()) {
        override_value = "-1";
    } else if (overrides.has_value()) {
        override_value = ssx::sformat("{}", overrides.value());
    }

    add_topic_config(
      result,
      default_name,
      default_value ? ssx::sformat("{}", default_value.value()) : "-1",
      override_name,
      override_value,
      include_synonyms,
      include_documentation,
      documentation,
      [](const ss::sstring& s) { return s; });
}

template<typename T>
static void add_topic_config_if_requested(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  std::string_view default_name,
  const std::optional<T>& default_value,
  std::string_view override_name,
  const tristate<T>& overrides,
  bool include_synonyms,
  bool include_documentation,
  ss::sstring documentation) {
    if (config_property_requested(resource.configuration_keys, override_name)) {
        add_topic_config(
          result,
          default_name,
          default_value,
          override_name,
          overrides,
          include_synonyms,
          include_documentation,
          documentation);
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

static void report_broker_config(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  bool include_synonyms,
  bool include_documentation) {
    if (!result.resource_name.empty()) {
        int32_t broker_id = -1;
        auto res = std::from_chars(
          result.resource_name.data(),
          result.resource_name.data() + result.resource_name.size(), // NOLINT
          broker_id);
        if (res.ec == std::errc()) {
            if (broker_id != *config::node().node_id()) {
                result.error_code = error_code::invalid_request;
                result.error_message = ssx::sformat(
                  "Unexpected broker id {} expected {}",
                  broker_id,
                  *config::node().node_id());
                return;
            }
        } else {
            result.error_code = error_code::invalid_request;
            result.error_message = ssx::sformat(
              "Broker id must be an integer but received {}",
              result.resource_name);
            return;
        }
    }

    add_broker_config_if_requested(
      resource,
      result,
      "listeners",
      config::node().kafka_api,
      include_synonyms,
      include_documentation,
      ss::sstring{config::node().kafka_api.desc()},
      &kafka_authn_endpoint_format);

    add_broker_config_if_requested(
      resource,
      result,
      "advertised.listeners",
      config::node().advertised_kafka_api_property(),
      include_synonyms,
      include_documentation,
      ss::sstring{config::node().advertised_kafka_api_property().desc()},
      &kafka_endpoint_format);

    add_broker_config_if_requested(
      resource,
      result,
      "log.segment.bytes",
      config::shard_local_cfg().log_segment_size,
      include_synonyms,
      include_documentation,
      ss::sstring{config::shard_local_cfg().log_segment_size.desc()},
      &describe_as_string<size_t>);

    add_broker_config_if_requested(
      resource,
      result,
      "log.retention.bytes",
      config::shard_local_cfg().retention_bytes,
      include_synonyms,
      include_documentation,
      ss::sstring{config::shard_local_cfg().retention_bytes.desc()},
      [](std::optional<size_t> sz) {
          return ssx::sformat("{}", sz ? sz.value() : -1);
      });

    add_broker_config_if_requested(
      resource,
      result,
      "log.retention.ms",
      config::shard_local_cfg().delete_retention_ms,
      include_synonyms,
      include_documentation,
      ss::sstring{config::shard_local_cfg().delete_retention_ms.desc()},
      [](const std::optional<std::chrono::milliseconds>& ret) {
          return ssx::sformat("{}", ret.value_or(-1ms).count());
      });

    add_broker_config_if_requested(
      resource,
      result,
      "num.partitions",
      config::shard_local_cfg().default_topic_partitions,
      include_synonyms,
      include_documentation,
      ss::sstring{config::shard_local_cfg().default_topic_partitions.desc()},
      &describe_as_string<int32_t>);

    add_broker_config_if_requested(
      resource,
      result,
      "default.replication.factor",
      config::shard_local_cfg().default_topic_replication,
      include_synonyms,
      include_documentation,
      ss::sstring{config::shard_local_cfg().default_topic_replication.desc()},
      &describe_as_string<int16_t>);

    add_broker_config_if_requested(
      resource,
      result,
      "log.dirs",
      config::node().data_directory,
      include_synonyms,
      include_documentation,
      ss::sstring{config::node().data_directory.desc()},
      [](const config::data_directory_path& path) {
          return path.as_sstring();
      });

    add_broker_config_if_requested(
      resource,
      result,
      "auto.create.topics.enable",
      config::shard_local_cfg().auto_create_topics_enabled,
      include_synonyms,
      include_documentation,
      ss::sstring{config::shard_local_cfg().auto_create_topics_enabled.desc()},
      &describe_as_string<bool>);
}

int64_t describe_retention_duration(
  tristate<std::chrono::milliseconds>& overrides,
  std::optional<std::chrono::milliseconds> def) {
    if (overrides.is_disabled()) {
        return -1;
    }
    if (overrides.has_value()) {
        return overrides.value().count();
    }

    return def ? def->count() : -1;
}
int64_t describe_retention_bytes(
  tristate<size_t>& overrides, std::optional<size_t> def) {
    if (overrides.is_disabled()) {
        return -1;
    }
    if (overrides.has_value()) {
        return overrides.value();
    }

    return def.value_or(-1);
}

template<>
ss::future<response_ptr> describe_configs_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    describe_configs_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(klog.trace, "Handling request {}", request);

    describe_configs_response response;
    response.data.results.reserve(request.data.resources.size());
    bool cluster_authorized = ctx.authorized(
      security::acl_operation::describe_configs,
      security::default_cluster_name);

    for (auto& resource : request.data.resources) {
        response.data.results.push_back(describe_configs_result{
          .error_code = error_code::none,
          .resource_type = resource.resource_type,
          .resource_name = resource.resource_name,
        });

        auto& result = response.data.results.back();

        switch (resource.resource_type) {
        case config_resource_type::topic: {
            model::topic_namespace topic(
              model::kafka_namespace, model::topic(resource.resource_name));

            auto err = model::validate_kafka_topic_name(topic.tp);
            if (err) {
                result.error_code = error_code::invalid_topic_exception;
                continue;
            }

            auto topic_config = ctx.metadata_cache().get_topic_cfg(topic);
            if (!topic_config) {
                result.error_code = error_code::unknown_topic_or_partition;
                continue;
            }

            if (!ctx.authorized(
                  security::acl_operation::describe_configs, topic.tp)) {
                result.error_code = error_code::topic_authorization_failed;
                continue;
            }

            /**
             * Kafka properties
             */
            add_topic_config_if_requested(
              resource,
              result,
              config::shard_local_cfg().log_compression_type.name(),
              ctx.metadata_cache().get_default_compression(),
              topic_property_compression,
              topic_config->properties.compression,
              request.data.include_synonyms,
              request.data.include_documentation,
              ss::sstring{
                config::shard_local_cfg().log_compression_type.desc()},
              &describe_as_string<model::compression>);

            add_topic_config_if_requested(
              resource,
              result,
              config::shard_local_cfg().log_cleanup_policy.name(),
              ctx.metadata_cache().get_default_cleanup_policy_bitflags(),
              topic_property_cleanup_policy,
              topic_config->properties.cleanup_policy_bitflags,
              request.data.include_synonyms,
              request.data.include_documentation,
              ss::sstring{config::shard_local_cfg().log_cleanup_policy.desc()},
              &describe_as_string<model::cleanup_policy_bitflags>);

            ss::sstring docstring{
              topic_config->properties.is_compacted()
                ? config::shard_local_cfg().compacted_log_segment_size.desc()
                : config::shard_local_cfg().log_segment_size.desc()};
            add_topic_config_if_requested(
              resource,
              result,
              topic_config->properties.is_compacted()
                ? config::shard_local_cfg().compacted_log_segment_size.name()
                : config::shard_local_cfg().log_segment_size.name(),
              topic_config->properties.is_compacted()
                ? ctx.metadata_cache()
                    .get_default_compacted_topic_segment_size()
                : ctx.metadata_cache().get_default_segment_size(),
              topic_property_segment_size,
              topic_config->properties.segment_size,
              request.data.include_synonyms,
              request.data.include_documentation,
              docstring,
              &describe_as_string<size_t>);

            add_topic_config_if_requested(
              resource,
              result,
              config::shard_local_cfg().delete_retention_ms.name(),
              ctx.metadata_cache().get_default_retention_duration(),
              topic_property_retention_duration,
              topic_config->properties.retention_duration,
              request.data.include_synonyms,
              request.data.include_documentation,
              ss::sstring{
                config::shard_local_cfg().delete_retention_ms.desc()});

            add_topic_config_if_requested(
              resource,
              result,
              config::shard_local_cfg().retention_bytes.name(),
              ctx.metadata_cache().get_default_retention_bytes(),
              topic_property_retention_bytes,
              topic_config->properties.retention_bytes,
              request.data.include_synonyms,
              request.data.include_documentation,
              ss::sstring{config::shard_local_cfg().retention_bytes.desc()});

            add_topic_config_if_requested(
              resource,
              result,
              config::shard_local_cfg().log_message_timestamp_type.name(),
              ctx.metadata_cache().get_default_timestamp_type(),
              topic_property_timestamp_type,
              topic_config->properties.timestamp_type,
              request.data.include_synonyms,
              request.data.include_documentation,
              ss::sstring{
                config::shard_local_cfg().log_message_timestamp_type.desc()},
              &describe_as_string<model::timestamp_type>);

            add_topic_config_if_requested(
              resource,
              result,
              config::shard_local_cfg().kafka_batch_max_bytes.name(),
              ctx.metadata_cache().get_default_batch_max_bytes(),
              topic_property_max_message_bytes,
              topic_config->properties.batch_max_bytes,
              request.data.include_synonyms,
              request.data.include_documentation,
              ss::sstring{
                config::shard_local_cfg().kafka_batch_max_bytes.desc()},
              &describe_as_string<uint32_t>);

            // Shadow indexing properties
            add_topic_config_if_requested(
              resource,
              result,
              topic_property_remote_read,
              model::is_fetch_enabled(
                ctx.metadata_cache().get_default_shadow_indexing_mode()),
              topic_property_remote_read,
              topic_config->properties.shadow_indexing.has_value()
                ? std::make_optional(model::is_fetch_enabled(
                  *topic_config->properties.shadow_indexing))
                : std::nullopt,
              request.data.include_synonyms,
              request.data.include_documentation,
              ss::sstring{config::shard_local_cfg()
                            .cloud_storage_enable_remote_read.desc()},
              &describe_as_string<bool>);

            add_topic_config_if_requested(
              resource,
              result,
              topic_property_remote_write,
              model::is_archival_enabled(
                ctx.metadata_cache().get_default_shadow_indexing_mode()),
              topic_property_remote_write,
              topic_config->properties.shadow_indexing.has_value()
                ? std::make_optional(model::is_archival_enabled(
                  *topic_config->properties.shadow_indexing))
                : std::nullopt,
              request.data.include_synonyms,
              request.data.include_documentation,
              ss::sstring{config::shard_local_cfg()
                            .cloud_storage_enable_remote_write.desc()},
              &describe_as_string<bool>);

            add_topic_config_if_requested(
              resource,
              result,
              topic_property_retention_local_target_bytes,
              ctx.metadata_cache().get_default_retention_local_target_bytes(),
              topic_property_retention_local_target_bytes,
              topic_config->properties.retention_local_target_bytes,
              request.data.include_synonyms,
              request.data.include_documentation,
              ss::sstring{config::shard_local_cfg()
                            .retention_local_target_bytes_default.desc()});

            add_topic_config_if_requested(
              resource,
              result,
              topic_property_retention_local_target_ms,
              std::make_optional(
                ctx.metadata_cache().get_default_retention_local_target_ms()),
              topic_property_retention_local_target_ms,
              topic_config->properties.retention_local_target_ms,
              request.data.include_synonyms,
              request.data.include_documentation,
              ss::sstring{config::shard_local_cfg()
                            .retention_local_target_ms_default.desc()});

            if (config_property_requested(
                  resource.configuration_keys, topic_property_remote_delete)) {
                // Doc string taken from src/v/storage/ntp_config.h
                ss::sstring documentation = "Controls whether topic deletion "
                                            "should imply deletion in S3";
                add_topic_config<bool>(
                  result,
                  topic_property_remote_delete,
                  storage::ntp_config::default_remote_delete,
                  topic_property_remote_delete,
                  std::make_optional<bool>(
                    topic_config->properties.remote_delete),
                  true,
                  request.data.include_documentation,
                  documentation,
                  [](const bool& b) { return b ? "true" : "false"; });
            }

            // Data-policy property
            ss::sstring property_name = "redpanda.datapolicy";
            // Doc string taken from src/v/v8_engine/data_policy.h
            ss::sstring documentation = "Datapolicy property for v8_engine";
            add_topic_config_if_requested(
              resource,
              result,
              property_name,
              v8_engine::data_policy("", ""),
              property_name,
              ctx.data_policy_table().get_data_policy(topic),
              request.data.include_synonyms,
              request.data.include_documentation,
              documentation,
              &describe_as_string<v8_engine::data_policy>);

            break;
        }

        case config_resource_type::broker:
            if (!cluster_authorized) {
                result.error_code = error_code::cluster_authorization_failed;
                continue;
            }
            report_broker_config(
              resource,
              result,
              request.data.include_synonyms,
              request.data.include_documentation);
            break;

        // resource types not yet handled
        case config_resource_type::broker_logger:
            result.error_code = error_code::invalid_request;
        }
    }

    return ctx.respond(std::move(response));
}

} // namespace kafka
