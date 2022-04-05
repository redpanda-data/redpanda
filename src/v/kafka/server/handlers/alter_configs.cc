// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/alter_configs.h"

#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/alter_configs_request.h"
#include "kafka/protocol/schemata/alter_configs_response.h"
#include "kafka/server/handlers/configs/config_utils.h"
#include "kafka/server/handlers/details/data_policy.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "utils/string_switch.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <absl/container/node_hash_set.h>
#include <fmt/ostream.h>

#include <string_view>

namespace kafka {
template<typename T>
void parse_and_set_optional(
  cluster::property_update<std::optional<T>>& property_update,
  const std::optional<ss::sstring>& value) {
    if (!value) {
        property_update.value = std::nullopt;
    }

    property_update.value = boost::lexical_cast<T>(*value);
}

template<typename T>
void parse_and_set_tristate(
  cluster::property_update<tristate<T>>& property_update,
  const std::optional<ss::sstring>& value) {
    if (!value) {
        property_update.value = tristate<T>(std::nullopt);
    }

    auto parsed = boost::lexical_cast<int64_t>(*value);
    if (parsed <= 0) {
        property_update.value = tristate<T>{};
    } else {
        property_update.value = tristate<T>(std::make_optional<T>(parsed));
    }
}

static void parse_and_set_shadow_indexing_mode(
  cluster::property_update<std::optional<model::shadow_indexing_mode>>&
    property_update,
  const std::optional<ss::sstring>& value,
  model::shadow_indexing_mode enabled_value) {
    if (!value) {
        property_update.value = model::shadow_indexing_mode::disabled;
    }
    property_update.value
      = string_switch<model::shadow_indexing_mode>(*value)
          .match("no", model::shadow_indexing_mode::disabled)
          .match("false", model::shadow_indexing_mode::disabled)
          .match("yes", enabled_value)
          .match("true", enabled_value)
          .default_match(model::shadow_indexing_mode::disabled);
}

void check_data_policy(std::string_view property_name) {
    if (
      property_name == topic_property_data_policy_function_name
      || property_name == topic_property_data_policy_script_name) {
        throw v8_engine::data_policy_exeption(
          "Alter config does not support data-policy");
    }
}

checked<cluster::topic_properties_update, alter_configs_resource_response>
create_topic_properties_update(alter_configs_resource& resource) {
    model::topic_namespace tp_ns(
      model::kafka_namespace, model::topic(resource.resource_name));
    cluster::topic_properties_update update(tp_ns);
    /**
     * Alter topic configuration should override topic properties with values
     * sent in the request, if given resource value isn't set in the request,
     * override for this value has to be removed. We override all defaults to
     * set, even if value for given property isn't set it will override
     * configuration in topic table
     */
    update.properties.cleanup_policy_bitflags.op
      = cluster::incremental_update_operation::set;
    update.properties.compaction_strategy.op
      = cluster::incremental_update_operation::set;
    update.properties.compression.op
      = cluster::incremental_update_operation::set;
    update.properties.segment_size.op
      = cluster::incremental_update_operation::set;
    update.properties.timestamp_type.op
      = cluster::incremental_update_operation::set;
    update.properties.retention_bytes.op
      = cluster::incremental_update_operation::set;
    update.properties.retention_duration.op
      = cluster::incremental_update_operation::set;
    update.properties.shadow_indexing.op
      = cluster::incremental_update_operation::set;
    update.custom_properties.data_policy.op
      = cluster::incremental_update_operation::none;

    for (auto& cfg : resource.configs) {
        try {
            if (cfg.name == topic_property_cleanup_policy) {
                parse_and_set_optional(
                  update.properties.cleanup_policy_bitflags, cfg.value);
                continue;
            }
            if (cfg.name == topic_property_compaction_strategy) {
                parse_and_set_optional(
                  update.properties.compaction_strategy, cfg.value);
                continue;
            }
            if (cfg.name == topic_property_compression) {
                parse_and_set_optional(
                  update.properties.compression, cfg.value);
                continue;
            }
            if (cfg.name == topic_property_segment_size) {
                parse_and_set_optional(
                  update.properties.segment_size, cfg.value);
                continue;
            }
            if (cfg.name == topic_property_timestamp_type) {
                parse_and_set_optional(
                  update.properties.timestamp_type, cfg.value);
                continue;
            }
            if (cfg.name == topic_property_retention_bytes) {
                parse_and_set_tristate(
                  update.properties.retention_bytes, cfg.value);
                continue;
            }
            if (cfg.name == topic_property_remote_write) {
                auto set_value = update.properties.shadow_indexing.value
                                   ? model::add_shadow_indexing_flag(
                                     *update.properties.shadow_indexing.value,
                                     model::shadow_indexing_mode::archival)
                                   : model::shadow_indexing_mode::archival;
                parse_and_set_shadow_indexing_mode(
                  update.properties.shadow_indexing, cfg.value, set_value);
                continue;
            }
            if (cfg.name == topic_property_remote_read) {
                auto set_value = update.properties.shadow_indexing.value
                                   ? model::add_shadow_indexing_flag(
                                     *update.properties.shadow_indexing.value,
                                     model::shadow_indexing_mode::fetch)
                                   : model::shadow_indexing_mode::fetch;
                parse_and_set_shadow_indexing_mode(
                  update.properties.shadow_indexing, cfg.value, set_value);
                continue;
            }
            if (cfg.name == topic_property_retention_duration) {
                parse_and_set_tristate(
                  update.properties.retention_duration, cfg.value);
                continue;
            }
            (check_data_policy(cfg.name));
        } catch (const boost::bad_lexical_cast& e) {
            return make_error_alter_config_resource_response<
              alter_configs_resource_response>(
              resource,
              error_code::invalid_config,
              fmt::format(
                "unable to parse property {} value {}", cfg.name, cfg.value));
        } catch (const v8_engine::data_policy_exeption& e) {
            return make_error_alter_config_resource_response<
              alter_configs_resource_response>(
              resource,
              error_code::invalid_config,
              fmt::format(
                "unable to parse property {}, value{}, error {}",
                cfg.name,
                cfg.value,
                e.what()));
        }

        // Unsupported property, return error
        return make_error_alter_config_resource_response<
          alter_configs_resource_response>(
          resource,
          error_code::invalid_config,
          fmt::format("invalid topic property: {}", cfg.name));
    }

    return update;
}

static ss::future<std::vector<alter_configs_resource_response>>
alter_topic_configuration(
  request_context& ctx,
  std::vector<alter_configs_resource> resources,
  bool validate_only) {
    return do_alter_topics_configuration<
      alter_configs_resource,
      alter_configs_resource_response>(
      ctx, std::move(resources), validate_only, [](alter_configs_resource& r) {
          return create_topic_properties_update(r);
      });
}

static ss::future<std::vector<alter_configs_resource_response>>
alter_broker_configuartion(std::vector<alter_configs_resource> resources) {
    return unsupported_broker_configuration<
      alter_configs_resource,
      alter_configs_resource_response>(
      std::move(resources),
      "changing broker properties isn't supported via this "
      "API. Try using kafka incremental config API or "
      "redpanda admin API.");
}

template<>
ss::future<response_ptr> alter_configs_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    alter_configs_request request;
    request.decode(ctx.reader(), ctx.header().version);

    auto groupped = group_alter_config_resources(
      std::move(request.data.resources));

    auto unauthorized_responsens = authorize_alter_config_resources<
      alter_configs_resource,
      alter_configs_resource_response>(ctx, groupped);

    std::vector<ss::future<std::vector<alter_configs_resource_response>>>
      futures;
    futures.reserve(2);
    futures.push_back(alter_topic_configuration(
      ctx, std::move(groupped.topic_changes), request.data.validate_only));
    futures.push_back(
      alter_broker_configuartion(std::move(groupped.broker_changes)));

    auto ret = co_await ss::when_all_succeed(futures.begin(), futures.end());
    // include authorization errors
    ret.push_back(std::move(unauthorized_responsens));

    co_return co_await ctx.respond(
      assemble_alter_config_response<
        alter_configs_response,
        alter_configs_resource_response>(std::move(ret)));
}

} // namespace kafka
