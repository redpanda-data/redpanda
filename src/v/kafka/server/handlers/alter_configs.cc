// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/alter_configs.h"

#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/alter_configs_response.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "kafka/types.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <absl/container/node_hash_set.h>
#include <fmt/ostream.h>

#include <string_view>

namespace kafka {
/**
 * Groupped alter_configs_resources
 *
 * NOTE:
 * We do not have to differentiate between broker and broker_logger as
 * broker_logger is deprecated it will be enough to generate error response.
 */
struct groupped_resources {
    std::vector<alter_configs_resource> topic_changes;
    std::vector<alter_configs_resource> broker_changes;
};

groupped_resources
group_alter_config_resources(std::vector<alter_configs_resource> req) {
    groupped_resources ret;
    for (auto& res : req) {
        switch (config_resource_type(res.resource_type)) {
        case config_resource_type::topic:
            ret.topic_changes.push_back(std::move(res));
            break;
        default:
            ret.broker_changes.push_back(std::move(res));
        };
    }
    return ret;
}

alter_configs_response assemble_alter_config_response(
  std::vector<std::vector<alter_configs_resource_response>> responses) {
    alter_configs_response response;
    for (auto& v : responses) {
        std::move(
          v.begin(), v.end(), std::back_inserter(response.data.responses));
    }

    return response;
}

alter_configs_resource_response make_error_alter_config_resource_response(
  alter_configs_resource& resource,
  kafka::error_code err,
  std::optional<ss::sstring> msg = {}) {
    return alter_configs_resource_response{
      .error_code = err,
      .error_message = std::move(msg),
      .resource_type = resource.resource_type,
      .resource_name = resource.resource_name};
}

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
            if (cfg.name == topic_property_retention_duration) {
                parse_and_set_tristate(
                  update.properties.retention_duration, cfg.value);
                continue;
            }
        } catch (const boost::bad_lexical_cast& e) {
            return make_error_alter_config_resource_response(
              resource,
              error_code::invalid_config,
              fmt::format(
                "unable to parse property {} value {}", cfg.name, cfg.value));
        }

        // Unsupported property, return error
        return make_error_alter_config_resource_response(
          resource,
          error_code::invalid_config,
          fmt::format("invalid topic property: {}", cfg.name));
    }

    return update;
}

ss::future<std::vector<alter_configs_resource_response>>
alter_topics_configuration(
  request_context& ctx,
  std::vector<alter_configs_resource> resources,
  bool validate_only) {
    std::vector<alter_configs_resource_response> responses;
    responses.reserve(resources.size());

    absl::node_hash_set<ss::sstring> topic_names;
    auto valid_end = std::stable_partition(
      resources.begin(),
      resources.end(),
      [&topic_names](alter_configs_resource& r) {
          return !topic_names.contains(r.resource_name);
      });

    for (auto& r : boost::make_iterator_range(valid_end, resources.end())) {
        responses.push_back(make_error_alter_config_resource_response(
          r,
          error_code::invalid_config,
          "duplicated topic {} alter config request"));
    }
    std::vector<cluster::topic_properties_update> updates;
    for (auto& r : boost::make_iterator_range(resources.begin(), valid_end)) {
        auto res = create_topic_properties_update(r);
        if (res.has_error()) {
            responses.push_back(std::move(res.error()));
        } else {
            updates.push_back(std::move(res.value()));
        }
    }

    if (validate_only) {
        // all pending updates are valid, just generate responses
        for (auto& u : updates) {
            responses.push_back(alter_configs_resource_response{
              .error_code = error_code::none,
              .resource_type = static_cast<int8_t>(config_resource_type::topic),
              .resource_name = u.tp_ns.tp,
            });
        }

        co_return responses;
    }

    auto update_results
      = co_await ctx.topics_frontend().update_topic_properties(
        std::move(updates),
        model::timeout_clock::now()
          + config::shard_local_cfg().alter_topic_cfg_timeout_ms());
    for (auto& res : update_results) {
        responses.push_back(alter_configs_resource_response{
          .error_code = error_code::none,
          .resource_type = static_cast<int8_t>(config_resource_type::topic),
          .resource_name = res.tp_ns.tp(),
        });
    }
    co_return responses;
}

ss::future<std::vector<alter_configs_resource_response>>
alter_broker_configuartion(std::vector<alter_configs_resource> resources) {
    // for now we do not support altering any of brokers config, generate errors
    std::vector<alter_configs_resource_response> responses;
    responses.reserve(resources.size());
    std::transform(
      resources.begin(),
      resources.end(),
      std::back_inserter(responses),
      [](alter_configs_resource& resource) {
          return make_error_alter_config_resource_response(
            resource,
            error_code::invalid_config,
            fmt::format(
              "changing '{}' broker property isn't currently supported",
              resource.resource_name));
      });

    co_return responses;
}

template<>
ss::future<response_ptr> alter_configs_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    alter_configs_request request;
    request.decode(ctx.reader(), ctx.header().version);

    auto groupped = group_alter_config_resources(
      std::move(request.data.resources));

    std::vector<ss::future<std::vector<alter_configs_resource_response>>>
      futures;
    futures.reserve(2);
    futures.push_back(alter_topics_configuration(
      ctx, std::move(groupped.topic_changes), request.data.validate_only));
    futures.push_back(
      alter_broker_configuartion(std::move(groupped.broker_changes)));
    auto ret = co_await ss::when_all_succeed(futures.begin(), futures.end());
    co_return co_await ctx.respond(
      assemble_alter_config_response(std::move(ret)));
}

} // namespace kafka
