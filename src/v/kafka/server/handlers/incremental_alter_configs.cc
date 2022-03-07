// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "kafka/server/handlers/incremental_alter_configs.h"

#include "cluster/config_frontend.h"
#include "cluster/types.h"
#include "config/node_config.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/incremental_alter_configs.h"
#include "kafka/protocol/schemata/incremental_alter_configs_request.h"
#include "kafka/protocol/schemata/incremental_alter_configs_response.h"
#include "kafka/server//handlers/configs/config_utils.h"
#include "kafka/server/handlers/details/data_policy.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "utils/string_switch.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

using req_resource_t = incremental_alter_configs_resource;
using resp_resource_t = incremental_alter_configs_resource_response;

/**
 * We pass returned value as a paramter to allow template to be automatically
 * resolved.
 */
template<typename T>
void parse_and_set_optional(
  cluster::property_update<std::optional<T>>& property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op) {
    // remove property value
    if (op == config_resource_operation::remove) {
        property.op = cluster::incremental_update_operation::remove;
        return;
    }
    // set property value
    if (op == config_resource_operation::set) {
        property.value = boost::lexical_cast<T>(*value);
        property.op = cluster::incremental_update_operation::set;
        return;
    }
}

template<typename T>
void parse_and_set_tristate(
  cluster::property_update<tristate<T>>& property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op) {
    // remove property value
    if (op == config_resource_operation::remove) {
        property.op = cluster::incremental_update_operation::remove;
        return;
    }
    // set property value
    if (op == config_resource_operation::set) {
        auto parsed = boost::lexical_cast<int64_t>(*value);
        if (parsed <= 0) {
            property.value = tristate<T>{};
        } else {
            property.value = tristate<T>(std::make_optional<T>(parsed));
        }

        property.op = cluster::incremental_update_operation::set;
        return;
    }
}

static void parse_and_set_shadow_indexing_mode(
  cluster::property_update<std::optional<model::shadow_indexing_mode>>& simode,
  const std::optional<ss::sstring>& value,
  config_resource_operation op,
  model::shadow_indexing_mode enabled_value) {
    switch (op) {
    case config_resource_operation::remove:
        simode.op = cluster::incremental_update_operation::remove;
        simode.value = model::negate_shadow_indexing_flag(enabled_value);
        break;
    case config_resource_operation::set:
        simode.op = cluster::incremental_update_operation::set;
        simode.value
          = string_switch<model::shadow_indexing_mode>(*value)
              .match("no", model::negate_shadow_indexing_flag(enabled_value))
              .match("false", model::negate_shadow_indexing_flag(enabled_value))
              .match("yes", enabled_value)
              .match("true", enabled_value)
              .default_match(model::shadow_indexing_mode::disabled);
        break;
    case config_resource_operation::append:
    case config_resource_operation::subtract:
        break;
    }
}

/**
 * valides the optional config
 */

std::optional<resp_resource_t> validate_single_value_config_resource(
  const req_resource_t& resource,
  const ss::sstring& config_name,
  const std::optional<ss::sstring>& config_value,
  config_resource_operation op) {
    if (
      op == config_resource_operation::append
      || op == config_resource_operation::subtract) {
        return make_error_alter_config_resource_response<resp_resource_t>(
          resource,
          kafka::error_code::invalid_config,
          fmt::format(
            "{} operation isn't supported for {} configuration",
            op,
            config_name));
    }

    if (op == config_resource_operation::set && !config_value) {
        return make_error_alter_config_resource_response<resp_resource_t>(
          resource,
          kafka::error_code::invalid_config,
          fmt::format(
            "{} operation for configuration {} requires a value to be set",
            op,
            config_name));
    }

    if (op == config_resource_operation::remove && config_value) {
        return make_error_alter_config_resource_response<resp_resource_t>(
          resource,
          kafka::error_code::invalid_config,
          fmt::format(
            "{} operation for configuration {} requires a value to be empty",
            op,
            config_name));
    }

    // success case
    return std::nullopt;
}

/**
 * Check that the deserialized integer is within bounds
 * for the config_resource_operation enum.  If this
 * returns false, reject the input.
 */
bool valid_config_resource_operation(uint8_t v) {
    return (v >= uint8_t(config_resource_operation::set))
           && (v <= uint8_t(config_resource_operation::subtract));
}

checked<cluster::topic_properties_update, resp_resource_t>
create_topic_properties_update(incremental_alter_configs_resource& resource) {
    model::topic_namespace tp_ns(
      model::kafka_namespace, model::topic(resource.resource_name));
    cluster::topic_properties_update update(tp_ns);

    data_policy_parser dp_parser;

    for (auto& cfg : resource.configs) {
        // Validate int8_t is within range of config_resource_operation
        // before casting (otherwise casting is undefined behaviour)
        if (!valid_config_resource_operation(cfg.config_operation)) {
            return make_error_alter_config_resource_response<resp_resource_t>(
              resource,
              error_code::invalid_config,
              fmt::format("invalid operation code {}", cfg.config_operation));
        }

        auto op = static_cast<config_resource_operation>(cfg.config_operation);

        auto err = validate_single_value_config_resource(
          resource, cfg.name, cfg.value, op);

        if (err) {
            // error case
            return *err;
        }
        try {
            if (cfg.name == topic_property_cleanup_policy) {
                parse_and_set_optional(
                  update.properties.cleanup_policy_bitflags, cfg.value, op);
                continue;
            }
            if (cfg.name == topic_property_compaction_strategy) {
                parse_and_set_optional(
                  update.properties.compaction_strategy, cfg.value, op);
                continue;
            }
            if (cfg.name == topic_property_compression) {
                parse_and_set_optional(
                  update.properties.compression, cfg.value, op);
                continue;
            }
            if (cfg.name == topic_property_segment_size) {
                parse_and_set_optional(
                  update.properties.segment_size, cfg.value, op);
                continue;
            }
            if (cfg.name == topic_property_timestamp_type) {
                parse_and_set_optional(
                  update.properties.timestamp_type, cfg.value, op);
                continue;
            }
            if (cfg.name == topic_property_retention_bytes) {
                parse_and_set_tristate(
                  update.properties.retention_bytes, cfg.value, op);
                continue;
            }
            if (cfg.name == topic_property_retention_duration) {
                parse_and_set_tristate(
                  update.properties.retention_duration, cfg.value, op);
                continue;
            }
            if (cfg.name == topic_property_remote_write) {
                parse_and_set_shadow_indexing_mode(
                  update.properties.shadow_indexing,
                  cfg.value,
                  op,
                  model::shadow_indexing_mode::archival);
                continue;
            }
            if (cfg.name == topic_property_remote_read) {
                parse_and_set_shadow_indexing_mode(
                  update.properties.shadow_indexing,
                  cfg.value,
                  op,
                  model::shadow_indexing_mode::fetch);
                continue;
            }
            if (update_data_policy_parser(dp_parser, cfg.name, cfg.value, op)) {
                continue;
            }
        } catch (const boost::bad_lexical_cast& e) {
            return make_error_alter_config_resource_response<
              incremental_alter_configs_resource_response>(
              resource,
              error_code::invalid_config,
              fmt::format(
                "unable to parse property {} value {}", cfg.name, cfg.value));
        } catch (const v8_engine::data_policy_exeption& e) {
            return make_error_alter_config_resource_response<
              incremental_alter_configs_resource_response>(
              resource,
              error_code::invalid_config,
              fmt::format(
                "unable to parse property redpanda.data-policy.{} value {}, "
                "error {}",
                cfg.name,
                cfg.value,
                e.what()));
        }
        // Unsupported property, return error
        return make_error_alter_config_resource_response<resp_resource_t>(
          resource,
          error_code::invalid_config,
          fmt::format("invalid topic property: {}", cfg.name));
    }
    try {
        update.custom_properties.data_policy.value = data_policy_from_parser(
          dp_parser);
        update.custom_properties.data_policy.op = dp_parser.op;
    } catch (const v8_engine::data_policy_exeption& e) {
        return make_error_alter_config_resource_response<
          incremental_alter_configs_resource_response>(
          resource,
          error_code::invalid_config,
          fmt::format(
            "unable to parse property redpanda.data-policy, error {}",
            e.what()));
    }

    return update;
}

static ss::future<std::vector<resp_resource_t>> alter_topic_configuration(
  request_context& ctx,
  std::vector<req_resource_t> resources,
  bool validate_only) {
    return do_alter_topics_configuration<req_resource_t, resp_resource_t>(
      ctx, std::move(resources), validate_only, [](req_resource_t& r) {
          return create_topic_properties_update(r);
      });
}

/**
 * For configuration properties that might reasonably be set via
 * the kafka API (i.e. those in common with other kafka implementations),
 * map their traditional kafka name to the redpanda config property name.
 */
inline std::string_view map_config_name(std::string_view input) {
    return string_switch<std::string_view>(input)
      .match("log.cleanup.policy", "log_cleanup_policy")
      .match("log.message.timestamp.type", "log_message_timestamp_type")
      .match("log.compression.type", "log_compression_type")
      .default_match(input);
}

static ss::future<std::vector<resp_resource_t>> alter_broker_configuartion(
  request_context& ctx, std::vector<req_resource_t> resources) {
    std::vector<resp_resource_t> responses;
    responses.reserve(resources.size());

    // If central config is disabled, we cannot set broker properties
    if (!ctx.feature_table().local().is_active(
          cluster::feature::central_config)) {
        co_return co_await unsupported_broker_configuration<
          req_resource_t,
          resp_resource_t>(
          std::move(resources),
          "changing broker properties via this API is not enabled");

        co_return responses;
    }

    for (const auto& resource : resources) {
        cluster::config_update_request req;

        if (!resource.resource_name.empty()) {
            responses.push_back(
              make_error_alter_config_resource_response<resp_resource_t>(
                resource,
                error_code::invalid_config,
                "Setting broker properties on named brokers is unsupported"));
            continue;
        }

        bool errored = false;
        for (const auto& c : resource.configs) {
            auto mapped_name = map_config_name(c.name);

            // Validate int8_t is within range of config_resource_operation
            // before casting (otherwise casting is undefined behaviour)
            if (!valid_config_resource_operation(c.config_operation)) {
                responses.push_back(
                  make_error_alter_config_resource_response<resp_resource_t>(
                    resource,
                    error_code::invalid_config,
                    fmt::format(
                      "invalid operation code {}", c.config_operation)));
                errored = true;
                continue;
            }

            auto op = static_cast<config_resource_operation>(
              c.config_operation);
            if (op == config_resource_operation::set) {
                req.upsert.push_back(
                  {ss::sstring(mapped_name), c.value.value()});
            } else if (op == config_resource_operation::remove) {
                req.remove.push_back(ss::sstring(mapped_name));
            } else {
                responses.push_back(
                  make_error_alter_config_resource_response<resp_resource_t>(
                    resource,
                    error_code::invalid_config,
                    fmt::format(
                      "operation {} on broker properties isn't currently "
                      "supported",
                      op)));
                errored = true;
            }
        }
        if (errored) {
            continue;
        }

        // Validate contents of the request
        config::configuration cfg;
        for (const auto& i : req.upsert) {
            // Decode to a YAML object because that's what the property
            // interface expects.
            // Don't both catching ParserException: this was encoded
            // just a few lines above.
            const auto& yaml_value = i.second;
            auto val = YAML::Load(yaml_value);

            if (!cfg.contains(i.first)) {
                responses.push_back(
                  make_error_alter_config_resource_response<resp_resource_t>(
                    resource,
                    error_code::invalid_config,
                    fmt::format("Unknown property '{}'", i.first)));
                errored = true;
                continue;
            }
            auto& property = cfg.get(i.first);
            try {
                property.set_value(val);
            } catch (...) {
                responses.push_back(
                  make_error_alter_config_resource_response<resp_resource_t>(
                    resource,
                    error_code::invalid_config,
                    fmt::format(
                      "bad property value for '{}': '{}'", i.first, i.second)));
                errored = true;
                continue;
            }
        }

        if (errored) {
            continue;
        }

        auto resp
          = co_await ctx.config_frontend()
              .invoke_on(
                cluster::config_frontend::version_shard,
                [req = std::move(req)](cluster::config_frontend& fe) mutable {
                    return fe.patch(
                      std::move(req),
                      model::timeout_clock::now()
                        + config::shard_local_cfg()
                            .alter_topic_cfg_timeout_ms());
                })
              .then([resource](cluster::config_frontend::patch_result pr) {
                  std::error_code& ec = pr.errc;
                  error_code kec = error_code::none;

                  std::string err_str;
                  if (ec) {
                      err_str = ec.message();
                      if (ec.category() == cluster::error_category()) {
                          kec = map_topic_error_code(cluster::errc(ec.value()));
                      } else {
                          // Generic config error
                          kec = error_code::invalid_config;
                      }
                  }
                  return make_error_alter_config_resource_response<
                    resp_resource_t>(resource, kec, err_str);
              });

        responses.push_back(resp);
    }

    co_return responses;
}

template<>
ss::future<response_ptr> incremental_alter_configs_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    incremental_alter_configs_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(klog.trace, "Handling request {}", request);

    auto groupped = group_alter_config_resources(
      std::move(request.data.resources));

    auto unauthorized_responsens = authorize_alter_config_resources<
      incremental_alter_configs_resource,
      resp_resource_t>(ctx, groupped);

    std::vector<ss::future<std::vector<resp_resource_t>>> futures;
    futures.reserve(2);
    futures.push_back(alter_topic_configuration(
      ctx, std::move(groupped.topic_changes), request.data.validate_only));
    futures.push_back(
      alter_broker_configuartion(ctx, std::move(groupped.broker_changes)));

    auto ret = co_await ss::when_all_succeed(futures.begin(), futures.end());
    // include authorization errors
    ret.push_back(std::move(unauthorized_responsens));

    co_return co_await ctx.respond(assemble_alter_config_response<
                                   incremental_alter_configs_response,
                                   resp_resource_t>(std::move(ret)));
}

} // namespace kafka
