// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/alter_configs.h"

#include "cluster/metadata_cache.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/alter_configs_request.h"
#include "kafka/protocol/schemata/alter_configs_response.h"
#include "kafka/server/handlers/configs/config_utils.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"
#include "strings/string_switch.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <absl/container/node_hash_set.h>
#include <fmt/ostream.h>

#include <string_view>

namespace kafka {
// Legacy function, bug prone for multiple property updates, i.e
// alter-config --set redpanda.remote.read=true --set
// redpanda.remote.write=false.
// Used if feature flag shadow_indexing_split_topic_property_update (v24.3) is
// not active.
static void parse_and_set_shadow_indexing_mode(
  cluster::property_update<std::optional<model::shadow_indexing_mode>>&
    property_update,
  const std::optional<ss::sstring>& value,
  model::shadow_indexing_mode enabled_value) {
    property_update.op = cluster::incremental_update_operation::set;
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

checked<cluster::topic_properties_update, alter_configs_resource_response>
create_topic_properties_update(
  const request_context& ctx, alter_configs_resource& resource) {
    using op_t = cluster::incremental_update_operation;

    model::topic_namespace tp_ns(
      model::kafka_namespace, model::topic(resource.resource_name));
    cluster::topic_properties_update update(tp_ns);
    /**
     * Alter topic configuration should override topic properties with values
     * sent in the request, if given resource value isn't set in the request,
     * override for this value has to be removed. We override all defaults to
     * set, even if value for given property isn't set it will override
     * configuration in topic table, the only difference is the replication
     * factor, if not set in the request explicitly it will not be overriden.
     */
    constexpr auto apply_op = [](op_t op) {
        return [op](auto&&... prop) { ((prop.op = op), ...); };
    };
    std::apply(apply_op(op_t::remove), update.properties.serde_fields());
    std::apply(apply_op(op_t::none), update.custom_properties.serde_fields());

    static_assert(
      std::tuple_size_v<decltype(update.properties.serde_fields())> == 30,
      "If you added a property, please decide on it's default alter config "
      "policy, and handle the update in the loop below");
    static_assert(
      std::tuple_size_v<decltype(update.custom_properties.serde_fields())> == 2,
      "If you added a property, please decide on it's default alter config "
      "policy, and handle the update in the loop below");

    /*
      As of v24.3, a new update path for shadow indexing properties should be
      used.
     */
    const auto shadow_indexing_split_update
      = ctx.feature_table().local().is_active(
        features::feature::shadow_indexing_split_topic_property_update);
    /**
     * The shadow_indexing properties ('redpanda.remote.(read|write|delete)')
     * are special "sticky" topic properties that are always set as a
     * topic-level override. We should prevent changing them unless explicitly
     * requested.
     *
     * See: https://github.com/redpanda-data/redpanda/issues/7451
     */
    update.properties.remote_read.op = op_t::none;
    update.properties.remote_write.op = op_t::none;
    update.properties.remote_delete.op = op_t::none;

    // Legacy
    auto& update_properties_shadow_indexing
      = update.properties.get_shadow_indexing();
    update_properties_shadow_indexing.op = op_t::none;

    // Now that the defaults are set, continue to set properties from the
    // request

    schema_id_validation_config_parser schema_id_validation_config_parser{
      update.properties};

    for (auto& cfg : resource.configs) {
        try {
            if (cfg.name == topic_property_cleanup_policy) {
                parse_and_set_optional(
                  update.properties.cleanup_policy_bitflags,
                  cfg.value,
                  kafka::config_resource_operation::set);
                continue;
            }
            if (cfg.name == topic_property_compaction_strategy) {
                parse_and_set_optional(
                  update.properties.compaction_strategy,
                  cfg.value,
                  kafka::config_resource_operation::set);
                continue;
            }
            if (cfg.name == topic_property_compression) {
                parse_and_set_optional(
                  update.properties.compression,
                  cfg.value,
                  kafka::config_resource_operation::set);
                continue;
            }
            if (cfg.name == topic_property_segment_size) {
                parse_and_set_optional(
                  update.properties.segment_size,
                  cfg.value,
                  kafka::config_resource_operation::set,
                  segment_size_validator{});
                continue;
            }
            if (cfg.name == topic_property_timestamp_type) {
                parse_and_set_optional(
                  update.properties.timestamp_type,
                  cfg.value,
                  kafka::config_resource_operation::set);
                continue;
            }
            if (cfg.name == topic_property_retention_bytes) {
                parse_and_set_tristate(
                  update.properties.retention_bytes,
                  cfg.value,
                  kafka::config_resource_operation::set);
                continue;
            }
            if (cfg.name == topic_property_remote_read) {
                if (shadow_indexing_split_update) {
                    parse_and_set_bool(
                      tp_ns,
                      update.properties.remote_read,
                      cfg.value,
                      kafka::config_resource_operation::set,
                      config::shard_local_cfg()
                        .cloud_storage_enable_remote_read());

                } else {
                    // Legacy update for shadow indexing field
                    auto set_value
                      = update_properties_shadow_indexing.value
                          ? model::add_shadow_indexing_flag(
                              *update_properties_shadow_indexing.value,
                              model::shadow_indexing_mode::fetch)
                          : model::shadow_indexing_mode::fetch;
                    parse_and_set_shadow_indexing_mode(
                      update_properties_shadow_indexing, cfg.value, set_value);
                }
                continue;
            }
            if (cfg.name == topic_property_remote_write) {
                if (shadow_indexing_split_update) {
                    parse_and_set_bool(
                      tp_ns,
                      update.properties.remote_write,
                      cfg.value,
                      kafka::config_resource_operation::set,
                      config::shard_local_cfg()
                        .cloud_storage_enable_remote_write());
                } else {
                    // Legacy update for shadow indexing field
                    auto set_value
                      = update_properties_shadow_indexing.value
                          ? model::add_shadow_indexing_flag(
                              *update_properties_shadow_indexing.value,
                              model::shadow_indexing_mode::archival)
                          : model::shadow_indexing_mode::archival;
                    parse_and_set_shadow_indexing_mode(
                      update_properties_shadow_indexing, cfg.value, set_value);
                }

                continue;
            }
            if (cfg.name == topic_property_remote_delete) {
                parse_and_set_bool(
                  tp_ns,
                  update.properties.remote_delete,
                  cfg.value,
                  kafka::config_resource_operation::set,
                  storage::ntp_config::default_remote_delete);
                continue;
            }
            if (cfg.name == topic_property_segment_ms) {
                parse_and_set_tristate(
                  update.properties.segment_ms,
                  cfg.value,
                  kafka::config_resource_operation::set);
                continue;
            }
            if (cfg.name == topic_property_retention_duration) {
                parse_and_set_tristate(
                  update.properties.retention_duration,
                  cfg.value,
                  kafka::config_resource_operation::set);
                continue;
            }
            if (cfg.name == topic_property_max_message_bytes) {
                parse_and_set_optional(
                  update.properties.batch_max_bytes,
                  cfg.value,
                  kafka::config_resource_operation::set);
                continue;
            }
            if (cfg.name == topic_property_retention_local_target_ms) {
                parse_and_set_tristate(
                  update.properties.retention_local_target_ms,
                  cfg.value,
                  kafka::config_resource_operation::set);
                continue;
            }
            if (cfg.name == topic_property_retention_local_target_bytes) {
                parse_and_set_tristate(
                  update.properties.retention_local_target_bytes,
                  cfg.value,
                  kafka::config_resource_operation::set);
                continue;
            }
            if (cfg.name == topic_property_replication_factor) {
                parse_and_set_topic_replication_factor(
                  tp_ns,
                  update.custom_properties.replication_factor,
                  cfg.value,
                  kafka::config_resource_operation::set,
                  replication_factor_validator{});
                continue;
            }
            if (cfg.name == topic_property_initial_retention_local_target_ms) {
                parse_and_set_tristate(
                  update.properties.initial_retention_local_target_ms,
                  cfg.value,
                  kafka::config_resource_operation::set);
                continue;
            }
            if (
              cfg.name == topic_property_initial_retention_local_target_bytes) {
                parse_and_set_tristate(
                  update.properties.initial_retention_local_target_bytes,
                  cfg.value,
                  kafka::config_resource_operation::set);
                continue;
            }
            if (
              config::shard_local_cfg().enable_schema_id_validation()
              != pandaproxy::schema_registry::schema_id_validation_mode::none) {
                if (schema_id_validation_config_parser(
                      cfg, kafka::config_resource_operation::set)) {
                    continue;
                }
            }
            if (
              std::find(
                std::begin(allowlist_topic_noop_confs),
                std::end(allowlist_topic_noop_confs),
                cfg.name)
              != std::end(allowlist_topic_noop_confs)) {
                // Skip unsupported Kafka config
                continue;
            };
            if (cfg.name == topic_property_write_caching) {
                parse_and_set_optional(
                  update.properties.write_caching,
                  cfg.value,
                  kafka::config_resource_operation::set,
                  write_caching_config_validator{});
                continue;
            }
            if (cfg.name == topic_property_flush_ms) {
                parse_and_set_optional_duration(
                  update.properties.flush_ms,
                  cfg.value,
                  kafka::config_resource_operation::set,
                  flush_ms_validator{},
                  true);
                continue;
            }
            if (cfg.name == topic_property_flush_bytes) {
                parse_and_set_optional(
                  update.properties.flush_bytes,
                  cfg.value,
                  kafka::config_resource_operation::set,
                  flush_bytes_validator{});
                continue;
            }
            if (cfg.name == topic_property_iceberg_enabled) {
                parse_and_set_bool(
                  tp_ns,
                  update.properties.iceberg_enabled,
                  cfg.value,
                  kafka::config_resource_operation::set,
                  storage::ntp_config::default_iceberg_enabled,
                  iceberg_config_validator{});
                continue;
            }
            if (cfg.name == topic_property_leaders_preference) {
                parse_and_set_optional(
                  update.properties.leaders_preference,
                  cfg.value,
                  kafka::config_resource_operation::set,
                  noop_validator<config::leaders_preference>{},
                  config::leaders_preference::parse);
                continue;
            }
            if (cfg.name == topic_property_cloud_topic_enabled) {
                if (config::shard_local_cfg()
                      .development_enable_cloud_topics()) {
                    throw validation_error(
                      "Cloud topics property cannot be changed");
                }
                throw validation_error("Cloud topics is not enabled");
            }

        } catch (const validation_error& e) {
            return make_error_alter_config_resource_response<
              alter_configs_resource_response>(
              resource, error_code::invalid_config, e.what());
        } catch (const boost::bad_lexical_cast& e) {
            return make_error_alter_config_resource_response<
              alter_configs_resource_response>(
              resource,
              error_code::invalid_config,
              fmt::format(
                "unable to parse property {} value {}", cfg.name, cfg.value));
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

static ss::future<chunked_vector<alter_configs_resource_response>>
alter_topic_configuration(
  request_context& ctx,
  chunked_vector<alter_configs_resource> resources,
  bool validate_only) {
    return do_alter_topics_configuration<
      alter_configs_resource,
      alter_configs_resource_response>(
      ctx,
      std::move(resources),
      validate_only,
      [&ctx](alter_configs_resource& r) {
          return create_topic_properties_update(ctx, r);
      });
}

static ss::future<chunked_vector<alter_configs_resource_response>>
alter_broker_configuration(chunked_vector<alter_configs_resource> resources) {
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
    log_request(ctx.header(), request);

    auto groupped = group_alter_config_resources(
      std::move(request.data.resources));

    auto unauthorized_responsens = authorize_alter_config_resources<
      alter_configs_resource,
      alter_configs_resource_response>(ctx, groupped);

    if (!ctx.audit()) {
        auto responses = make_audit_failure_response<
          alter_configs_resource_response,
          alter_configs_resource>(
          std::move(groupped), std::move(unauthorized_responsens));

        co_return co_await ctx.respond(
          assemble_alter_config_response<
            alter_configs_response,
            alter_configs_resource_response>(std::move(responses)));
    }

    std::vector<ss::future<chunked_vector<alter_configs_resource_response>>>
      futures;
    futures.reserve(2);
    futures.push_back(alter_topic_configuration(
      ctx, std::move(groupped.topic_changes), request.data.validate_only));
    futures.push_back(
      alter_broker_configuration(std::move(groupped.broker_changes)));

    auto ret = co_await ss::when_all_succeed(futures.begin(), futures.end());
    // include authorization errors
    ret.push_back(std::move(unauthorized_responsens));

    co_return co_await ctx.respond(
      assemble_alter_config_response<
        alter_configs_response,
        alter_configs_resource_response>(std::move(ret)));
}

} // namespace kafka
