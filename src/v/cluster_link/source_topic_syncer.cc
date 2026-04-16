/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#include "cluster_link/source_topic_syncer.h"

#include "cluster_link/link.h"
#include "cluster_link/model/filter_utils.h"
#include "cluster_link/model/types.h"
#include "model/namespace.h"

#include <fmt/ranges.h>

namespace cluster_link {

using properties_set = model::topic_metadata_mirroring_config::properties_set;

namespace {

/*
 * This map contains topic properties that will be overridden when creating
 * mirror topics.
 */
const absl::flat_hash_map<ss::sstring, ss::sstring>
  topic_configuration_overrides{
    {{ss::sstring(kafka::topic_property_remote_allow_gaps), "true"}}};

const chunked_vector<ss::sstring> topic_prefix_denylist{
  ss::sstring("__redpanda"), ss::sstring("_redpanda")};

bool has_required_permissions(
  kafka::topic_authorized_operations permissions_to_check,
  kafka::topic_authorized_operations required_permissions) {
    return (permissions_to_check & required_permissions)
           == required_permissions;
}

template<typename K, typename V>
chunked_hash_map<K, V> copy_hash_map(const chunked_hash_map<K, V>& source) {
    chunked_hash_map<K, V> copy;
    copy.reserve(source.size());
    for (const auto& [key, value] : source) {
        copy.emplace(key, value);
    }
    return copy;
}

// This function validates that the describe_configs response returned
// successfully and contains a topic resource response
std::optional<chunked_hash_map<ss::sstring, ss::sstring>>
validate_and_get_configs_from_response(
  prefix_logger& logger, const kafka::describe_configs_result& resp) {
    if (resp.error_code != kafka::error_code::none) {
        vlog(
          logger.debug,
          "Failed to fetch configs for topic {}: {}{}",
          resp.resource_name,
          resp.error_code,
          resp.error_message.has_value() ? " - " + *resp.error_message : "");
        return std::nullopt;
    }
    if (resp.resource_type != kafka::config_resource_type::topic) {
        vlog(
          logger.debug,
          "Unexpected resource type {} for topic {}",
          resp.resource_type,
          resp.resource_name);
        return std::nullopt;
    }
    chunked_hash_map<ss::sstring, ss::sstring> configs;
    configs.reserve(resp.configs.size());
    for (const auto& c : resp.configs) {
        if (c.value.has_value()) {
            configs.emplace(c.name, *c.value);
        }
    }

    // apply overrides
    for (const auto& [k, v] : topic_configuration_overrides) {
        auto it = configs.find(k);
        if (it != configs.end()) {
            vlog(
              logger.debug,
              "Overriding topic property {} value from {} to {}",
              k,
              it->second,
              v);
            it->second = v;
        } else {
            vlog(
              logger.trace,
              "Adding topic property override {} with value {}",
              k,
              v);
            configs.emplace(k, v);
        }
    }

    return configs;
}

// Validates the contents of the metadata cache from the source cluster.  If
// partition count or replication count are invalid then will return
// std::nullopt.  Otherwise returns the metadata information.
std::optional<std::tuple<
  int32_t,
  int16_t,
  kafka::topic_authorized_operations,
  std::optional<::model::topic_id>>>
validate_topic_cache_entry(
  prefix_logger& logger,
  const kafka::client::topic_cache& cache,
  ::model::topic_view topic) {
    const auto& topic_cache_map = cache.cache();
    auto it = topic_cache_map.find(topic);
    if (it == topic_cache_map.end()) {
        vlog(logger.trace, "Topic {} not found in cache", topic);
        return std::nullopt;
    }

    auto partition_count = static_cast<int32_t>(it->second.partitions.size());
    if (partition_count < 1) {
        vlog(logger.trace, "Skipping topic {} with no partitions", topic);
        return std::nullopt;
    }

    auto replication_factor = it->second.replication_factor;
    if (replication_factor < 1) {
        vlog(
          logger.trace,
          "Skipping topic {} with invalid replication factor {}",
          topic,
          replication_factor);
        return std::nullopt;
    }

    auto topic_id = it->second.topic_id;

    vlog(
      logger.trace,
      "Topic {} has {} partitions, RF={} and authorized operations {:08x} and "
      "topic_id {}",
      topic,
      partition_count,
      replication_factor,
      it->second.authorized_operations,
      topic_id);

    return std::make_tuple(
      partition_count,
      replication_factor,
      it->second.authorized_operations,
      topic_id);
}

std::optional<ss::sstring> is_valid_topic(
  ::model::topic_view topic,
  const chunked_vector<model::resource_name_filter_pattern>& patterns) {
    if (!::model::is_shadow_link_enabled({::model::kafka_namespace, topic})) {
        return ssx::sformat(
          "Topic {} is not a valid topic for Shadow Linking", topic);
    }
    for (const auto& prefix : topic_prefix_denylist) {
        if (topic().starts_with(prefix)) {
            // Need to check the list of include filters and see if it matches
            // any specifically included topics
            for (const auto& p : patterns) {
                if (
                  p.filter == model::filter_type::include
                  && p.pattern_type == model::filter_pattern_type::literal
                  && p.pattern == topic) {
                    // Even though the topic is prefixed with either "_redpanda"
                    // or "__redpanda", if it's specifically included, then we
                    // will permit shadowing of this topic
                    return std::nullopt;
                }
            }
            return ssx::sformat(
              "Topic {} starts with a denied prefix: {}", topic, prefix);
        }
    }
    return std::nullopt;
}

bool shadowing_entire_sr(const model::schema_registry_sync_config& cfg) {
    return cfg.sync_schema_registry_topic_mode.has_value()
           && std::holds_alternative<
             ::cluster_link::model::schema_registry_sync_config::
               shadow_entire_schema_registry>(
             *cfg.sync_schema_registry_topic_mode);
}

bool select_topic(
  ::model::topic_view topic,
  const model::topic_metadata_mirroring_config& cfg,
  const model::schema_registry_sync_config& sr_cfg) {
    if (topic == ::model::schema_registry_internal_tp.topic) [[unlikely]] {
        return shadowing_entire_sr(sr_cfg);
    }

    return ::cluster_link::model::select_topic(topic, cfg.topic_name_filters);
}
} // namespace

source_topic_syncer::source_topic_syncer(
  link* link, const model::metadata& config)
  : controller_locked_task(
      link,
      config.configuration.topic_metadata_mirroring_cfg.get_task_interval(),
      source_topic_syncer::task_name)
  , _config(config.configuration.topic_metadata_mirroring_cfg.copy())
  , _sr_config(config.configuration.schema_registry_sync_cfg) {}

void source_topic_syncer::update_config(const model::metadata& config) {
    _config = config.configuration.topic_metadata_mirroring_cfg.copy();
    _sr_config = config.configuration.schema_registry_sync_cfg;
    set_run_interval(
      config.configuration.topic_metadata_mirroring_cfg.get_task_interval());
}

model::enabled_t source_topic_syncer::is_enabled() const {
    return _config.is_enabled;
}

ss::future<task::state_transition>
source_topic_syncer::run_impl(ss::abort_source& as) {
    vlog(logger().trace, "Running auto topic sensor task");

    auto& cluster = get_link()->get_cluster_connection();

    // Perform a metadata update to get as fresh as possible data from the
    // source cluster
    try {
        co_await cluster.request_metadata_update();
    } catch (const std::exception& e) {
        auto msg = ssx::sformat("Failed to update metadata: {}", e.what());
        vlog(logger().warn, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(msg)};
    }

    // Ensure there is a controller on the source cluster
    auto controller_id = cluster.get_controller_id();
    if (!controller_id) {
        auto msg = ssx::sformat(
          "Failed to get controller id for link {}",
          get_link()->get_config()->name);
        vlog(logger().warn, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(msg)};
    }

    // Grab the version of DescribeConfigs that's supported on the source
    // cluster and ensure we support it
    kafka::api_version describe_configs_version;
    try {
        auto supported_api_versions = co_await cluster.supported_api_versions(
          kafka::describe_configs_api::key, as);
        if (!supported_api_versions.has_value()) {
            auto msg = ssx::sformat(
              "Failed to get supported API version for describe_configs");
            vlog(logger().warn, "{}", msg);
            co_return state_transition{
              .desired_state = model::task_state::link_unavailable,
              .reason = std::move(msg)};
        }
        // Make sure the minimum version supported on the cluster is not higher
        // than the maximum version supported by the shadow cluster
        if (
          supported_api_versions.value().min
          > kafka::describe_configs_api::max_valid) {
            auto msg = ssx::sformat(
              "Unsupported DescribeConfigs API version: {}",
              supported_api_versions.value());
            vlog(logger().warn, "{}", msg);
            co_return state_transition{
              .desired_state = model::task_state::link_unavailable,
              .reason = std::move(msg)};
        }
        describe_configs_version = std::min(
          supported_api_versions.value().max,
          kafka::describe_configs_api::max_valid);
        vlog(
          logger().debug,
          "Using describe_configs version: {}",
          describe_configs_version);
    } catch (const ss::abort_requested_exception&) {
        // Rethrow abort requested to allow caller to handle it
        throw;
    } catch (const std::exception& e) {
        auto msg = ssx::sformat(
          "Failed to get supported API version for describe_configs: {}",
          e.what());
        vlog(logger().warn, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(msg)};
    }

    // If we are going to shadow the schema registry topic, we need to ensure
    // that it's either empty or it does not exist.  Check for its HWM.  This is
    // not 100% guaranteed as it may be possible for a record to get written
    // after the check in HWM, however the SR is disabled from writing when
    // shadowing is active so the likelihood is small.
    sr_is_empty_t sr_is_empty{false};
    if (shadowing_entire_sr(_sr_config)) {
        as.check();
        sr_is_empty = co_await check_if_schema_registry_is_empty();
    }

    // Now grab two lists of topics:
    // * Topics that are candidates for creation - topics that do not currently
    //   exist but are selected by the auto topic create filters
    // * Topics that are candidates for updates - existing mirror topics
    auto candidates_for_creation = find_candidate_topics_for_creation(
      cluster, sr_is_empty);
    auto candidates_for_update = find_candidate_topics_for_update(cluster);

    if (candidates_for_creation.empty() && candidates_for_update.empty()) {
        vlog(
          logger().debug,
          "No candidate topics for creation or update for link {}",
          get_link()->get_config()->name);
        co_return state_transition{
          .desired_state = model::task_state::active,
          .reason
          = "No candidate topics for creation or update - run successful"};
    }

    // Build a list of topics to describe
    chunked_vector<::model::topic> topics_to_describe;
    topics_to_describe.reserve(
      candidates_for_creation.size() + candidates_for_update.size());

    std::ranges::copy(
      std::views::keys(candidates_for_creation),
      std::back_inserter(topics_to_describe));
    std::ranges::copy(
      std::views::keys(candidates_for_update),
      std::back_inserter(topics_to_describe));

    as.check();

    kafka::describe_configs_response response;
    try {
        response = co_await describe_topics(
          cluster,
          controller_id.value(),
          describe_configs_version,
          topics_to_describe,
          _config.get_topic_properties_to_mirror());
        vlog(logger().trace, "Describe topics response: {}", response);
    } catch (const std::exception& e) {
        auto msg = ssx::sformat("Failed to describe topics: {}", e.what());
        vlog(logger().warn, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(msg)};
    }

    // Build a list of commands, fill it in with commands to add mirror topics,
    // update mirror topic properties, or update mirror topic state
    reconciler_commands_vector commands;
    enqueue_create_mirror_topic_commands(
      commands, candidates_for_creation, response.data.results);
    enqueue_update_mirror_topic_commands(
      commands, candidates_for_update, response.data.results);

    as.check();

    // Execute the commands
    co_await submit_commands(std::move(commands));

    vlog(logger().trace, "Auto topic sensor task completed");
    co_return state_transition{
      .desired_state = model::task_state::active,
      .reason = "Auto topic sensor task completed"};
}

void source_topic_syncer::enqueue_create_mirror_topic_commands(
  reconciler_commands_vector& commands,
  const chunked_hash_map<::model::topic, topic_metadata>& candidates,
  const chunked_vector<kafka::describe_configs_result>& describe_results) {
    // This function will go through the describe result and select any topic
    // that responded successfully and are not currently mirror topics
    for (const auto& describe_result : describe_results) {
        // Check to see if the describe result contains a candidates for topic
        // creation
        auto it = candidates.find(
          ::model::topic_view{describe_result.resource_name});
        if (it == candidates.end()) {
            continue;
        }
        vlog(
          logger().trace,
          "Validating describe result for create topic candidate {}",
          describe_result.resource_name);
        auto configs = validate_and_get_configs_from_response(
          logger(), describe_result);
        if (!configs.has_value()) {
            vlog(
              logger().trace,
              "Failed to validate describe result for topic {}",
              describe_result.resource_name);
            continue;
        }

        commands.emplace_back(
          model::add_mirror_topic_cmd{
            .topic = it->first,
            .metadata = model::mirror_topic_metadata{
              .status = model::mirror_topic_status::active,
              .source_topic_id = it->second.topic_id,
              .source_topic_name = it->first,
              .partition_count = it->second.partition_count,
              .replication_factor = it->second.rf,
              .topic_configs = std::move(*configs),
              .start_offset_ts = _config.get_start_offset_ts(),
            }});
    }
}

void source_topic_syncer::enqueue_update_mirror_topic_commands(
  reconciler_commands_vector& commands,
  const candidate_update_map& candidates,
  const chunked_vector<kafka::describe_configs_result>& describe_results) {
    // This function steps through the describe results and attempts to create
    // update properites or update topic state commands.  It will first check
    // that the partition count or replication factor have been changed and that
    // the partition count did not go backwards.  If the partition count went
    // down, it will add an update_topic_state command to set the mirror topic
    // state to failed.
    for (const auto& describe_result : describe_results) {
        bool enqueue_command = false;
        auto it = candidates.find(
          ::model::topic_view{describe_result.resource_name});
        if (it == candidates.end()) {
            continue;
        }

        vlog(
          logger().trace,
          "Validating describe result for update topic candidates {}",
          describe_result.resource_name);

        const auto& topic = it->first;
        const auto& metadata_cache = it->second.first;
        const auto& mirror_topic_cache = it->second.second;

        if (mirror_topic_cache.status != model::mirror_topic_status::active) {
            vlog(
              logger().debug,
              "Skipping update to topic {} which is in a non-active state: {}",
              topic,
              mirror_topic_cache.status);
            continue;
        }

        // TODO: Once Topic IDs are supported, check that the Topic ID in the
        // metadata response matches the expecteed Topic ID for this topic.

        // If we detect that the partition count has gone down, this indicates
        // that the topic may have been deleted and then re-created, so we will
        // put the topic into the failed state
        if (
          mirror_topic_cache.partition_count > metadata_cache.partition_count) {
            vlog(
              logger().warn,
              "Topic {} has fewer partitions than expected, marking as failed",
              topic);
            commands.emplace_back(
              model::update_mirror_topic_status_cmd{
                .topic = topic,
                .status = model::mirror_topic_status::failed,
              });
            continue;
        }

        if (
          mirror_topic_cache.source_topic_id.has_value()
          && metadata_cache.topic_id.has_value()
          && mirror_topic_cache.source_topic_id != metadata_cache.topic_id) {
            vlog(
              logger().warn,
              "Topic {} has changed its topic ID from {} -> {}.  Marking as "
              "failed",
              topic,
              mirror_topic_cache.source_topic_id,
              metadata_cache.topic_id);
            commands.emplace_back(
              model::update_mirror_topic_status_cmd{
                .topic = topic, .status = model::mirror_topic_status::failed});
            continue;
        }

        // Detect if the partition count has changed
        if (
          mirror_topic_cache.partition_count
          != metadata_cache.partition_count) {
            vlog(
              logger().trace,
              "Topic {} has updated its partition count: {} -> {}",
              topic,
              mirror_topic_cache.partition_count,
              metadata_cache.partition_count);
            enqueue_command = true;
        }

        // Detect if RF has changed
        if (mirror_topic_cache.replication_factor != metadata_cache.rf) {
            vlog(
              logger().trace,
              "Topic {} has updated its RF: {} -> {}",
              topic,
              mirror_topic_cache.replication_factor,
              metadata_cache.rf);
            enqueue_command = true;
        }

        auto configs = validate_and_get_configs_from_response(
          logger(), describe_result);

        if (configs.has_value()) {
            // Now check to see if the the properties on the topic have differed
            for (const auto& [key, val] : *configs) {
                auto cached_config_it = mirror_topic_cache.topic_configs.find(
                  key);
                if (
                  cached_config_it == mirror_topic_cache.topic_configs.end()
                  || cached_config_it->second != val) {
                    vlog(
                      logger().trace,
                      "Topic {} property {} changed: {} -> {}",
                      topic,
                      key,
                      cached_config_it == mirror_topic_cache.topic_configs.end()
                        ? "<not-set>"
                        : cached_config_it->second,
                      val);
                    enqueue_command = true;
                }
            }
        } else {
            vlog(
              logger().trace,
              "Failed to validate describe result for topic {}. We will use "
              "cached topic configs if needed.",
              describe_result.resource_name);
        }

        // One or more of the partition count, replication factor, or some topic
        // configs changed, so we should update the mirror topic properties
        // forthwith. Note that in any case the new value(s) have been validated
        // already.
        if (enqueue_command) {
            commands.emplace_back(
              model::update_mirror_topic_properties_cmd{
                .topic = topic,
                .partition_count = metadata_cache.partition_count,
                .replication_factor = metadata_cache.rf,
                .topic_configs = std::move(configs).value_or(
                  copy_hash_map(mirror_topic_cache.topic_configs)),
              });
        }
    }
}

ss::future<>
source_topic_syncer::submit_commands(reconciler_commands_vector commands) {
    if (commands.empty()) {
        co_return;
    }

    for (auto& c : commands) {
        auto res = co_await ss::visit(
          std::move(c),
          [this](model::add_mirror_topic_cmd c) {
              return get_link()->add_mirror_topic(std::move(c));
          },
          [this](model::update_mirror_topic_properties_cmd c) {
              return get_link()->update_mirror_topic_properties(std::move(c));
          },
          [this](model::update_mirror_topic_status_cmd c) {
              return get_link()->update_mirror_topic_state(std::move(c));
          });
        if (res != ::cluster::cluster_link::errc::success) {
            vlog(
              logger().warn, "Failed to process mirror topic command: {}", res);
        } else {
            vlog(logger().trace, "Successfully processed mirror topic command");
        }
    }
}

source_topic_syncer::candidate_update_map
source_topic_syncer::find_candidate_topics_for_update(
  kafka::client::cluster& cluster) {
    // All mirror topics this link is responsible for
    auto mirror_topics = get_link()->get_mirror_topics_for_link();
    if (!mirror_topics.has_value()) {
        vlog(
          logger().error,
          "Cluster link table reporting that link does not exist!");
        return {};
    }

    auto mirror_rf = _config.get_topic_properties_to_mirror().contains(
      kafka::topic_property_replication_factor);

    candidate_update_map candidate_topics;
    candidate_topics.reserve(mirror_topics->size());

    for (auto& [topic, mirror_metadata] : *mirror_topics) {
        vlog(logger().trace, "Checking metadata cache for topic {}", topic);
        auto metadata_value = validate_topic_cache_entry(
          logger(), cluster.get_topics(), topic);

        if (!metadata_value.has_value()) {
            vlog(
              logger().trace,
              "Skipping topic {} with invalid partition count or RF",
              topic);
            continue;
        }

        auto [partition_count, rf, authorized_operations, topic_id]
          = metadata_value.value();

        vlog(
          logger().trace,
          "Emplacing topic {} with {} partitions, RF={}, topic_id={} for "
          "update candidate",
          topic,
          partition_count,
          rf,
          topic_id);
        candidate_topics.emplace(
          std::move(topic),
          std::make_pair(
            topic_metadata{
              .partition_count = partition_count,
              // Only mirror source topic replication factor if configured to do
              .rf = mirror_rf
                      ? std::make_optional<int16_t>(maybe_clamp_rf(rf, topic))
                      : std::nullopt,
              .topic_id = topic_id},
            std::move(mirror_metadata)));
    }

    return candidate_topics;
}

source_topic_syncer::candidate_create_map
source_topic_syncer::find_candidate_topics_for_creation(
  kafka::client::cluster& cluster, sr_is_empty_t sr_is_empty) {
    auto& topic_cache = cluster.get_topics();
    auto topics = topic_cache.topics();

    /// Map of topics with partition count
    candidate_create_map candidate_topics;
    candidate_topics.reserve(topics.size());

    auto mirror_rf = _config.get_topic_properties_to_mirror().contains(
      kafka::topic_property_replication_factor);

    for (const auto& topic : topics) {
        vlog(logger().trace, "Checking topic: {}", topic);
        auto deny_msg = is_valid_topic(topic, _config.topic_name_filters);
        if (deny_msg.has_value()) {
            vlog(logger().trace, "{}", *deny_msg);
            continue;
        }

        if (!select_topic(topic, _config, _sr_config)) {
            vlog(
              logger().trace,
              "Topic {} does not match inclusion filters",
              topic);
            continue;
        }

        auto metadata_value = validate_topic_cache_entry(
          logger(), topic_cache, topic);
        if (!metadata_value.has_value()) {
            vlog(
              logger().trace,
              "Skipping topic {} with invalid partition count or RF",
              topic);
            continue;
        }

        auto [partition_count, rf, authorized_operations, topic_id]
          = metadata_value.value();

        if (
          authorized_operations == kafka::topic_authorized_operations_not_set) {
            vlog(logger().trace, "Missing permissions for topic {}", topic);
            continue;
        }

        if (!has_required_permissions(
              authorized_operations, required_permissions)) {
            vlog(
              logger().trace,
              "Insufficient permissions for topic {}.  Requires {:08x}, has "
              "{:08x}",
              topic,
              required_permissions,
              authorized_operations);
            continue;
        }

        if (get_link()->get_config()->state.mirror_topics.contains(topic)) {
            vlog(logger().trace, "Topic {} is already being mirrored", topic);
            continue;
        }

        if (
          get_link()
            ->topic_metadata_cache()
            .find_topic_cfg({::model::kafka_namespace, topic})
            .has_value()) {
            if (topic == ::model::schema_registry_internal_tp.topic) {
                // It is possible that the _schemas topic already exists.  The
                // Schema Registry service will create the topic if it doesn't
                // exist upon the first API request the service handles.  If the
                // topic is empty, then we will go ahead and make it a candidate
                // for 'topic creation' for shadowing.  This means we will add
                // that topic to the list of shadow topics.  If the topic
                // doesn't exist, then the topic_reconciler will create it.
                if (!sr_is_empty) {
                    vlog(
                      logger().info,
                      "Unable to shadow topic '{}' since it is not empty. To "
                      "shadow this topic, then please delete it first",
                      ::model::schema_registry_internal_tp.topic);
                    continue;
                }
                vlog(
                  logger().trace,
                  "Topic {} is empty and will be mirrored",
                  ::model::schema_registry_internal_tp.topic);
            } else {
                vlog(logger().trace, "Topic {} already exists", topic);
                continue;
            }
        }

        vlog(logger().debug, "Topic {} is candidate for mirroring", topic);
        candidate_topics.emplace(
          topic,
          topic_metadata{
            .partition_count = partition_count,
            // Only mirror source topic replication factor if configured to do
            // so
            .rf = mirror_rf
                    ? std::make_optional<int16_t>(maybe_clamp_rf(rf, topic))
                    : std::nullopt,
            .topic_id = topic_id});
    }

    return candidate_topics;
}

ss::future<kafka::describe_configs_response>
source_topic_syncer::describe_topics(
  kafka::client::cluster& cluster,
  ::model::node_id controller_id,
  kafka::api_version describe_configs_version,
  const chunked_vector<::model::topic>& topics,
  const properties_set& configs) {
    chunked_vector<ss::sstring> requested_configs;
    requested_configs.reserve(configs.size());
    for (const auto& c : configs) {
        if (c != kafka::topic_property_replication_factor) {
            // Replication factor is synced via topic metadata
            requested_configs.emplace_back(c);
        }
    }

    kafka::describe_configs_request request;
    request.data.include_documentation = false;
    request.data.include_synonyms = false;

    request.data.resources.reserve(topics.size());
    for (const auto& topic : topics) {
        kafka::describe_configs_resource resource;
        resource.resource_type = kafka::config_resource_type::topic;
        resource.resource_name = topic;
        resource.configuration_keys = requested_configs.copy();
        request.data.resources.emplace_back(std::move(resource));
    }

    co_return co_await cluster.dispatch_to(
      controller_id, std::move(request), describe_configs_version);
}

ss::future<source_topic_syncer::sr_is_empty_t>
source_topic_syncer::check_if_schema_registry_is_empty() {
    chunked_vector<::model::partition_id> partitions{
      ::model::schema_registry_internal_tp.partition};
    kafka::data::rpc::topic_partitions sr_hwm_req{
      .topic = ::model::schema_registry_internal_tp.topic,
      .partitions = std::move(partitions),
    };
    chunked_vector<kafka::data::rpc::topic_partitions> req;
    req.emplace_back(std::move(sr_hwm_req));
    auto sr_hwm_res = co_await get_link()
                        ->get_kafka_rpc_client_service()
                        .get_partition_offsets(std::move(req));
    if (!sr_hwm_res.has_value()) {
        vlog(
          logger().warn,
          "Failed to get HWM for schema registry topic: {}",
          sr_hwm_res.assume_error());
        co_return sr_is_empty_t::no;
    }
    auto sr_hwm = std::move(sr_hwm_res).assume_value();
    auto it = sr_hwm.find(::model::schema_registry_internal_tp.topic);
    if (it == sr_hwm.end()) {
        // Topic does not exist, so it's empty
        co_return sr_is_empty_t::yes;
    }
    auto pit = it->second.find(::model::schema_registry_internal_tp.partition);
    if (pit == it->second.end()) {
        // The partition not existing would be an error condition, so assume
        // it is not empty
        co_return sr_is_empty_t::no;
    }
    if (pit->second.err != cluster::errc::success) {
        vlog(
          logger().warn,
          "Failed to get HWM for schema registry topic partition: {}",
          pit->second.err);
        co_return sr_is_empty_t::no;
    }

    co_return pit->second.offsets.high_watermark == kafka::offset(0)
      ? sr_is_empty_t::yes
      : sr_is_empty_t::no;
}

int16_t source_topic_syncer::maybe_clamp_rf(
  int16_t source_rf, const ::model::topic& topic) noexcept {
    auto node_count = get_link()->get_members_table_provider().node_count();
    if (std::cmp_less(node_count, source_rf)) {
        vlog(
          logger().info,
          "Topic {} has a replication factor greater than current node "
          "count ({} > {}).  Clamping replication factor",
          topic,
          source_rf,
          node_count);
        return static_cast<int16_t>(node_count);
    }
    return source_rf;
}

std::string_view
source_topic_syncer_factory::created_task_name() const noexcept {
    return source_topic_syncer::task_name;
}

std::unique_ptr<task> source_topic_syncer_factory::create_task(link* link) {
    return std::make_unique<source_topic_syncer>(link, *(link->get_config()));
}
} // namespace cluster_link
