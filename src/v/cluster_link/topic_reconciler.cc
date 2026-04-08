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

#include "cluster_link/topic_reconciler.h"

#include "cluster/types.h"
#include "cluster_link/logger.h"
#include "cluster_link/model/types.h"
#include "cluster_link/utils/topic_properties_utils.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/switch_to.hh>

#include <exception>

using namespace std::chrono_literals;

namespace cluster_link {
topic_reconciler::topic_reconciler(
  kafka::data::rpc::topic_creator* topic_creator,
  kafka::data::rpc::topic_metadata_cache* topic_metadata_cache,
  link_registry* link_registry,
  ss::lowres_clock::duration run_interval,
  config::binding<int16_t> default_topic_replication,
  ss::scheduling_group scheduling_group)
  : _topic_creator(topic_creator)
  , _topic_metadata_cache(topic_metadata_cache)
  , _link_registry(link_registry)
  , _run_interval(run_interval)
  , _default_topic_replication(std::move(default_topic_replication))
  , _scheduling_group(scheduling_group) {}

ss::future<> topic_reconciler::start() {
    vlog(cllog.info, "Starting topic reconciler");
    _reconciler_timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] {
            return execute()
              .handle_exception([](const std::exception_ptr& e) {
                  vlogl(
                    cllog,
                    ssx::is_shutdown_exception(e) ? ss::log_level::trace
                                                  : ss::log_level::error,
                    "Failure during topic reconciliation: {}",
                    e);
              })
              .finally([this] {
                  if (_as.abort_requested()) {
                      return;
                  }
                  _reconciler_timer.arm(_run_interval);
              });
        });
    });
    _reconciler_timer.arm(_run_interval);
    co_return;
}

ss::future<> topic_reconciler::stop() noexcept {
    vlog(cllog.info, "Stopping topic reconciler");
    _reconciler_timer.cancel();
    _as.request_abort();
    co_await _gate.close();
    vlog(cllog.info, "Topic Reconciler stopped");
}

void topic_reconciler::trigger(model::id_t link_id) {
    vlog(cllog.trace, "Triggering topic reconciler for link id: {}", link_id);
    ssx::spawn_with_gate(_gate, [this, link_id] {
        return execute(link_id).handle_exception(
          [](const std::exception_ptr& e) {
              auto level = ssx::is_shutdown_exception(e) ? ss::log_level::trace
                                                         : ss::log_level::warn;
              vlogl(cllog, level, "Topic reconciler failed: {}", e);
          });
    });
}

ss::future<> topic_reconciler::execute(std::optional<model::id_t> link_id) {
    co_await ss::coroutine::switch_to(_scheduling_group);
    static constexpr auto max_concurrent_reconcilations = 5;
    vlog(cllog.trace, "Executing topic reconciler, link_id: {}", link_id);
    auto units = co_await _reconciler_mutex.get_units(_as);
    if (link_id.has_value()) {
        co_return co_await do_reconcile_topic(link_id.value());
    }

    auto link_ids = _link_registry->get_all_link_ids();
    co_return co_await ss::max_concurrent_for_each(
      link_ids, max_concurrent_reconcilations, [this](model::id_t id) {
          return do_reconcile_topic(id);
      });
}

ss::future<> topic_reconciler::do_reconcile_topic(model::id_t link_id) {
    vlog(cllog.trace, "Reconciling link with id: {}", link_id);
    auto link = _link_registry->find_link_by_id(link_id);
    if (!link) {
        vlog(cllog.warn, "Link with id: {} not found", link_id);
        co_return;
    }

    chunked_hash_map<::model::topic, model::mirror_topic_metadata>
      mirror_topics;
    const auto& to_copy = link->state.mirror_topics;
    mirror_topics.reserve(to_copy.size());
    for (const auto& [k, v] : to_copy) {
        mirror_topics.emplace(k, v.copy());
    }

    for (auto& [topic_name, mirror_topic_config] : mirror_topics) {
        vlog(cllog.trace, "Checking topic: {}", topic_name);
        // Do not attempt to reconcile mirror topics that have failed or are
        // being promoted
        if (
          mirror_topic_config.status == model::mirror_topic_status::failed
          || mirror_topic_config.status
               == model::mirror_topic_status::promoted) {
            vlog(
              cllog.trace,
              "Skipping topic {} with state {}",
              topic_name,
              mirror_topic_config.status);
            continue;
        }

        auto local_topic_config = _topic_metadata_cache->find_topic_cfg(
          {::model::kafka_namespace, topic_name});

        // First reconcile any existing topics
        if (local_topic_config.has_value()) {
            co_await maybe_update_existing_topic(
              link_id,
              std::move(topic_name),
              std::move(mirror_topic_config),
              std::move(local_topic_config).value());
        } else {
            co_await maybe_create_mirror_topic(
              std::move(topic_name), std::move(mirror_topic_config));
        }
    }
}

ss::future<> topic_reconciler::maybe_update_existing_topic(
  model::id_t link_id,
  ::model::topic topic,
  model::mirror_topic_metadata mirror_topic_config,
  cluster::topic_configuration topic_configuration) {
    const auto update_timeout = 5s;
    vlog(cllog.trace, "Checking if mirror topic {} needs to be updated", topic);
    auto err_msg = check_if_source_topic_is_invalid(
      topic_configuration, mirror_topic_config);
    if (err_msg.has_value()) {
        vlog(
          cllog.warn,
          "Source topic {} is invalid: '{}'. Marking topic as failed",
          topic,
          err_msg.value());
        auto res = co_await _link_registry->update_mirror_topic_state(
          link_id,
          model::update_mirror_topic_status_cmd{
            .topic = topic,
            .status = model::mirror_topic_status::failed,
          },
          ::model::timeout_clock::now() + update_timeout);
        if (res != ::cluster::cluster_link::errc::success) {
            vlog(
              cllog.warn,
              "Failed to mark mirror topic {} as failed: {}",
              topic,
              res);
        } else {
            vlog(
              cllog.trace,
              "Successfully marked mirror topic {} as failed",
              topic);
        }
        co_return;
    }

    if (
      mirror_topic_config.partition_count
      > topic_configuration.partition_count) {
        vlog(
          cllog.trace,
          "Updating mirroring topic {} partition count: {} -> {}",
          topic,
          topic_configuration.partition_count,
          mirror_topic_config.partition_count);
        auto res = co_await _topic_creator->create_partitions(
          {::model::kafka_namespace, topic},
          mirror_topic_config.partition_count,
          ::model::timeout_clock::now() + update_timeout);
        if (res != cluster::errc::success) {
            vlog(
              cllog.warn,
              "Failed to set partition count on mirror topic {} to {}: {}",
              topic,
              mirror_topic_config.partition_count,
              res);
        } else {
            vlog(
              cllog.trace,
              "Successfully set partition count on mirror topic {} to {}",
              topic,
              mirror_topic_config.partition_count);
        }
    }

    auto update_cfg = maybe_create_update_mirror_topic(
      topic, topic_configuration, mirror_topic_config);
    if (!update_cfg.has_value()) {
        vlog(
          cllog.trace, "Mirror topic {} is stable, no need to update", topic);
        co_return;
    }

    vlog(
      cllog.trace,
      "Detecting config change for topic {}: {}",
      topic,
      update_cfg.value());

    auto res = co_await _topic_creator->update_topic(
      std::move(update_cfg).value());
    if (res != cluster::errc::success) {
        vlog(cllog.warn, "Failed to update mirror topic {}: {}", topic, res);
    } else {
        vlog(
          cllog.trace,
          "Successfully updated mirror topic {} with new configs",
          topic);
    }
}

ss::future<> topic_reconciler::maybe_create_mirror_topic(
  ::model::topic topic, model::mirror_topic_metadata mirror_topic_config) {
    vlog(cllog.trace, "Topic {} does not exist, attempting creation", topic);

    kafka::config_map_t topic_configs;
    topic_configs.reserve(mirror_topic_config.topic_configs.size());
    for (auto& [config_name, config_value] :
         mirror_topic_config.topic_configs) {
        topic_configs.emplace(std::move(config_name), std::move(config_value));
    }

    auto cfg = kafka::to_topic_config(
      ::model::kafka_namespace,
      topic,
      mirror_topic_config.partition_count,
      mirror_topic_config.replication_factor.value_or(
        _default_topic_replication()),
      topic_configs);

    auto res = co_await _topic_creator->create_topic(
      {::model::kafka_namespace, topic},
      mirror_topic_config.partition_count,
      std::move(cfg.properties),
      mirror_topic_config.replication_factor.value_or(
        _default_topic_replication()));

    if (res != cluster::errc::success) {
        vlog(cllog.warn, "Failed to create mirror topic {}: {}", topic, res);
    } else {
        vlog(cllog.debug, "Successfully created mirror topic {}", topic);
    }
}

std::optional<cluster::topic_properties_update>
topic_reconciler::maybe_create_update_mirror_topic(
  const ::model::topic& topic_name,
  const cluster::topic_configuration& local_topic_config,
  const model::mirror_topic_metadata& mirror_topic_config) {
    bool config_updated = false;
    cluster::topic_properties_update update(
      {::model::kafka_namespace, topic_name});

    if (
      mirror_topic_config.replication_factor.has_value()
      && *mirror_topic_config.replication_factor
           != local_topic_config.replication_factor) {
        vlog(
          cllog.debug,
          "Updating replication factor for topic {} from {} to {}",
          topic_name,
          local_topic_config.replication_factor,
          mirror_topic_config.replication_factor);
        update.custom_properties.replication_factor.op
          = cluster::incremental_update_operation::set;
        update.custom_properties.replication_factor.value
          = cluster::replication_factor(
            mirror_topic_config.replication_factor.value());
        config_updated = true;
    }

    for (const auto& [source_topic_config_name, source_topic_config_value] :
         mirror_topic_config.topic_configs) {
        // Need to check the current value of the local topic config
        // against that stored in the table, so I need to go from
        // string name to actual type
        try {
            if (
              utils::maybe_append_update(
                update,
                source_topic_config_name,
                source_topic_config_value,
                local_topic_config)) {
                config_updated = true;
            }
        } catch (const std::exception& e) {
            vlog(
              cllog.warn,
              "Failed updating topic property {} for topic {}: {}",
              source_topic_config_name,
              topic_name,
              e);
        }
    }

    if (config_updated) {
        return update;
    }

    return std::nullopt;
}

std::optional<ss::sstring> topic_reconciler::check_if_source_topic_is_invalid(
  const cluster::topic_configuration& local_topic_config,
  const model::mirror_topic_metadata& mirror_topic_config) {
    if (
      mirror_topic_config.partition_count
      < local_topic_config.partition_count) {
        return ssx::sformat(
          "Partition count has decreased from {} to {}",
          local_topic_config.partition_count,
          mirror_topic_config.partition_count);
    }
    return std::nullopt;
}
} // namespace cluster_link
