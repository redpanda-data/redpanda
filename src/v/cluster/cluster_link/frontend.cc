/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cluster_link/frontend.h"

#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/types.h"
#include "cluster_link/model/filter_utils.h"
#include "cluster_link/model/types.h"
#include "config/configuration.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "rpc/connection_cache.h"
#include "ssx/when_all.h"

namespace cluster::cluster_link {

using ::cluster_link::model::add_mirror_topic_cmd;
using ::cluster_link::model::delete_mirror_topic_cmd;
using ::cluster_link::model::id_t;
using ::cluster_link::model::metadata;
using ::cluster_link::model::metadata_ptr;
using ::cluster_link::model::name_t;
using ::cluster_link::model::update_cluster_link_configuration_cmd;
using ::cluster_link::model::update_mirror_topic_properties_cmd;
using ::cluster_link::model::update_mirror_topic_status_cmd;

namespace {
errc map_errc(std::error_code ec) {
    if (ec == errc::success) {
        return errc::success;
    }
    if (ec.category() == raft::error_category()) {
        switch (raft::errc(ec.value())) {
        case raft::errc::timeout:
            return errc::timeout;
        case raft::errc::not_leader:
            return errc::not_leader_controller;
        default:
            return errc::replication_error;
        }
    }
    if (ec.category() == rpc::error_category()) {
        switch (rpc::errc(ec.value())) {
        case rpc::errc::client_request_timeout:
            return errc::timeout;
        default:
            return errc::rpc_error;
        }
    }
    if (ec.category() == cluster::error_category()) {
        switch (cluster::errc(ec.value())) {
        case cluster::errc::not_leader_controller:
            return errc::not_leader_controller;
        case cluster::errc::replication_error:
            return errc::replication_error;
        case cluster::errc::timeout:
            return errc::timeout;
        default:
            return errc::rpc_error;
        }
    }
    if (ec.category() == error_category()) {
        return errc(ec.value());
    }
    return errc::rpc_error;
}

bool is_topic_mutable(::cluster_link::model::mirror_topic_status status) {
    switch (status) {
    case ::cluster_link::model::mirror_topic_status::active:
    case ::cluster_link::model::mirror_topic_status::failed:
    case ::cluster_link::model::mirror_topic_status::paused:
    case ::cluster_link::model::mirror_topic_status::failing_over:
    case ::cluster_link::model::mirror_topic_status::promoting:
        return false;
    case ::cluster_link::model::mirror_topic_status::failed_over:
    case ::cluster_link::model::mirror_topic_status::promoted:
        return true;
    }
}
} // namespace

frontend::frontend(
  model::node_id self,
  cluster::partition_leaders_table* leaders,
  table* table,
  cluster::controller_stm* controller,
  rpc::connection_cache* connections,
  features::feature_table* features,
  ss::abort_source* as)
  : _self(self)
  , _leaders(leaders)
  , _connections(connections)
  , _table(table)
  , _as(as)
  , _controller(controller)
  , _features(features) {}

ss::future<errc> frontend::upsert_cluster_link(
  ::cluster_link::model::metadata meta,
  model::timeout_clock::time_point timeout) {
    if (is_sanctioned()) {
        vlog(
          clusterlog.warn,
          "Missing license - unable to create new shadow link");
        co_return errc::license_required;
    }
    cluster_link_cmd c{cluster::cluster_link_upsert_cmd{0, std::move(meta)}};
    co_return co_await do_mutation(std::move(c), timeout);
}

ss::future<errc> frontend::remove_cluster_link(
  ::cluster_link::model::name_t name,
  bool force,
  model::timeout_clock::time_point timeout) {
    cluster_link_cmd c{cluster::cluster_link_remove_cmd(
      0,
      ::cluster_link::model::delete_shadow_link_cmd{
        .link_name = std::move(name), .force = force})};
    co_return co_await do_mutation(std::move(c), timeout);
}

ss::future<errc> frontend::add_mirror_topic(
  id_t id, add_mirror_topic_cmd cmd, model::timeout_clock::time_point timeout) {
    cluster_link_cmd c{
      cluster::cluster_link_add_mirror_topic_cmd(id, std::move(cmd))};
    co_return co_await do_mutation(std::move(c), timeout);
}

ss::future<errc> frontend::delete_mirror_topic(
  id_t id,
  delete_mirror_topic_cmd cmd,
  model::timeout_clock::time_point timeout) {
    cluster_link_cmd c{
      cluster::cluster_link_delete_mirror_topic_cmd(id, std::move(cmd))};
    co_return co_await do_mutation(std::move(c), timeout);
}

ss::future<errc> frontend::update_mirror_topic_status(
  id_t id,
  update_mirror_topic_status_cmd cmd,
  model::timeout_clock::time_point timeout) {
    if (!cluster_linking_enabled()) {
        co_return errc::feature_disabled;
    }
    cluster_link_cmd c{
      cluster::cluster_link_update_mirror_topic_status_cmd(id, std::move(cmd))};
    co_return co_await do_mutation(std::move(c), timeout);
}

ss::future<errc> frontend::update_mirror_topic_properties(
  id_t id,
  update_mirror_topic_properties_cmd cmd,
  model::timeout_clock::time_point timeout) {
    cluster_link_cmd c{cluster::cluster_link_update_mirror_topic_properties_cmd(
      id, std::move(cmd))};
    co_return co_await do_mutation(std::move(c), timeout);
}

ss::future<errc> frontend::update_cluster_link_configuration(
  id_t id,
  update_cluster_link_configuration_cmd cmd,
  model::timeout_clock::time_point timeout) {
    cluster_link_cmd c{
      cluster::cluster_link_update_cluster_link_configuration_cmd(
        id, std::move(cmd))};
    co_return co_await do_mutation(std::move(c), timeout);
}

bool frontend::cluster_link_active() const {
    return _table->cluster_link_active();
}

bool frontend::cluster_linking_enabled() const {
    return config::shard_local_cfg().enable_shadow_linking()
           && _features->is_active(features::feature::shadow_linking);
}

frontend::notification_id
frontend::register_for_updates(notification_callback cb) {
    return _table->register_for_updates(std::move(cb));
}

void frontend::unregister_for_updates(notification_id id) {
    _table->unregister_for_updates(id);
}

std::optional<id_t> frontend::find_link_id_by_name(const name_t& name) const {
    return _table->find_id_by_name(name);
}

std::optional<id_t>
frontend::find_link_id_by_topic(model::topic_view topic) const {
    return _table->find_id_by_topic(topic);
}

metadata_ptr frontend::find_link_by_id(id_t id) const {
    return _table->find_link_by_id(id);
}

metadata_ptr frontend::find_link_by_name(const name_t& name) const {
    return _table->find_link_by_name(name);
}

chunked_vector<id_t> frontend::get_all_link_ids() const {
    return _table->get_all_link_ids();
}

bool frontend::is_topic_mutable_for_kafka_api(const model::topic& topic) const {
    auto status = _table->find_mirror_topic_status(topic);
    if (!status) {
        // topic does not belong to any cluster link
        return true;
    }
    return is_topic_mutable(*status);
}

std::optional<chunked_hash_map<
  ::model::topic,
  ::cluster_link::model::mirror_topic_metadata>>
frontend::get_mirror_topics_for_link(id_t id) const {
    auto link = _table->find_link_by_id(id);
    if (!link) {
        return std::nullopt;
    }
    chunked_hash_map<
      ::model::topic,
      ::cluster_link::model::mirror_topic_metadata>
      mirror_topics;
    mirror_topics.reserve(link->state.mirror_topics.size());
    for (const auto& [topic, metadata] : link->state.mirror_topics) {
        mirror_topics.emplace(topic, metadata.copy());
    }
    return mirror_topics;
}

std::optional<::model::revision_id> frontend::get_last_update_revision(
  const ::cluster_link::model::id_t& id) const {
    return _table->get_link_last_update_revision(id);
}

bool frontend::is_autocreate_mirror_topic(const model::topic& topic) const {
    auto link_id = _table->find_id_by_topic(topic);
    if (!link_id.has_value()) {
        return false;
    }
    auto link = _table->find_link_by_id(link_id.value());
    vassert(
      link != nullptr, "Expected value for link with id {}", link_id.value());
    const auto& topic_filters
      = link->configuration.topic_metadata_mirroring_cfg.topic_name_filters;
    return ::cluster_link::model::select_topic(topic, topic_filters);
}

ss::future<topic_result> frontend::delete_mirror_topic(
  model::topic topic, model::timeout_clock::time_point timeout) {
    const auto& link_id = _table->find_id_by_topic(topic);
    if (!link_id.has_value()) {
        co_return topic_result{
          std::move(topic), errc::topic_not_being_mirrored};
    }

    topic_result ret;
    ret.topic = topic;
    ret.ec = co_await delete_mirror_topic(
      link_id.value(),
      delete_mirror_topic_cmd{.topic = std::move(topic)},
      timeout);

    co_return ret;
}

ss::future<chunked_vector<topic_result>> frontend::delete_mirror_topics(
  chunked_vector<model::topic> topics,
  model::timeout_clock::time_point timeout) {
    vlog(clusterlog.debug, "Deleting mirror topics {}", topics);

    const auto do_delete =
      [this, timeout](model::topic t) -> ss::future<topic_result> {
        return delete_mirror_topic(std::move(t), timeout);
    };

    auto fut_r = topics | std::views::as_rvalue
                 | std::views::transform(do_delete);
    chunked_vector<ss::future<topic_result>> futures{
      fut_r.begin(), fut_r.end()};
    return ssx::when_all_succeed<chunked_vector<topic_result>>(
      std::move(futures));
}

bool frontend::schema_registry_shadowing_active() const {
    if (!cluster_link_active()) {
        // If not shadow links are active then quick exit
        return false;
    }

    auto link_ids = get_all_link_ids();
    return std::ranges::any_of(link_ids, [this](id_t link_id) -> bool {
        const auto md = find_link_by_id(link_id);
        if (!md) {
            return false;
        }
        // Check to see if the schema registry topic is in the mirror topic list
        const auto& mirror_topics = md->state.mirror_topics;
        auto topic_it = mirror_topics.find(
          ::model::schema_registry_internal_tp.topic);
        if (topic_it != mirror_topics.end()) {
            // If it is, return whether or not it is mutable based on its status
            return !is_topic_mutable(topic_it->second.status);
        }
        // If mirror_schema_registry_topic option is set and the topic is not
        // yet in the mirror topic list, then shadowing for SR is active
        const auto& sr_cfg = md->configuration.schema_registry_sync_cfg;
        if (
          sr_cfg.sync_schema_registry_topic_mode.has_value()
          && std::holds_alternative<
            ::cluster_link::model::schema_registry_sync_config::
              shadow_entire_schema_registry>(
            sr_cfg.sync_schema_registry_topic_mode.value())) {
            return true;
        }

        return false;
    });
}

ss::future<errc> frontend::do_mutation(
  cluster_link_cmd cmd, model::timeout_clock::time_point timeout) {
    auto cluster_leader = _leaders->get_leader(model::controller_ntp);
    if (!cluster_leader) {
        co_return errc::not_leader_controller;
    }
    if (*cluster_leader != _self) {
        co_return co_await dispatch_mutation_to_remote(
          *cluster_leader,
          std::move(cmd),
          timeout - model::timeout_clock::now());
    }

    co_return co_await container().invoke_on(
      cluster::controller_stm_shard,
      [cmd = std::move(cmd), timeout](auto& service) mutable {
          return service.do_local_mutation(std::move(cmd), timeout);
      });
}

ss::future<errc> frontend::dispatch_mutation_to_remote(
  model::node_id cluster_leader,
  cluster_link_cmd cmd,
  model::timeout_clock::duration timeout) {
    return _connections
      ->with_node_client<cluster::controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        cluster_leader,
        timeout,
        [timeout, cmd = std::move(cmd)](
          cluster::controller_client_protocol client) mutable {
            return ss::visit(
              std::move(cmd),
              [client, timeout](cluster::cluster_link_upsert_cmd cmd) mutable {
                  return client
                    .upsert_cluster_link(
                      cluster::upsert_cluster_link_request{
                        .metadata = std::move(cmd.value), .timeout = timeout},
                      rpc::client_opts(timeout))
                    .then(
                      &rpc::get_ctx_data<cluster::upsert_cluster_link_response>)
                    .then([](result<cluster::upsert_cluster_link_response> r) {
                        if (r.has_error()) {
                            return result<void>(r.error());
                        }
                        return result<void>(r.value().ec);
                    });
              },
              [client, timeout](cluster::cluster_link_remove_cmd cmd) mutable {
                  return client
                    .remove_cluster_link(
                      cluster::remove_cluster_link_request{
                        .cmd = std::move(cmd.value), .timeout = timeout},
                      rpc::client_opts(timeout))
                    .then(
                      &rpc::get_ctx_data<cluster::remove_cluster_link_response>)
                    .then([](result<cluster::remove_cluster_link_response> r) {
                        if (r.has_error()) {
                            return result<void>(r.error());
                        }
                        return result<void>(r.value().ec);
                    });
              },
              [client, timeout](
                cluster::cluster_link_add_mirror_topic_cmd cmd) mutable {
                  return client
                    .add_mirror_topic(
                      cluster::add_mirror_topic_request{
                        .link_id = cmd.key,
                        .cmd = std::move(cmd.value),
                        .timeout = timeout},
                      rpc::client_opts(timeout))
                    .then(
                      &rpc::get_ctx_data<cluster::add_mirror_topic_response>)
                    .then([](result<cluster::add_mirror_topic_response> r) {
                        if (r.has_error()) {
                            return result<void>(r.error());
                        }
                        return result<void>(r.value().ec);
                    });
              },
              [client, timeout](
                cluster::cluster_link_update_mirror_topic_status_cmd
                  cmd) mutable {
                  return client
                    .update_mirror_topic_status(
                      cluster::update_mirror_topic_status_request{
                        .link_id = cmd.key,
                        .cmd = std::move(cmd.value),
                        .timeout = timeout},
                      rpc::client_opts(timeout))
                    .then(&rpc::get_ctx_data<
                          cluster::update_mirror_topic_status_response>)
                    .then([](
                            result<cluster::update_mirror_topic_status_response>
                              r) {
                        if (r.has_error()) {
                            return result<void>(r.error());
                        }
                        return result<void>(r.value().ec);
                    });
              },
              [client, timeout](
                cluster::cluster_link_update_mirror_topic_properties_cmd
                  cmd) mutable {
                  return client
                    .update_mirror_topic_properties(
                      cluster::update_mirror_topic_properties_request{
                        .link_id = cmd.key,
                        .cmd = std::move(cmd.value),
                        .timeout = timeout},
                      rpc::client_opts(timeout))
                    .then(&rpc::get_ctx_data<
                          cluster::update_mirror_topic_properties_response>)
                    .then(
                      [](
                        result<cluster::update_mirror_topic_properties_response>
                          r) {
                          if (r.has_error()) {
                              return result<void>(r.error());
                          }
                          return result<void>(r.value().ec);
                      });
              },
              [client, timeout](
                cluster::cluster_link_delete_mirror_topic_cmd cmd) mutable {
                  return client
                    .delete_mirror_topic(
                      cluster::delete_mirror_topic_request{
                        .link_id = cmd.key,
                        .cmd = std::move(cmd.value),
                        .timeout = timeout},
                      rpc::client_opts(timeout))
                    .then(
                      &rpc::get_ctx_data<cluster::delete_mirror_topic_response>)
                    .then([](result<cluster::delete_mirror_topic_response> r) {
                        if (r.has_error()) {
                            return result<void>(r.error());
                        }
                        return result<void>(r.value().ec);
                    });
              },
              [client, timeout](
                cluster::cluster_link_update_cluster_link_configuration_cmd
                  cmd) mutable {
                  return client
                    .update_cluster_link_configuration(
                      cluster::update_cluster_link_configuration_request{
                        .link_id = cmd.key,
                        .cmd = std::move(cmd.value),
                        .timeout = timeout},
                      rpc::client_opts(timeout))
                    .then(&rpc::get_ctx_data<
                          cluster::update_cluster_link_configuration_response>)
                    .then(
                      [](
                        result<
                          cluster::update_cluster_link_configuration_response>
                          r) {
                          if (r.has_error()) {
                              return result<void>(r.error());
                          }
                          return result<void>(r.value().ec);
                      });
              });
        })
      .then([](result<void> r) {
          if (r.has_error()) {
              return map_errc(r.error());
          }
          return errc::success;
      });
}

ss::future<errc> frontend::do_local_mutation(
  cluster_link_cmd cmd, model::timeout_clock::time_point timeout) {
    auto u = co_await _mu.get_units();
    auto result = co_await _controller->insert_linearizable_barrier(timeout);
    if (!result) {
        co_return errc::not_leader_controller;
    }
    auto [_, term] = result.value();
    auto ec = validate_mutation(cmd);
    if (ec != errc::success) {
        co_return ec;
    }
    auto ok = std::visit(
      [this](const auto& cmd) {
          using T = std::decay_t<decltype(cmd)>;
          return _controller->throttle<T>();
      },
      cmd);
    if (!ok) {
        co_return errc::throttling_quota_exceeded;
    };

    auto b = std::visit(
      [](auto cmd) { return serde_serialize_cmd(std::move(cmd)); },
      std::move(cmd));
    auto err_code = co_await _controller->replicate_and_wait(
      std::move(b), timeout, *_as, term);
    co_return map_errc(err_code);
}

} // namespace cluster::cluster_link
