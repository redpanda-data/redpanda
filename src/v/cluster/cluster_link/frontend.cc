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
#include "cluster_link/model/types.h"
#include "config/configuration.h"
#include "model/validation.h"
#include "rpc/connection_cache.h"

namespace cluster::cluster_link {

using ::cluster_link::model::add_mirror_topic_cmd;
using ::cluster_link::model::id_t;
using ::cluster_link::model::metadata;
using ::cluster_link::model::name_t;
using ::cluster_link::model::update_cluster_link_configuration_cmd;
using ::cluster_link::model::update_mirror_topic_properties_cmd;
using ::cluster_link::model::update_mirror_topic_state_cmd;

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
    if (!cluster_linking_enabled()) {
        co_return errc::feature_disabled;
    }
    cluster_link_cmd c{cluster::cluster_link_upsert_cmd{0, std::move(meta)}};
    co_return co_await do_mutation(std::move(c), timeout);
}

ss::future<errc> frontend::remove_cluster_link(
  ::cluster_link::model::name_t name,
  model::timeout_clock::time_point timeout) {
    if (!cluster_linking_enabled()) {
        co_return errc::feature_disabled;
    }
    cluster_link_cmd c{cluster::cluster_link_remove_cmd(std::move(name), 0)};
    co_return co_await do_mutation(std::move(c), timeout);
}

ss::future<errc> frontend::add_mirror_topic(
  id_t id, add_mirror_topic_cmd cmd, model::timeout_clock::time_point timeout) {
    if (!cluster_linking_enabled()) {
        co_return errc::feature_disabled;
    }
    cluster_link_cmd c{
      cluster::cluster_link_add_mirror_topic_cmd(id, std::move(cmd))};
    co_return co_await do_mutation(std::move(c), timeout);
}

ss::future<errc> frontend::update_mirror_topic_state(
  id_t id,
  update_mirror_topic_state_cmd cmd,
  model::timeout_clock::time_point timeout) {
    if (!cluster_linking_enabled()) {
        co_return errc::feature_disabled;
    }
    cluster_link_cmd c{
      cluster::cluster_link_update_mirror_topic_state_cmd(id, std::move(cmd))};
    co_return co_await do_mutation(std::move(c), timeout);
}

ss::future<errc> frontend::update_mirror_topic_properties(
  id_t id,
  update_mirror_topic_properties_cmd cmd,
  model::timeout_clock::time_point timeout) {
    if (!cluster_linking_enabled()) {
        co_return errc::feature_disabled;
    }
    cluster_link_cmd c{cluster::cluster_link_update_mirror_topic_properties_cmd(
      id, std::move(cmd))};
    co_return co_await do_mutation(std::move(c), timeout);
}

ss::future<errc> frontend::update_cluster_link_configuration(
  id_t id,
  update_cluster_link_configuration_cmd cmd,
  model::timeout_clock::time_point timeout) {
    if (!cluster_link_active()) {
        co_return errc::feature_disabled;
    }
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

std::optional<std::reference_wrapper<const metadata>>
frontend::find_link_by_id(id_t id) const {
    return _table->find_link_by_id(id);
}

std::optional<std::reference_wrapper<const metadata>>
frontend::find_link_by_name(const name_t& name) const {
    return _table->find_link_by_name(name);
}

chunked_vector<id_t> frontend::get_all_link_ids() const {
    return _table->get_all_link_ids();
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
    mirror_topics.reserve(link->get().state.mirror_topics.size());
    for (const auto& [topic, metadata] : link->get().state.mirror_topics) {
        mirror_topics.emplace(topic, metadata.copy());
    }
    return mirror_topics;
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
                        .name = std::move(cmd.key), .timeout = timeout},
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
                cluster::cluster_link_update_mirror_topic_state_cmd
                  cmd) mutable {
                  return client
                    .update_mirror_topic_state(
                      cluster::update_mirror_topic_state_request{
                        .link_id = cmd.key,
                        .cmd = std::move(cmd.value),
                        .timeout = timeout},
                      rpc::client_opts(timeout))
                    .then(&rpc::get_ctx_data<
                          cluster::update_mirror_topic_state_response>)
                    .then(
                      [](
                        result<cluster::update_mirror_topic_state_response> r) {
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
      std::move(b), timeout, *_as);
    co_return map_errc(err_code);
}

errc frontend::validate_mutation(const cluster_link_cmd& cmd) const {
    // Initially for DR, we will only support a single cluster link at a time.
    static constexpr size_t max_links = 1;
    validator v{
      _table,
      max_links,
      {"redpanda.remote.readreplica", "redpanda.remote.recovery"}};
    return v.validate_mutation(cmd);
}

frontend::validator::validator(
  table* table,
  size_t max_links,
  chunked_vector<ss::sstring> excluded_topic_properties)
  : _table(table)
  , _max_links(max_links)
  , _excluded_topic_properties(std::move(excluded_topic_properties)) {}

errc frontend::validator::validate_mutation(const cluster_link_cmd& cmd) const {
    return ss::visit(
      cmd,
      [this](const cluster::cluster_link_upsert_cmd& cmd) {
          auto existing = _table->find_link_by_name(cmd.value.name);
          if (existing.has_value()) {
              // upsert
              const auto& meta = existing->get();
              if (meta.uuid != cmd.value.uuid) {
                  // If the UUIDs do not match, it means we are trying to
                  // update an existing link with a different UUID.
                  vlog(
                    cluster::clusterlog.warn,
                    "Attempting to upsert a cluster link with name {} with a "
                    "different UUID ({}) than the existing one ({})",
                    cmd.value.name,
                    cmd.value.uuid,
                    meta.uuid);
                  return errc::uuid_conflict;
              }
              auto ec = validate_connection_config(cmd.value.connection);
              if (ec != errc::success) {
                  return ec;
              }

              return validate_metadata_mirroring_config(
                cmd.value.configuration.topic_metadata_mirroring_cfg);
          }
          // New item!
          if (cmd.value.name().empty()) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to create a cluster link without a name");
              return errc::link_name_invalid;
          }
          constexpr static size_t max_name_size = 128;
          if (cmd.value.name().size() > max_name_size) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to create a cluster link with too large of a "
                "name "
                "{} > {}",
                cmd.value.name().size(),
                max_name_size);
              return errc::link_name_invalid;
          }
          if (!std::ranges::all_of(cmd.value.name(), [](char c) {
                  return std::isalnum(c) || c == '.' || c == '-' || c == '_';
              })) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to create a cluster link with a name containing "
                "invalid characters");
              return errc::link_name_invalid;
          }
          if (_table->size() >= _max_links) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to create a cluster link when the maximum number "
                "of links ({}) is already reached",
                _max_links);
              return errc::limit_exceeded;
          }

          auto ec = validate_connection_config(cmd.value.connection);
          if (ec != errc::success) {
              return ec;
          }

          return validate_metadata_mirroring_config(
            cmd.value.configuration.topic_metadata_mirroring_cfg);
      },
      [this](const cluster::cluster_link_remove_cmd& cmd) {
          auto meta = _table->find_link_by_name(cmd.key);
          if (!meta.has_value()) {
              return errc::does_not_exist;
          }
          return errc::success;
      },
      [this](const cluster::cluster_link_add_mirror_topic_cmd& cmd) {
          auto ec = model::validate_kafka_topic_name(cmd.value.topic);
          if (ec) {
              vlog(cluster::clusterlog.warn, "Invalid topic name: {}", ec);
              return errc::mirror_topic_name_invalid;
          }
          auto meta = _table->find_link_by_id(cmd.key);
          if (!meta.has_value()) {
              return errc::does_not_exist;
          }
          auto id = _table->find_id_by_topic(cmd.value.topic);
          if (id.has_value()) {
              if (id.value() != cmd.key) {
                  vlog(
                    cluster::clusterlog.warn,
                    "Attempting to add mirror topic '{}' to '{}', however it "
                    "is already mirrored by another link",
                    cmd.value.topic,
                    meta->get().name);
                  return errc::topic_being_mirrored_by_other_link;
              } else {
                  vlog(
                    cluster::clusterlog.warn,
                    "Topic '{}' is already mirrored by link '{}'",
                    cmd.value.topic,
                    meta->get().name);
                  return errc::topic_already_being_mirrored;
              }
          }
          if (cmd.value.metadata.partition_count < 1) {
              vlog(
                cluster::clusterlog.warn,
                "Invalid partition count for topic {} in link {}: {}",
                cmd.value.topic,
                meta->get().name,
                cmd.value.metadata.partition_count);
              return errc::invalid_update;
          }
          if (
            cmd.value.metadata.replication_factor.has_value()
            && cmd.value.metadata.replication_factor < 1) {
              vlog(
                cluster::clusterlog.warn,
                "Invalid replication factor: {}",
                cmd.value.metadata.replication_factor);
              return errc::invalid_update;
          }
          return errc::success;
      },
      [this](const cluster::cluster_link_update_mirror_topic_state_cmd& cmd) {
          auto ec = model::validate_kafka_topic_name(cmd.value.topic);
          if (ec) {
              vlog(cluster::clusterlog.warn, "Invalid topic name: {}", ec);
              return errc::mirror_topic_name_invalid;
          }
          auto meta = _table->find_link_by_id(cmd.key);
          if (!meta.has_value()) {
              return errc::does_not_exist;
          }
          auto id = _table->find_id_by_topic(cmd.value.topic);
          if (!id.has_value()) {
              vlog(
                cluster::clusterlog.warn,
                "Topic '{}' is not being mirrored",
                cmd.value.topic);
              return errc::topic_not_being_mirrored;
          } else if (id.value() != cmd.key) {
              vlog(
                cluster::clusterlog.warn,
                "Topic '{}' is being mirrored by another link",
                cmd.value.topic);
              return errc::topic_being_mirrored_by_other_link;
          }
          return errc::success;
      },
      [this](
        const cluster::cluster_link_update_mirror_topic_properties_cmd& cmd) {
          auto ec = model::validate_kafka_topic_name(cmd.value.topic);
          if (ec) {
              vlog(cluster::clusterlog.warn, "Invalid topic name: {}", ec);
              return errc::mirror_topic_name_invalid;
          }
          auto meta = _table->find_link_by_id(cmd.key);
          if (!meta.has_value()) {
              return errc::does_not_exist;
          }
          auto id = _table->find_id_by_topic(cmd.value.topic);
          if (!id.has_value()) {
              vlog(
                cluster::clusterlog.warn,
                "Topic '{}' is not being mirrored",
                cmd.value.topic);
              return errc::topic_not_being_mirrored;
          }
          if (id.value() != cmd.key) {
              vlog(
                cluster::clusterlog.warn,
                "Topic '{}' is being mirrored by another link",
                cmd.value.topic);
              return errc::topic_being_mirrored_by_other_link;
          }
          const auto& mirror_state = meta->get().state;
          const auto it = mirror_state.mirror_topics.find(cmd.value.topic);

          vassert(
            it != mirror_state.mirror_topics.end(),
            "State inconsistency detected, should have been able to find {}",
            cmd.value.topic);

          if (cmd.value.partition_count < it->second.partition_count) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to update partition count of topic '{}' to {}, "
                "which is less than the current partition count {}",
                cmd.value.topic,
                cmd.value.partition_count,
                it->second.partition_count);
              return errc::invalid_update;
          }

          if (
            cmd.value.replication_factor.has_value()
            && cmd.value.replication_factor < 1) {
              vlog(
                cluster::clusterlog.warn,
                "Invalid replication factor: {}",
                cmd.value.replication_factor);
              return errc::invalid_update;
          }

          return errc::success;
      },
      [this](
        const cluster::cluster_link_update_cluster_link_configuration_cmd&
          cmd) {
          if (!_table->find_link_by_id(cmd.key).has_value()) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to update a non-existant link id {}",
                cmd.key);
              return errc::does_not_exist;
          }

          auto ec = validate_connection_config(cmd.value.connection);
          if (ec != errc::success) {
              return ec;
          }

          ec = validate_metadata_mirroring_config(
            cmd.value.link_config.topic_metadata_mirroring_cfg);
          if (ec != errc::success) {
              return ec;
          }

          return errc::success;
      });
}

errc frontend::validator::validate_connection_config(
  const ::cluster_link::model::connection_config& config) const {
    if (config.bootstrap_servers.empty()) {
        vlog(
          cluster::clusterlog.warn,
          "Attempting to create a cluster link without bootstrap servers");
        return errc::bootstrap_servers_empty;
    }

    if (config.cert.has_value() != config.key.has_value()) {
        vlog(
          cluster::clusterlog.warn,
          "If providing a certificate or key, both must be provided or "
          "neither");
        return errc::tls_configuration_invalid;
    }

    if (
      config.cert.has_value()
      && config.cert.value().index() != config.key.value().index()) {
        vlog(
          cluster::clusterlog.warn,
          "If providing a certificate or key, both must be file paths or "
          "both must be values");
        return errc::tls_configuration_invalid;
    }

    if (config.authn_config.has_value()) {
        auto ec = ss::visit(
          config.authn_config.value(),
          [](const ::cluster_link::model::scram_credentials& c) {
              if (c.username.empty()) {
                  vlog(
                    cluster::clusterlog.warn,
                    "Username for SCRAM credentials is empty");
                  return errc::scram_configuration_invalid;
              }

              if (c.password.empty()) {
                  vlog(
                    cluster::clusterlog.warn,
                    "Password for SCRAM credentials is empty");
                  return errc::scram_configuration_invalid;
              }

              if (
                c.mechanism != "SCRAM-SHA-256"
                && c.mechanism != "SCRAM-SHA-512") {
                  vlog(
                    cluster::clusterlog.warn,
                    "Unsupported SCRAM mechanism: {}",
                    c.mechanism);
                  return errc::scram_configuration_invalid;
              }
              return errc::success;
          });

        if (ec != errc::success) {
            return ec;
        }
    }

    return errc::success;
}

errc frontend::validator::validate_metadata_mirroring_config(
  const ::cluster_link::model::topic_metadata_mirroring_config& config) const {
    // Validates that the pattern:
    // - is not empty
    // - does not contain the wildcard character '*' unless it is the only
    //   character in the pattern
    // - the characters are valid UTF-8
    // - wildcard only present in 'literal' patterns
    const auto check_filter_pattern =
      [](const ::cluster_link::model::resource_name_filter_pattern& p) {
          if (p.pattern.empty()) {
              vlog(cluster::clusterlog.info, "Filter pattern is empty");
              return true;
          }
          if (
            p.pattern.contains(
              ::cluster_link::model::resource_name_filter_pattern::wildcard)
            && p.pattern
                 != ::cluster_link::model::resource_name_filter_pattern::
                   wildcard) {
              vlog(
                cluster::clusterlog.info,
                "Filter pattern is invalid: Contains '*'");
              return true;
          }
          if (
            p.pattern
              == ::cluster_link::model::resource_name_filter_pattern::wildcard
            && p.pattern_type
                 != ::cluster_link::model::filter_pattern_type::literal) {
              vlog(
                cluster::clusterlog.info,
                "Filter pattern is invalid: Wildcard '*' can only be used in "
                "literal patterns");
              return true;
          }
          if (
            p.pattern
              != ::cluster_link::model::resource_name_filter_pattern::wildcard
            && !std::ranges::all_of(p.pattern, [](char c) {
                   return std::isalnum(c) || c == '.' || c == '-' || c == '_';
               })) {
              vlog(
                cluster::clusterlog.info,
                "Filter pattern contains invalid characters");
              return true;
          }
          if (
            p.pattern.starts_with("_redpanda")
            || p.pattern.starts_with("__redpanda")
            || p.pattern == ::model::kafka_consumer_offsets_topic()) {
              vlog(
                cluster::clusterlog.info,
                "Filter pattern filtering on invalid topic name: {}",
                p.pattern);
              return true;
          }
          return false;
      };
    if (std::ranges::any_of(config.topic_name_filters, check_filter_pattern)) {
        return errc::topic_filter_invalid;
    }
    for (const auto& prop : config.topic_properties_to_mirror) {
        if (
          std::ranges::find(_excluded_topic_properties, prop)
          != _excluded_topic_properties.end()) {
            vlog(
              cluster::clusterlog.info,
              "Topic property '{}' is excluded from mirroring",
              prop);
            return errc::topic_property_excluded_from_mirroring;
        }
    }

    return errc::success;
}
} // namespace cluster::cluster_link
