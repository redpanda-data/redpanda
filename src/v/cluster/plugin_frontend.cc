/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "plugin_frontend.h"

#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/logger.h"
#include "cluster/partition_leaders_table.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/transform.h"
#include "strings/utf8.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_join.h>

#include <utility>

namespace cluster {

using model::transform_id;
using model::transform_metadata;
using model::transform_name;
using mutation_result = plugin_frontend::mutation_result;
namespace {

ss::sstring loggable_string(std::string_view s) {
    constexpr size_t max_log_string_size = 512;
    if (s.size() > max_log_string_size) {
        s = s.substr(0, max_log_string_size);
    }
    if (is_valid_utf8(s) && !contains_control_character(s)) {
        return ss::sstring(s);
    }
    return absl::CHexEscape(s);
}

std::vector<ss::sstring>
loggable_topics(const std::vector<model::topic_namespace>& topics) {
    std::vector<ss::sstring> sanitized;
    sanitized.reserve(topics.size());
    for (const auto& topic : topics) {
        sanitized.push_back(loggable_string(topic.tp()));
    }
    return sanitized;
}

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
            return errc::replication_error;
        }
    }
    if (ec.category() == error_category()) {
        return errc(ec.value());
    }
    return errc::replication_error;
}
} // namespace

plugin_frontend::plugin_frontend(
  model::node_id s,
  partition_leaders_table* l,
  plugin_table* t,
  topic_table* tp,
  controller_stm* c,
  rpc::connection_cache* r,
  ss::abort_source* a)
  : _self(s)
  , _leaders(l)
  , _connections(r)
  , _table(t)
  , _topics(tp)
  , _abort_source(a)
  , _controller(c) {}

ss::future<errc> plugin_frontend::upsert_transform(
  transform_metadata meta, model::timeout_clock::time_point timeout) {
    // The ID is looked up later.
    transform_cmd c{transform_update_cmd{0, std::move(meta)}};
    mutation_result r = co_await do_mutation(std::move(c), timeout);
    co_return r.ec;
}

ss::future<mutation_result> plugin_frontend::remove_transform(
  transform_name name, model::timeout_clock::time_point timeout) {
    transform_cmd c{transform_remove_cmd{std::move(name), 0}};
    return do_mutation(std::move(c), timeout);
}

ss::future<mutation_result> plugin_frontend::do_mutation(
  transform_cmd cmd, model::timeout_clock::time_point timeout) {
    auto cluster_leader = _leaders->get_leader(model::controller_ntp);
    if (!cluster_leader) {
        co_return mutation_result{.ec = errc::no_leader_controller};
    }
    if (*cluster_leader != _self) {
        co_return co_await dispatch_mutation_to_remote(
          *cluster_leader,
          std::move(cmd),
          timeout - model::timeout_clock::now());
    }
    if (ss::this_shard_id() != controller_stm_shard) {
        co_return co_await container().invoke_on(
          controller_stm_shard,
          // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
          [cmd = std::move(cmd), timeout](auto& service) mutable {
              return service.do_mutation(std::move(cmd), timeout);
          });
    }
    co_return co_await do_local_mutation(std::move(cmd), timeout);
}

ss::future<mutation_result> plugin_frontend::dispatch_mutation_to_remote(
  model::node_id cluster_leader,
  transform_cmd cmd,
  model::timeout_clock::duration timeout) {
    return _connections
      ->with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        cluster_leader,
        timeout,
        [timeout,
         cmd = std::move(cmd)](controller_client_protocol client) mutable {
            return ss::visit(
              std::move(cmd),
              [client, timeout](transform_update_cmd cmd) mutable {
                  auto uuid = cmd.value.uuid;
                  return client
                    .upsert_plugin(
                      upsert_plugin_request{
                        .transform = std::move(cmd.value), .timeout = timeout},
                      rpc::client_opts(timeout))
                    .then(&rpc::get_ctx_data<upsert_plugin_response>)
                    .then([uuid](auto r) {
                        if (r.has_error()) {
                            return result<mutation_result>(r.error());
                        }
                        return result<mutation_result>(
                          mutation_result{.uuid = uuid, .ec = r.value().ec});
                    });
              },
              [client, timeout](transform_remove_cmd cmd) mutable {
                  return client
                    .remove_plugin(
                      remove_plugin_request{
                        .name = std::move(cmd.key), .timeout = timeout},
                      rpc::client_opts(timeout))
                    .then(&rpc::get_ctx_data<remove_plugin_response>)
                    .then([](auto r) {
                        if (r.has_error()) {
                            return result<mutation_result>(r.error());
                        }
                        return result<mutation_result>(mutation_result{
                          .uuid = r.value().uuid, .ec = r.value().ec});
                    });
              });
        })
      .then([](result<mutation_result> r) {
          if (r.has_error()) {
              return mutation_result{.ec = map_errc(r.error())};
          }
          return r.value();
      });
}
ss::future<mutation_result> plugin_frontend::do_local_mutation(
  transform_cmd cmd, model::timeout_clock::time_point timeout) {
    auto u = co_await _mu.get_units();
    // Make sure we're up to date
    auto result = co_await _controller->insert_linearizable_barrier(timeout);
    if (!result) {
        co_return mutation_result{.ec = errc::not_leader_controller};
    }
    auto ec = validate_mutation(cmd);
    if (ec != errc::success) {
        co_return mutation_result{.ec = ec};
    }
    bool ok = std::visit(
      [this](const auto& cmd) {
          using T = std::decay_t<decltype(cmd)>;
          return _controller->throttle<T>();
      },
      cmd);
    if (!ok) {
        co_return mutation_result{.ec = errc::throttling_quota_exceeded};
    }
    auto uuid = ss::visit(
      cmd,
      [](const transform_update_cmd& cmd) { return cmd.value.uuid; },
      [this](const transform_remove_cmd& cmd) {
          // This is safe because we've validated the mutation above.
          return _table->find_by_name(cmd.key)->uuid;
      });
    auto b = std::visit(
      [](auto cmd) { return serde_serialize_cmd(std::move(cmd)); },
      std::move(cmd));
    co_return co_await _controller
      ->replicate_and_wait(std::move(b), timeout, *_abort_source)
      .then([uuid](std::error_code ec) {
          return ss::make_ready_future<mutation_result>(
            mutation_result{.uuid = uuid, .ec = map_errc(ec)});
      });
}

errc plugin_frontend::validate_mutation(const transform_cmd& cmd) {
    absl::flat_hash_set<model::topic> no_sink_topics;
    no_sink_topics.insert(model::kafka_audit_logging_topic);
    no_sink_topics.insert(model::kafka_consumer_offsets_topic);
    no_sink_topics.insert(model::schema_registry_internal_tp.topic);
    const auto& noproduce
      = config::shard_local_cfg().kafka_noproduce_topics.value();
    for (const auto& topic : noproduce) {
        no_sink_topics.emplace(topic);
    }
    const size_t max_transforms
      = config::shard_local_cfg()
          .data_transforms_per_core_memory_reservation.value()
        / config::shard_local_cfg()
            .data_transforms_per_function_memory_limit.value();

    validator v(_topics, _table, std::move(no_sink_topics), max_transforms);
    return v.validate_mutation(cmd);
}

plugin_frontend::validator::validator(
  topic_table* topic_table,
  plugin_table* plugin_table,
  absl::flat_hash_set<model::topic> no_sink_topics,
  size_t max)
  : _topics(topic_table)
  , _table(plugin_table)
  , _no_sink_topics(std::move(no_sink_topics))
  , _max_transforms(max) {}

errc plugin_frontend::validator::validate_mutation(const transform_cmd& cmd) {
    return ss::visit(
      cmd,
      [this](const transform_update_cmd& cmd) {
          // Any mutations are allowed to change environment variables, so we
          // always need to validate those
          constexpr static size_t max_key_size = 128;
          constexpr static size_t max_value_size = 2_KiB;
          constexpr static size_t max_env_vars = 128;
          if (cmd.value.environment.size() > max_env_vars) {
              vlog(
                clusterlog.info,
                "deploy of transform {} contained too many environment "
                "variables {} > {}",
                cmd.value.name,
                cmd.value.environment.size(),
                max_env_vars);
              return errc::transform_invalid_environment;
          }
          for (const auto& [k, v] : cmd.value.environment) {
              if (k.empty()) {
                  vlog(
                    clusterlog.info,
                    "attempted deploy of transform {} had an empty environment "
                    "variable key",
                    cmd.value.name);
                  return errc::transform_invalid_environment;
              }
              if (k.size() > max_key_size) {
                  vlog(
                    clusterlog.info,
                    "attempted deploy of transform {} had too large of an "
                    "environment variable key {}",
                    cmd.value.name,
                    k.size(),
                    max_key_size);
                  return errc::transform_invalid_environment;
              }
              if (!is_valid_utf8(k)) {
                  vlog(
                    clusterlog.info,
                    "attempted deploy of transform {} had an environment "
                    "variable key with invalid utf8: {}",
                    cmd.value.name,
                    loggable_string(k));
                  return errc::transform_invalid_environment;
              }
              if (contains_control_character(k)) {
                  vlog(
                    clusterlog.info,
                    "attempted deploy of transform {} had an environment "
                    "variable key with invalid characters: {}",
                    cmd.value.name,
                    loggable_string(k));
                  return errc::transform_invalid_environment;
              }
              if (k.find("=") != ss::sstring::npos) {
                  vlog(
                    clusterlog.info,
                    "attempted deploy of transform {} contained invalid "
                    "environment variable key {}",
                    cmd.value.name,
                    k);
                  return errc::transform_invalid_environment;
              }
              if (k.find("REDPANDA_") == 0) {
                  vlog(
                    clusterlog.info,
                    "attempted deploy of transform {} contained reserved "
                    "environment variable key {}",
                    cmd.value.name,
                    k);
                  return errc::transform_invalid_environment;
              }
              if (v.empty()) {
                  vlog(
                    clusterlog.info,
                    "attempted deploy of transform {} had an empty environment "
                    "variable value for key {}",
                    cmd.value.name,
                    k);
                  return errc::transform_invalid_environment;
              }
              if (v.size() > max_value_size) {
                  vlog(
                    clusterlog.info,
                    "attempted deploy of transform {} had too large of an "
                    "environment variable value {} > {}",
                    cmd.value.name,
                    v.size(),
                    max_value_size);
                  return errc::transform_invalid_environment;
              }
              if (!is_valid_utf8(v)) {
                  vlog(
                    clusterlog.info,
                    "attempted deploy of transform {} had an environment "
                    "variable value with invalid utf8: {}",
                    cmd.value.name,
                    loggable_string(v));
                  return errc::transform_invalid_environment;
              }
              if (contains_control_character(v)) {
                  vlog(
                    clusterlog.info,
                    "attempted deploy of transform {} had an environment "
                    "variable value with invalid characters: {}",
                    cmd.value.name,
                    loggable_string(v));
                  return errc::transform_invalid_environment;
              }
          }

          auto existing = _table->find_by_name(cmd.value.name);
          if (existing.has_value()) {
              // update!
              // Only the offset pointer and environment can change.
              if (existing->input_topic != cmd.value.input_topic) {
                  vlog(
                    clusterlog.info,
                    "deploy of transform {} attempted to change the input "
                    "topic from {} to {}",
                    cmd.value.name,
                    existing->input_topic,
                    loggable_string(cmd.value.input_topic.tp()));
                  return errc::transform_invalid_update;
              }
              if (existing->output_topics != cmd.value.output_topics) {
                  vlog(
                    clusterlog.info,
                    "deploy of transform {} attempted to change the output "
                    "topics from {} to {}",
                    cmd.value.name,
                    existing->output_topics,
                    loggable_topics(cmd.value.output_topics));
                  return errc::transform_invalid_update;
              }
              return errc::success;
          }

          // create!

          if (_table->all_transforms().size() >= _max_transforms) {
              vlog(
                clusterlog.info, "too many transforms, more memory required");
              return errc::transform_count_limit_exceeded;
          }

          if (cmd.value.name().empty()) {
              vlog(
                clusterlog.info,
                "attempted to deploy a transform without a name");
              return errc::transform_invalid_create;
          }
          constexpr static size_t max_name_size = 128;
          if (cmd.value.name().size() > max_name_size) {
              vlog(
                clusterlog.info,
                "attempted to deploy a transform with too large of a name {} > "
                "{}",
                cmd.value.name().size(),
                max_name_size);
              return errc::transform_invalid_create;
          }
          if (!is_valid_utf8(cmd.value.name())) {
              vlog(
                clusterlog.info,
                "attempted to deploy a transform with a non-utf8 name {}",
                loggable_string(cmd.value.name()));
              return errc::transform_invalid_create;
          }
          if (contains_control_character(cmd.value.name())) {
              vlog(
                clusterlog.info,
                "attempted to deploy a transform with an invalid name {}",
                loggable_string(cmd.value.name()));
              return errc::transform_invalid_create;
          }
          if (cmd.value.input_topic.ns != model::kafka_namespace) {
              vlog(
                clusterlog.info,
                "attempted to deploy transform {} to a non-kafka topic {}",
                cmd.value.name,
                loggable_string(cmd.value.input_topic.ns()));
              return errc::transform_invalid_create;
          }
          auto input_topic = _topics->get_topic_metadata(cmd.value.input_topic);
          if (!input_topic) {
              vlog(
                clusterlog.info,
                "attempted to deploy transform {} to a non-existant topic {}",
                cmd.value.name,
                loggable_string(cmd.value.input_topic.tp()));
              return errc::topic_not_exists;
          }
          const auto& input_config = input_topic->get_configuration();
          if (input_config.is_internal()) {
              vlog(
                clusterlog.info,
                "attempted to deploy transform {} to write to a protected "
                "topic {}",
                cmd.value.name,
                loggable_string(cmd.value.input_topic.tp()));
              return errc::transform_invalid_create;
          }
          // TODO: Support input read replicas?
          if (input_config.is_read_replica()) {
              vlog(
                clusterlog.info,
                "attempted to deploy transform {} to an read replica topic {}",
                cmd.value.name,
                loggable_string(cmd.value.input_topic.tp()));
              return errc::transform_invalid_create;
          }
          if (_topics->is_fully_disabled(cmd.value.input_topic)) {
              vlog(
                clusterlog.info,
                "attempted to deploy transform {} to a disabled topic {}",
                cmd.value.name,
                loggable_string(cmd.value.input_topic.tp()));
              return errc::transform_invalid_create;
          }
          if (cmd.value.output_topics.empty()) {
              vlog(
                clusterlog.info,
                "attempted to deploy transform {} without any output topics",
                loggable_string(cmd.value.name()));
              return errc::transform_invalid_create;
          }
          constexpr static size_t max_output_topics = 8;
          if (cmd.value.output_topics.size() > max_output_topics) {
              vlog(
                clusterlog.info,
                "attempted to deploy transform {} with too many output topics: "
                "{} > {}",
                cmd.value.name,
                cmd.value.output_topics.size(),
                max_output_topics);
              return errc::transform_invalid_create;
          }
          absl::flat_hash_set<model::topic_namespace> uniq(
            cmd.value.output_topics.begin(), cmd.value.output_topics.end());
          if (uniq.size() != cmd.value.output_topics.size()) {
              vlog(
                clusterlog.info,
                "attempted to deploy transform {} with duplicate output "
                "topics {}",
                cmd.value.name,
                loggable_topics(cmd.value.output_topics));
              return errc::transform_invalid_create;
          }
          for (const auto& out_name : cmd.value.output_topics) {
              if (out_name.ns != model::kafka_namespace) {
                  vlog(
                    clusterlog.info,
                    "attempted to deploy transform {} to a non-kafka topic {}",
                    cmd.value.name,
                    loggable_string(out_name.ns()));
                  return errc::transform_invalid_create;
              }
              if (_no_sink_topics.contains(out_name.tp)) {
                  vlog(
                    clusterlog.info,
                    "attempted to deploy transform {} to write to an internal "
                    "kafka topic "
                    "{}",
                    cmd.value.name,
                    loggable_string(out_name.tp()));
                  return errc::transform_invalid_create;
              }
              auto output_topic = _topics->get_topic_metadata(out_name);
              if (!output_topic) {
                  vlog(
                    clusterlog.info,
                    "attempted to deploy transform {} to a non-existant topic "
                    "{}",
                    cmd.value.name,
                    loggable_string(out_name.tp()));
                  return errc::topic_not_exists;
              }
              const auto& output_config = output_topic->get_configuration();
              if (output_config.is_internal()) {
                  vlog(
                    clusterlog.info,
                    "attempted to deploy transform {} to an internal topic "
                    "{}",
                    cmd.value.name,
                    out_name.tp);
                  return errc::transform_invalid_create;
              }
              if (output_config.is_read_replica()) {
                  vlog(
                    clusterlog.info,
                    "attempted to deploy transform {} to a read replica topic "
                    "{}",
                    cmd.value.name,
                    out_name.tp);
                  return errc::transform_invalid_create;
              }
              if (_topics->is_fully_disabled(out_name)) {
                  vlog(
                    clusterlog.info,
                    "attempted to deploy transform {} to write to a disabled "
                    "topic {}",
                    cmd.value.name,
                    loggable_string(out_name.tp()));
                  return errc::transform_invalid_create;
              }
              if (would_cause_cycle(cmd.value.input_topic, out_name)) {
                  vlog(
                    clusterlog.info,
                    "attempted to deploy transform {} that would cause a "
                    "cycle",
                    cmd.value.name);
                  return errc::transform_invalid_create;
              }
          }

          return errc::success;
      },
      [this](const transform_remove_cmd& cmd) {
          auto transform = _table->find_by_name(cmd.key);
          if (!transform) {
              vlog(
                clusterlog.info,
                "attempted to delete a transform that does not exist {}",
                cmd.key);
              return errc::transform_does_not_exist;
          }
          return errc::success;
      });
}

bool plugin_frontend::validator::would_cause_cycle(
  model::topic_namespace_view input, model::topic_namespace output) {
    model::topic_namespace_eq eq;
    // Does output ever lead to input? let's find out via breadth first search.
    ss::circular_buffer<model::topic_namespace> queue;
    queue.push_back(std::move(output));
    // explicitly bound loops to protect against looping forever
    constexpr int max_iterations = 10000;
    for (int i = 0; i < max_iterations; ++i) {
        if (queue.empty()) {
            return false;
        }
        model::topic_namespace current = std::move(queue.front());
        queue.pop_front();
        if (eq(input, current)) {
            return true;
        }
        const auto& metas = _table->find_by_input_topic(current);
        for (const auto& [id, meta] : metas) {
            for (const auto& output_topic : meta.output_topics) {
                queue.push_back(output_topic);
            }
        }
    }
    // we reached our max iterations and gave up
    return true;
}

plugin_frontend::notification_id plugin_frontend::register_for_updates(
  plugin_frontend::notification_callback cb) {
    return _table->register_for_updates(std::move(cb));
}

void plugin_frontend::unregister_for_updates(notification_id id) {
    return _table->unregister_for_updates(id);
}
std::optional<transform_metadata>
plugin_frontend::lookup_transform(transform_id id) const {
    return _table->find_by_id(id);
}
std::optional<transform_metadata>
plugin_frontend::lookup_transform(const transform_name& name) const {
    return _table->find_by_name(name);
}
absl::btree_map<transform_id, transform_metadata>
plugin_frontend::lookup_transforms_by_input_topic(
  model::topic_namespace_view tp_ns) const {
    return _table->find_by_input_topic(tp_ns);
}

absl::btree_map<transform_id, transform_metadata>
plugin_frontend::all_transforms() const {
    return _table->all_transforms();
}
} // namespace cluster
