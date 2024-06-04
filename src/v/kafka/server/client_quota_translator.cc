/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/client_quota_translator.h"

#include "config/configuration.h"

#include <optional>
#include <utility>

namespace kafka {

std::ostream& operator<<(std::ostream& os, const client_quota_limits& l) {
    fmt::print(
      os,
      "limits{{produce_limit: {}, fetch_limit: {}, "
      "partition_mutation_limit: {}}}",
      l.produce_limit,
      l.fetch_limit,
      l.partition_mutation_limit);
    return os;
}

client_quota_translator::client_quota_translator()
  : _default_target_produce_tp_rate(
    config::shard_local_cfg().target_quota_byte_rate.bind())
  , _default_target_fetch_tp_rate(
      config::shard_local_cfg().target_fetch_quota_byte_rate.bind())
  , _target_partition_mutation_quota(
      config::shard_local_cfg().kafka_admin_topic_api_rate.bind())
  , _target_produce_tp_rate_per_client_group(
      config::shard_local_cfg().kafka_client_group_byte_rate_quota.bind())
  , _target_fetch_tp_rate_per_client_group(
      config::shard_local_cfg()
        .kafka_client_group_fetch_byte_rate_quota.bind()) {}

uint64_t client_quota_translator::get_client_target_produce_tp_rate(
  const tracker_key& quota_id) {
    return ss::visit(
      quota_id,
      [this](const k_client_id&) -> uint64_t {
          return _default_target_produce_tp_rate();
      },
      [this](const k_group_name& k) -> uint64_t {
          auto group = _target_produce_tp_rate_per_client_group().find(k);
          if (group != _target_produce_tp_rate_per_client_group().end()) {
              return group->second.quota;
          }
          return _default_target_produce_tp_rate();
      });
}

std::optional<uint64_t>
client_quota_translator::get_client_target_fetch_tp_rate(
  const tracker_key& quota_id) {
    return ss::visit(
      quota_id,
      [this](const k_client_id&) -> std::optional<uint64_t> {
          return _default_target_fetch_tp_rate();
      },
      [this](const k_group_name& k) -> std::optional<uint64_t> {
          auto group = _target_fetch_tp_rate_per_client_group().find(k);
          if (group != _target_fetch_tp_rate_per_client_group().end()) {
              return group->second.quota;
          }
          return _default_target_fetch_tp_rate();
      });
}

namespace {
// If client is part of some group then client quota ID is a group
// else client quota ID is client_id
tracker_key get_client_quota_id(
  const std::optional<std::string_view>& client_id,
  const std::unordered_map<ss::sstring, config::client_group_quota>&
    group_quota) {
    if (!client_id) {
        // requests without a client id are grouped into an anonymous group that
        // shares a default quota. the anonymous group is keyed on empty string.
        return tracker_key{std::in_place_type<k_client_id>, ""};
    }
    for (const auto& group_and_limit : group_quota) {
        if (client_id->starts_with(
              std::string_view(group_and_limit.second.clients_prefix))) {
            return tracker_key{
              std::in_place_type<k_group_name>, group_and_limit.first};
        }
    }
    return tracker_key{std::in_place_type<k_client_id>, *client_id};
}

} // namespace

tracker_key client_quota_translator::get_produce_key(
  std::optional<std::string_view> client_id) {
    return get_client_quota_id(
      client_id, _target_produce_tp_rate_per_client_group());
}

tracker_key client_quota_translator::get_fetch_key(
  std::optional<std::string_view> client_id) {
    return get_client_quota_id(
      client_id, _target_fetch_tp_rate_per_client_group());
}

tracker_key client_quota_translator::get_partition_mutation_key(
  std::optional<std::string_view> client_id) {
    return get_client_quota_id(client_id, {});
}

tracker_key
client_quota_translator::find_quota_key(const client_quota_request_ctx& ctx) {
    switch (ctx.q_type) {
    case client_quota_type::produce_quota:
        return get_produce_key(ctx.client_id);
    case client_quota_type::fetch_quota:
        return get_fetch_key(ctx.client_id);
    case client_quota_type::partition_mutation_quota:
        return get_partition_mutation_key(ctx.client_id);
    };
}

std::pair<tracker_key, client_quota_limits>
client_quota_translator::find_quota(const client_quota_request_ctx& ctx) {
    auto key = find_quota_key(ctx);
    auto value = find_quota_value(key);
    return {std::move(key), value};
}

client_quota_limits
client_quota_translator::find_quota_value(const tracker_key& key) {
    return client_quota_limits{
      .produce_limit = get_client_target_produce_tp_rate(key),
      .fetch_limit = get_client_target_fetch_tp_rate(key),
      .partition_mutation_limit = _target_partition_mutation_quota(),
    };
}

void client_quota_translator::watch(on_change_fn&& fn) {
    auto watcher = [fn = std::move(fn)]() { fn(); };
    _target_produce_tp_rate_per_client_group.watch(watcher);
    _target_fetch_tp_rate_per_client_group.watch(watcher);
    _target_partition_mutation_quota.watch(watcher);
    _default_target_produce_tp_rate.watch(watcher);
    _default_target_fetch_tp_rate.watch(watcher);
}

} // namespace kafka
