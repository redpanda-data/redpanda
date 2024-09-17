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

#include "cluster/client_quota_store.h"
#include "config/configuration.h"
#include "kafka/server/logger.h"

#include <seastar/core/shard_id.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/algorithm/container.h>

#include <optional>

namespace kafka {

using cluster::client_quota::entity_key;
using cluster::client_quota::entity_value;

std::ostream& operator<<(std::ostream& os, const tracker_key& k) {
    ss::visit(
      k,
      [&os](const k_client_id& c) mutable {
          fmt::print(os, "k_client_id{{{}}}", c());
      },
      [&os](const k_group_name& g) mutable {
          fmt::print(os, "k_group_name{{{}}}", g());
      });
    return os;
}

std::ostream& operator<<(std::ostream& os, client_quota_type quota_type) {
    switch (quota_type) {
    case client_quota_type::produce_quota:
        return os << "produce_quota";
    case client_quota_type::fetch_quota:
        return os << "fetch_quota";
    case client_quota_type::partition_mutation_quota:
        return os << "partition_mutation_quota";
    }
}

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

std::ostream&
operator<<(std::ostream& os, const client_quota_request_ctx& ctx) {
    fmt::print(
      os, "{{quota_type: {}, client_id: {}}}", ctx.q_type, ctx.client_id);
    return os;
}

std::ostream& operator<<(std::ostream& os, client_quota_rule r) {
    switch (r) {
    case client_quota_rule::not_applicable:
        return os << "not_applicable";
    case client_quota_rule::cluster_client_default:
        return os << "cluster_client_default";
    case client_quota_rule::kafka_client_default:
        return os << "kafka_client_default";
    case client_quota_rule::cluster_client_prefix:
        return os << "cluster_client_prefix";
    case client_quota_rule::kafka_client_prefix:
        return os << "kafka_client_prefix";
    case client_quota_rule::kafka_client_id:
        return os << "kafka_client_id";
    }
}

std::ostream& operator<<(std::ostream& os, client_quota_value value) {
    fmt::print(os, "{{limit: {}, rule: {}}}", value.limit, value.rule);
    return os;
}

client_quota_translator::client_quota_translator(
  ss::sharded<cluster::client_quota::store>& quota_store)
  : _quota_store(quota_store)
  , _default_target_produce_tp_rate(
      config::shard_local_cfg().target_quota_byte_rate.bind())
  , _default_target_fetch_tp_rate(
      config::shard_local_cfg().target_fetch_quota_byte_rate.bind())
  , _target_partition_mutation_quota(
      config::shard_local_cfg().kafka_admin_topic_api_rate.bind())
  , _target_produce_tp_rate_per_client_group(
      config::shard_local_cfg().kafka_client_group_byte_rate_quota.bind())
  , _target_fetch_tp_rate_per_client_group(
      config::shard_local_cfg()
        .kafka_client_group_fetch_byte_rate_quota.bind()) {
    if (ss::this_shard_id() == 0) {
        maybe_log_deprecated_configs_nag();

        _config_callbacks.emplace_back(
          [this]() { maybe_log_deprecated_configs_nag(); });
    }

    // Each config binding only supports a single watch() callback, so we
    // create a vector of callbacks to execute whenever they update
    auto call_config_callbacks = [this]() {
        for (auto& f : _config_callbacks) {
            f();
        }
    };
    _target_produce_tp_rate_per_client_group.watch(call_config_callbacks);
    _target_fetch_tp_rate_per_client_group.watch(call_config_callbacks);
    _target_partition_mutation_quota.watch(call_config_callbacks);
    _default_target_produce_tp_rate.watch(call_config_callbacks);
    _default_target_fetch_tp_rate.watch(call_config_callbacks);
}

client_quota_value client_quota_translator::get_client_quota_value(
  const tracker_key& quota_id, client_quota_type qt) const {
    const auto accessor = [qt](const cluster::client_quota::entity_value& ev) {
        switch (qt) {
        case client_quota_type::produce_quota:
            return ev.producer_byte_rate;
        case client_quota_type::fetch_quota:
            return ev.consumer_byte_rate;
        case client_quota_type::partition_mutation_quota:
            return ev.controller_mutation_rate;
        }
    };
    return ss::visit(
      quota_id,
      [this, qt, &accessor](const k_client_id& k) -> client_quota_value {
          auto exact_match_key = entity_key{entity_key::client_id_match{k}};
          auto exact_match_quota = _quota_store.local().get_quota(
            exact_match_key);
          if (exact_match_quota && accessor(*exact_match_quota)) {
              return client_quota_value{
                accessor(*exact_match_quota),
                client_quota_rule::kafka_client_id};
          }

          static const auto default_client_key = entity_key{
            entity_key::client_id_default_match{}};
          auto default_quota = _quota_store.local().get_quota(
            default_client_key);
          if (default_quota && accessor(*default_quota)) {
              return client_quota_value{
                accessor(*default_quota),
                client_quota_rule::kafka_client_default};
          }

          auto default_config = get_default_config(qt);
          if (default_config) {
              return client_quota_value{
                *default_config * ss::smp::count,
                client_quota_rule::cluster_client_default};
          }
          return client_quota_value{
            std::nullopt, client_quota_rule::not_applicable};
      },
      [this, qt, &accessor](const k_group_name& k) -> client_quota_value {
          const auto& group_quota_config = get_quota_config(qt);
          auto group_key = entity_key{entity_key::client_id_prefix_match{k}};
          auto group_quota = _quota_store.local().get_quota(group_key);
          if (group_quota && accessor(*group_quota)) {
              return client_quota_value{
                accessor(*group_quota), client_quota_rule::kafka_client_prefix};
          }

          auto group = group_quota_config.find(k);
          if (group != group_quota_config.end()) {
              return client_quota_value{
                static_cast<uint64_t>(group->second.quota * ss::smp::count),
                client_quota_rule::cluster_client_prefix};
          }

          return client_quota_value{
            std::nullopt, client_quota_rule::not_applicable};
      });
}

// If client is part of some group then client quota ID is a group
// else client quota ID is client_id
tracker_key client_quota_translator::find_quota_key(
  const client_quota_request_ctx& ctx) const {
    auto qt = ctx.q_type;
    const auto& client_id = ctx.client_id;
    const auto& group_quota = get_quota_config(qt);
    const auto& quota_store = _quota_store.local();

    const auto checker = [qt](const entity_value val) {
        switch (qt) {
        case kafka::client_quota_type::produce_quota:
            return val.producer_byte_rate.has_value();
        case kafka::client_quota_type::fetch_quota:
            return val.consumer_byte_rate.has_value();
        case kafka::client_quota_type::partition_mutation_quota:
            return val.controller_mutation_rate.has_value();
        }
    };

    if (!client_id) {
        // requests without a client id are grouped into an anonymous group that
        // shares a default quota. the anonymous group is keyed on empty string.
        return tracker_key{std::in_place_type<k_client_id>, ""};
    }

    // Exact match quotas
    auto exact_match_key = entity_key{entity_key::client_id_match{*client_id}};
    auto exact_match_quota = quota_store.get_quota(exact_match_key);
    if (exact_match_quota && checker(*exact_match_quota)) {
        return tracker_key{std::in_place_type<k_client_id>, *client_id};
    }

    // Group quotas configured through the Kafka API
    auto group_quotas = quota_store.range(
      cluster::client_quota::store::prefix_group_filter(*client_id));
    for (auto& [gk, gv] : group_quotas) {
        if (checker(gv)) {
            for (auto& part : gk.parts) {
                using client_id_prefix_match
                  = entity_key::part::client_id_prefix_match;

                if (std::holds_alternative<client_id_prefix_match>(part.part)) {
                    auto& prefix_key_part = get<client_id_prefix_match>(
                      part.part);
                    return tracker_key{
                      std::in_place_type<k_group_name>, prefix_key_part.value};
                }
            }
        }
    }

    // Group quotas configured through cluster configs
    for (const auto& group_and_limit : group_quota) {
        if (client_id->starts_with(
              std::string_view(group_and_limit.second.clients_prefix))) {
            return tracker_key{
              std::in_place_type<k_group_name>, group_and_limit.first};
        }
    }

    // Default quotas configured through either the Kafka API or cluster configs
    return tracker_key{std::in_place_type<k_client_id>, *client_id};
}

std::pair<tracker_key, client_quota_value>
client_quota_translator::find_quota(const client_quota_request_ctx& ctx) const {
    auto key = find_quota_key(ctx);
    auto value = get_client_quota_value(key, ctx.q_type);
    return {std::move(key), value};
}

client_quota_limits
client_quota_translator::find_quota_value(const tracker_key& key) const {
    return client_quota_limits{
      .produce_limit
      = get_client_quota_value(key, client_quota_type::produce_quota).limit,
      .fetch_limit
      = get_client_quota_value(key, client_quota_type::fetch_quota).limit,
      .partition_mutation_limit
      = get_client_quota_value(key, client_quota_type::partition_mutation_quota)
          .limit,
    };
}

void client_quota_translator::watch(on_change_fn&& fn) {
    auto watcher = [fn = std::move(fn)]() { fn(); };
    _config_callbacks.emplace_back(watcher);
    _quota_store.local().watch(watcher);
}

const client_quota_translator::quota_config&
client_quota_translator::get_quota_config(client_quota_type qt) const {
    static const quota_config empty;
    switch (qt) {
    case kafka::client_quota_type::produce_quota:
        return _target_produce_tp_rate_per_client_group();
    case kafka::client_quota_type::fetch_quota:
        return _target_fetch_tp_rate_per_client_group();
    case kafka::client_quota_type::partition_mutation_quota:
        return empty;
    }
}

std::optional<uint64_t>
client_quota_translator::get_default_config(client_quota_type qt) const {
    switch (qt) {
    case kafka::client_quota_type::produce_quota: {
        auto produce_quota = _default_target_produce_tp_rate();
        return (produce_quota > 0) ? std::make_optional<uint64_t>(produce_quota)
                                   : std::nullopt;
    }
    case kafka::client_quota_type::fetch_quota:
        return _default_target_fetch_tp_rate();
    case kafka::client_quota_type::partition_mutation_quota:
        return _target_partition_mutation_quota();
    }
}

void client_quota_translator::maybe_log_deprecated_configs_nag() const {
    auto emit_nag
      = _default_target_produce_tp_rate()
          != config::configuration::target_produce_quota_byte_rate_default
        || _default_target_fetch_tp_rate() || _target_partition_mutation_quota()
        || !_target_produce_tp_rate_per_client_group().empty()
        || !_target_fetch_tp_rate_per_client_group().empty();

    if (emit_nag) {
        vlog(
          client_quota_log.warn,
          "You have configured client quotas using cluster configs. The "
          "behaviour of these cluster configs changed in v24.2 to throttle "
          "node-wide instead of per-shard. Furthermore, this mode of "
          "configuring quotas is going to be deprecated in v25.1. You should "
          "use `rpk cluster quotas` to recreate your client quotas and remove "
          "the cluster configs. You can find more information about this "
          "change in the Redpanda Docs page. Deprecated configs: "
          "target_quota_byte_rate, target_fetch_quota_byte_rate, "
          "kafka_admin_topic_api_rate, kafka_client_group_byte_rate_quota and "
          "kafka_client_group_fetch_byte_rate_quota.");
    }
}

bool client_quota_translator::is_empty() const {
    return _quota_store.local().size() == 0
           && absl::c_all_of(all_client_quota_types, [this](auto qt) {
                  return !get_default_config(qt);
              });
}

} // namespace kafka
