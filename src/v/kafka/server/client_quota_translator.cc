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
#include "kafka/server/logger.h"

#include <seastar/core/shard_id.hh>
#include <seastar/util/variant_utils.hh>

#include <optional>

namespace kafka {

using cluster::client_quota::entity_key;
using cluster::client_quota::entity_value;

namespace {
template<typename Match>
std::optional<Match>
get_part(const absl::flat_hash_set<entity_key::part_t>& parts) {
    const auto it = std::ranges::find_if(parts, [](const auto& part) {
        return std::holds_alternative<Match>(part.part);
    });
    if (it == parts.end()) {
        return std::nullopt;
    }
    return std::make_optional(std::get<Match>(it->part));
};

template<typename K>
tracker_key make_tracker_key(const std::string_view k_name) {
    return tracker_key{std::in_place_type<K>, k_name};
}
template<typename K1, typename K2>
tracker_key make_tracker_key(
  const std::string_view k1_name, const std::string_view k2_name) {
    return tracker_key{
      std::in_place_type<std::pair<K1, K2>>, std::make_pair(k1_name, k2_name)};
}
} // namespace
std::ostream& operator<<(std::ostream& os, const tracker_key& k) {
    ss::visit(
      k,
      [&os](const std::pair<k_user, k_client_id>& p) mutable {
          fmt::print(
            os, "k_user{{{}}}, k_client_id{{{}}}", p.first(), p.second());
      },
      [&os](const std::pair<k_user, k_group_name>& p) mutable {
          fmt::print(
            os, "k_user{{{}}}, k_group_name{{{}}}", p.first(), p.second());
      },
      [&os](const k_user& u) mutable { fmt::print(os, "k_user{{{}}}", u()); },
      [&os](const k_client_id& c) mutable {
          fmt::print(os, "k_client_id{{{}}}", c());
      },
      [&os](const k_group_name& g) mutable {
          fmt::print(os, "k_group_name{{{}}}", g());
      },
      [&os](const k_not_applicable&) mutable {
          fmt::print(os, "k_not_applicable");
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
      os,
      "{{quota_type: {}, user: {}, client_id: {}}}",
      ctx.q_type,
      ctx.user,
      ctx.client_id);
    return os;
}

std::ostream& operator<<(std::ostream& os, client_quota_rule r) {
    switch (r) {
    case client_quota_rule::not_applicable:
        return os << "not_applicable";
    case client_quota_rule::kafka_client_default:
        return os << "kafka_client_default";
    case client_quota_rule::kafka_client_prefix:
        return os << "kafka_client_prefix";
    case client_quota_rule::kafka_client_id:
        return os << "kafka_client_id";
    case client_quota_rule::kafka_user_default:
        return os << "kafka_user_default";
    case client_quota_rule::kafka_user_default_client_default:
        return os << "kafka_user_default_client_default";
    case client_quota_rule::kafka_user_default_client_prefix:
        return os << "kafka_user_default_client_prefix";
    case client_quota_rule::kafka_user_default_client_id:
        return os << "kafka_user_default_client_id";
    case client_quota_rule::kafka_user:
        return os << "kafka_user";
    case client_quota_rule::kafka_user_client_default:
        return os << "kafka_user_client_default";
    case client_quota_rule::kafka_user_client_prefix:
        return os << "kafka_user_client_prefix";
    case client_quota_rule::kafka_user_client_id:
        return os << "kafka_user_client_id";
    }
}

std::ostream& operator<<(std::ostream& os, client_quota_value value) {
    fmt::print(os, "{{limit: {}, rule: {}}}", value.limit, value.rule);
    return os;
}

client_quota_translator::client_quota_translator(
  ss::sharded<cluster::client_quota::store>& quota_store)
  : _quota_store(quota_store) {}

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
    const auto get_quota =
      [&accessor,
       this](const entity_key& match_key) -> std::optional<uint64_t> {
        auto match_quota = _quota_store.local().get_quota(match_key);
        if (!match_quota.has_value()) {
            return std::nullopt;
        }
        return accessor(*match_quota);
    };

    return ss::visit(
      quota_id,
      [&get_quota](
        const std::pair<k_user, k_client_id>& p) -> client_quota_value {
          const auto& [u, c] = p;
          // Exact user exact client id
          {
              auto match_key = entity_key{
                entity_key::user_match{u}, entity_key::client_id_match{c}};
              auto quota = get_quota(match_key);
              if (quota) {
                  return client_quota_value{
                    quota, client_quota_rule::kafka_user_client_id};
              }
          }

          // Exact user default client id
          {
              auto match_key = entity_key{
                entity_key::user_match{u},
                entity_key::client_id_default_match{}};
              auto quota = get_quota(match_key);
              if (quota) {
                  return client_quota_value{
                    quota, client_quota_rule::kafka_user_client_default};
              }
          }

          // Default user exact client id
          {
              auto match_key = entity_key{
                entity_key::user_default_match{},
                entity_key::client_id_match{c}};
              auto quota = get_quota(match_key);
              if (quota) {
                  return client_quota_value{
                    quota, client_quota_rule::kafka_user_default_client_id};
              }
          }

          // Default user default client id
          {
              auto match_key = entity_key{
                entity_key::user_default_match{},
                entity_key::client_id_default_match{}};
              auto quota = get_quota(match_key);
              if (quota) {
                  return client_quota_value{
                    quota,
                    client_quota_rule::kafka_user_default_client_default};
              }
          }

          return client_quota_value{
            std::nullopt, client_quota_rule::not_applicable};
      },
      [&get_quota](
        const std::pair<k_user, k_group_name>& p) -> client_quota_value {
          const auto& [u, g] = p;
          // Exact user client group
          {
              auto match_key = entity_key{
                entity_key::user_match{u},
                entity_key::client_id_prefix_match{g}};
              auto quota = get_quota(match_key);
              if (quota) {
                  return client_quota_value{
                    quota, client_quota_rule::kafka_user_client_prefix};
              }
          }

          // Default user client group
          {
              auto match_key = entity_key{
                entity_key::user_default_match{},
                entity_key::client_id_prefix_match{g}};
              auto quota = get_quota(match_key);
              if (quota) {
                  return client_quota_value{
                    quota, client_quota_rule::kafka_user_default_client_prefix};
              }
          }

          return client_quota_value{
            std::nullopt, client_quota_rule::not_applicable};
      },
      [this, &accessor](const k_user& u) -> client_quota_value {
          auto exact_match_key = entity_key{entity_key::user_match{u}};
          auto exact_match_quota = _quota_store.local().get_quota(
            exact_match_key);
          if (exact_match_quota && accessor(*exact_match_quota)) {
              return client_quota_value{
                accessor(*exact_match_quota), client_quota_rule::kafka_user};
          }

          static const auto default_user_key = entity_key{
            entity_key::user_default_match{}};
          auto default_quota = _quota_store.local().get_quota(default_user_key);
          if (default_quota && accessor(*default_quota)) {
              return client_quota_value{
                accessor(*default_quota),
                client_quota_rule::kafka_user_default};
          }

          return client_quota_value{
            std::nullopt, client_quota_rule::not_applicable};
      },
      [&get_quota](const k_client_id& k) -> client_quota_value {
          auto match_key = entity_key{entity_key::client_id_match{k}};
          if (auto quota = get_quota(match_key); quota.has_value()) {
              return client_quota_value{
                quota, client_quota_rule::kafka_client_id};
          }

          static const auto default_client_key = entity_key{
            entity_key::client_id_default_match{}};
          if (auto quota = get_quota((default_client_key)); quota.has_value()) {
              return client_quota_value{
                quota, client_quota_rule::kafka_client_default};
          }

          return client_quota_value{
            std::nullopt, client_quota_rule::not_applicable};
      },
      [&get_quota](const k_group_name& k) -> client_quota_value {
          auto match_key = entity_key{entity_key::client_id_prefix_match{k}};
          if (auto quota = get_quota(match_key); quota.has_value()) {
              return client_quota_value{
                quota, client_quota_rule::kafka_client_prefix};
          }

          return client_quota_value{
            std::nullopt, client_quota_rule::not_applicable};
      },
      [](const k_not_applicable&) -> client_quota_value {
          return client_quota_value{
            std::nullopt, client_quota_rule::not_applicable};
      });
}

// If client is part of some group then client quota ID is a group
// else client quota ID is client_id
tracker_key client_quota_translator::find_quota_key(
  const client_quota_request_ctx& ctx) const {
    auto qt = ctx.q_type;
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

    // requests without a client id are grouped into an anonymous group that
    // shares a default quota. the anonymous group is keyed on empty string.
    std::string_view user = ctx.user.value_or("");
    std::string_view client_id = ctx.client_id.value_or("");

    const auto has_quota = [&checker,
                            quota_store](const entity_key& match_key) {
        auto match_quota = quota_store.get_quota(match_key);
        return match_quota && checker(*match_quota);
    };

    /// config/user/<user>/client-id/<client-id>
    {
        const entity_key key{
          entity_key::user_match{user}, entity_key::client_id_match{client_id}};
        if (has_quota(key)) {
            return make_tracker_key<k_user, k_client_id>(user, client_id);
        }
    }

    auto group_quotas = quota_store.range(
      cluster::client_quota::store::prefix_group_filter(client_id));

    /// config/user/<user>/client-id-prefix/<client-id-prefix>
    for (auto& [gk, gv] : group_quotas) {
        if (checker(gv)) {
            auto user_match = get_part<entity_key::part::user_match>(gk.parts);
            if (!user_match.has_value()) {
                continue;
            }

            if (user_match->value != user) {
                continue;
            }

            auto client_prefix
              = get_part<entity_key::part::client_id_prefix_match>(gk.parts);
            if (!client_prefix.has_value()) {
                continue;
            }

            return make_tracker_key<k_user, k_group_name>(
              user, client_prefix->value);
        }
    }

    /// config/user/<user>/client-id/<default>
    {
        const entity_key key{
          entity_key::user_match{user}, entity_key::client_id_default_match{}};
        if (has_quota(key)) {
            return make_tracker_key<k_user, k_client_id>(user, client_id);
        }
    }

    /// config/user/<user>
    {
        const entity_key key{entity_key::user_match{user}};
        if (has_quota(key)) {
            return make_tracker_key<k_user>(user);
        }
    }

    /// config/user/<default>/client-id/<client-id>
    {
        const entity_key key{
          entity_key::user_default_match{},
          entity_key::client_id_match{client_id}};
        if (has_quota(key)) {
            return make_tracker_key<k_user, k_client_id>(user, client_id);
        }
    }

    /// config/user/<default>/client-id-prefix/<client-id-prefix>
    for (auto& [gk, gv] : group_quotas) {
        if (checker(gv)) {
            auto default_user_match
              = get_part<entity_key::part::user_default_match>(gk.parts);
            if (!default_user_match.has_value()) {
                continue;
            }

            auto client_prefix_match
              = get_part<entity_key::part::client_id_prefix_match>(gk.parts);
            if (!client_prefix_match.has_value()) {
                continue;
            }

            return make_tracker_key<k_user, k_group_name>(
              user, client_prefix_match->value);
        }
    }

    /// config/user/<default>/client-id/<default>
    {
        const entity_key key{
          entity_key::user_default_match{},
          entity_key::client_id_default_match{}};
        if (has_quota(key)) {
            return make_tracker_key<k_user, k_client_id>(user, client_id);
        }
    }

    /// config/user/<default>
    {
        const entity_key key{entity_key::user_default_match{}};
        if (has_quota(key)) {
            return make_tracker_key<k_user>(user);
        }
    }

    /// config/client-id/<client-id>
    {
        const entity_key key{entity_key::client_id_match{client_id}};
        if (has_quota(key)) {
            return make_tracker_key<k_client_id>(client_id);
        }
    }

    // Group quotas configured through the Kafka API
    /// config/client-id-prefix/<client-id-prefix>
    for (auto& [gk, gv] : group_quotas) {
        if (checker(gv)) {
            auto client_prefix_match
              = get_part<entity_key::part::client_id_prefix_match>(gk.parts);
            if (!client_prefix_match.has_value()) {
                continue;
            }

            return make_tracker_key<k_group_name>(client_prefix_match->value);
        }
    }

    // Default quotas configured through the Kafka API
    /// config/client-id/<default>
    {
        const entity_key key{entity_key::client_id_default_match{}};
        if (has_quota(key)) {
            return make_tracker_key<k_client_id>(client_id);
        }
    }

    // No relevant quotas where found
    return tracker_key{std::in_place_type<k_not_applicable>};
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
    _quota_store.local().watch(watcher);
}

bool client_quota_translator::is_empty() const {
    return _quota_store.local().size() == 0;
}

} // namespace kafka
