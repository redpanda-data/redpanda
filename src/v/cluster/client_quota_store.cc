// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "client_quota_store.h"

#include "client_quota_serde.h"

#include <type_traits>

namespace cluster::client_quota {

namespace {
template<typename T>
bool has_part(const entity_key& key) {
    return std::ranges::any_of(key.parts, [](const auto& part) {
        return std::holds_alternative<T>(part.part);
    });
};

rule get_rule(const entity_key& key) {
    const bool has_user = has_part<entity_key::part::user_match>(key);
    const bool has_user_default
      = has_part<entity_key::part::user_default_match>(key);
    const bool has_client_id = has_part<entity_key::part::client_id_match>(key);
    const bool has_client_prefix
      = has_part<entity_key::part::client_id_prefix_match>(key);
    const bool has_client_default
      = has_part<entity_key::part::client_id_default_match>(key);

    if (has_user) {
        if (has_client_id) {
            return rule::kafka_user_client_id;
        }
        if (has_client_prefix) {
            return rule::kafka_user_client_prefix;
        }
        if (has_client_default) {
            return rule::kafka_user_client_default;
        }
        return rule::kafka_user;
    }

    if (has_user_default) {
        if (has_client_id) {
            return rule::kafka_user_default_client_id;
        }
        if (has_client_prefix) {
            return rule::kafka_user_default_client_prefix;
        }
        if (has_client_default) {
            return rule::kafka_user_default_client_default;
        }
        return rule::kafka_user_default;
    }

    if (has_client_id) {
        return rule::kafka_client_id;
    }
    if (has_client_prefix) {
        return rule::kafka_client_prefix;
    }
    if (has_client_default) {
        return rule::kafka_client_default;
    }

    return rule::not_applicable;
}
} // namespace

void store::set_quota(
  const entity_key& key, const entity_value& value, bool trigger_notify) {
    if (!value.is_empty()) {
        const auto [_, inserted] = _quotas.insert_or_assign(key, value);
        if (inserted) {
            ++_rules_counters[static_cast<std::underlying_type_t<rule>>(
              get_rule(key))];
        }
    } else {
        auto n_erased = _quotas.erase(key);
        if (n_erased != 0) {
            --_rules_counters[static_cast<std::underlying_type_t<rule>>(
              get_rule(key))];
        }
    }

    if (trigger_notify) {
        notify_watchers();
    }
}

void store::remove_quota(const entity_key& key) {
    auto n_erased = _quotas.erase(key);
    if (n_erased != 0) {
        --_rules_counters[static_cast<std::underlying_type_t<rule>>(
          get_rule(key))];
    }
    notify_watchers();
}

std::optional<entity_value> store::get_quota(const entity_key& key) const {
    auto it = _quotas.find(key);
    if (it != _quotas.end()) {
        return it->second;
    }
    return std::nullopt;
}

store::range_container_type store::range(
  std::function<bool(const std::pair<entity_key, entity_value>&)>&& pred)
  const {
    range_container_type result;
    std::copy_if(
      _quotas.cbegin(), _quotas.cend(), std::back_inserter(result), pred);
    return result;
}

store::container_type::size_type store::size() const { return _quotas.size(); }

void store::clear() {
    _quotas.clear();
    notify_watchers();

    for (size_t& counter : _rules_counters) {
        counter = 0;
    }
}

const store::container_type& store::all_quotas() const { return _quotas; }

void store::apply_delta(const alter_delta_cmd_data& data) {
    for (auto& [key, value] : data.ops) {
        auto it = _quotas.find(key);
        entity_value q = it == _quotas.end() ? entity_value{} : it->second;
        for (const auto& entry : value.entries) {
            auto& entity = [&]() -> auto& {
                switch (entry.type) {
                case entity_value_diff::key::producer_byte_rate:
                    return q.producer_byte_rate;
                case entity_value_diff::key::consumer_byte_rate:
                    return q.consumer_byte_rate;
                case entity_value_diff::key::controller_mutation_rate:
                    return q.controller_mutation_rate;
                }
            }();
            switch (entry.op) {
            case entity_value_diff::operation::remove:
                entity.reset();
                break;
            case entity_value_diff::operation::upsert:
                entity = entry.value;
                break;
            }
        }
        set_quota(key, q, false);
    }
    _quotas.rehash(0);
    notify_watchers();
}

void store::watch(on_change_callback_type&& f) {
    _on_change_watchers.push_back(std::move(f));
}

void store::notify_watchers() const {
    for (auto& f : _on_change_watchers) {
        f();
    }
}

} // namespace cluster::client_quota
