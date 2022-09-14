/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/kafka_client_cache.h"

namespace pandaproxy {

kafka_client_cache::kafka_client_cache(
  YAML::Node const& cfg,
  size_t max_size,
  std::vector<config::broker_authn_endpoint> kafka_api,
  model::timestamp::type keep_alive)
  : _config{cfg}
  , _cache_max_size{max_size}
  , _kafka_has_sasl{false}
  , _keep_alive{keep_alive} {
    // Is there a Kafka listener with SASL enabled?
    auto ep_it = std::find_if(
      kafka_api.begin(),
      kafka_api.end(),
      [](const config::broker_authn_endpoint& ep) {
          auto method = ep.authn_method.value_or(
            config::broker_authn_method::none);
          return method == config::broker_authn_method::sasl;
      });

    _kafka_has_sasl = ep_it != kafka_api.end();
}

client_ptr kafka_client_cache::fetch(credential_t user) {
    auto& inner_map = _cache.get<underlying_map>();
    auto it_map = inner_map.find(user.name);

    // User not found
    if (it_map == inner_map.end()) {
        throw std::out_of_range(fmt::format("User {} not found", user.name));
    }

    // Otherwise user found. Move them to the beginning
    // of the frequency list.

    client_ptr client{it_map->client};
    auto& inner_list = _cache.get<underlying_list>();
    // Convert the map iterator to list iterator
    auto it_list = _cache.project<underlying_list>(it_map);
    inner_list.relocate(it_list, inner_list.begin());

    return client;
}

client_ptr kafka_client_cache::make_client(
  credential_t user, config::rest_authn_type authn_type) {
    // Set the principal when there is a Kafka listener with SASL
    // enabled and the incoming request is using HTTP Basic AuthN
    if (_kafka_has_sasl && authn_type == config::rest_authn_type::http_basic) {
        // Need to specify type or else bad any_cast runtime error
        _config.sasl_mechanism.set_value(ss::sstring{"SCRAM-SHA-256"});
        _config.scram_username.set_value(user.name);
        _config.scram_password.set_value(user.pass);
    }

    return ss::make_lw_shared<timestamped_client>(
      to_yaml(_config, config::redact_secrets::no), model::new_timestamp());
}

void kafka_client_cache::insert(credential_t user, client_ptr client) {
    auto& inner_list = _cache.get<underlying_list>();
    auto& inner_map = _cache.get<underlying_map>();

    // First remove the last used client if the
    // cache is full.
    if (_cache.size() >= _cache_max_size) {
        auto lru = inner_list.end();
        --lru; // Last item is back one step
        inner_map.erase(lru->username);
        inner_list.erase(lru);
    }

    user_client_pair pair{.username = user.name, .client = client};

    // Add the user-client pair to front of frequency
    // list since it will become recently "used"
    inner_list.push_front(pair);
    inner_map.insert(pair);
}

void kafka_client_cache::clean_stale_clients() {
    // absl::erase_if(_user_client_map, [this](const auto& item) {
    //     auto const& [key, it] = item;
    //     auto live = (model::new_timestamp()() - it->second->creation_time());

    //     if (live >= _keep_alive) {
    //         vlog(plog.debug, "Erased {} from map", key);
    //         return true;
    //     }
    //     return false;
    // });

    // std::erase_if(_user_client_list, [this](const user_client_pair& item) {
    //     auto live = (model::new_timestamp()() -
    //     item.second->creation_time());

    //     if (live >= _keep_alive) {
    //         vlog(plog.debug, "Erased {} from list", item.first);
    //         return true;
    //     }
    //     return false;
    // });
}

size_t kafka_client_cache::size() const { return _cache.size(); }

} // namespace pandaproxy
