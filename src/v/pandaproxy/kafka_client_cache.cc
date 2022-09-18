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

#include "pandaproxy/client_cache_error.h"
#include "pandaproxy/logger.h"

#include <chrono>

using namespace std::chrono_literals;

namespace pandaproxy {

static constexpr auto clean_timer_period = 10s;

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
          return ep.authn_method == config::broker_authn_method::sasl;
      });

    _kafka_has_sasl = ep_it != kafka_api.end();

    _clean_timer.set_callback([this] {
        // Do not need a gate since there are limited items
        // in the cache. Therefore, relative to other design,
        // garbage collection is inexpensive here.
        clean_stale_clients();
    });
    _clean_timer.arm_periodic(clean_timer_period);
}

kafka_client_cache::~kafka_client_cache() {
    _clean_timer.cancel();
}

client_ptr kafka_client_cache::fetch(credential_t user) {
    auto& inner_hash = _cache.get<underlying_hash>();
    auto it_hash = inner_hash.find(user.name);

    if (it_hash == inner_hash.end()) {
        return nullptr;
    }

    // Otherwise user-client found.

    // Convert the hash iterator to list iterator
    auto it_list = _cache.project<underlying_list>(it_hash);
    auto& inner_list = _cache.get<underlying_list>();

    // Extract the item. This is OK because elements are
    // not copied or moved. Instead internal pointers
    // are simply re-balanced and no allocations are made.
    auto node = inner_list.extract(it_list);
    
    // Update the timestamp and put it to the front
    node.value().client->last_used = model::timestamp::now();
    inner_list.push_front(node.value());

    return node.value().client;
}

client_ptr kafka_client_cache::make_client(
  credential_t user, config::rest_authn_type authn_type) {
    kafka::client::configuration cfg{to_yaml(_config, config::redact_secrets::no)};
    
    // Set the principal when there is a Kafka listener with SASL
    // enabled and the incoming request is using HTTP Basic AuthN
    if (_kafka_has_sasl && authn_type == config::rest_authn_type::http_basic) {
        // Need to specify type or else bad any_cast runtime error
        cfg.sasl_mechanism.set_value(ss::sstring{"SCRAM-SHA-256"});
        cfg.scram_username.set_value(user.name);
        cfg.scram_password.set_value(user.pass);
    }

    return ss::make_lw_shared<timestamped_client>(to_yaml(cfg, config::redact_secrets::no), model::new_timestamp());
}

void kafka_client_cache::insert(credential_t user, client_ptr client) {
    // Warn the user and do nothing if the 
    // max size is 0
    if (_cache_max_size == 0) {
        throw client_cache_error("Failed to insert client. Max cache size is 0.");
    }

    auto& inner_list = _cache.get<underlying_list>();

    // First remove the last used client if the
    // cache is full.
    if (_cache.size() >= _cache_max_size) {
        if (_cache.size() != 0) {
            inner_list.pop_back();
        }
    }

    user_client_pair pair{.username = user.name, .client = client};

    // Add the user-client pair to front of frequency
    // list since it will become recently "used"
    // NOTE: push_front will silently fail if a duplicate
    // already exists.
    inner_list.push_front(pair);
}

client_ptr kafka_client_cache::fetch_or_insert(credential_t user, config::rest_authn_type authn_type) {
  client_ptr client{fetch(user)};

  // Make a client and insert
  // if the fetch failed
  if (client == client_ptr{nullptr}) {
    client = make_client(user, authn_type);
    insert(user, client);
    vlog(plog.debug, "Made client for user {}", user.name);
  } else {
    vlog(plog.debug, "Reuse client for user {}", user.name);
  }

  return client;
}

void kafka_client_cache::clean_stale_clients() {
    auto& inner_list = _cache.get<underlying_list>();
    inner_list.remove_if([this](const user_client_pair& item) {
        auto live = (model::new_timestamp()() - item.client->last_used());

        if (live >= _keep_alive) {
            vlog(plog.debug, "Removed {} from cache", item.username);
            return true;
        }
        return false;  
    });
}

size_t kafka_client_cache::size() const { return _cache.size(); }
size_t kafka_client_cache::max_size() const { return _cache_max_size; }
std::chrono::milliseconds kafka_client_cache::clean_timer() { return clean_timer_period; }

} // namespace pandaproxy
