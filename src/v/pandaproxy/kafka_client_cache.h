/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "kafka/client/client.h"
#include "kafka/protocol/errors.h"
#include "model/timestamp.h"
#include "pandaproxy/logger.h"
#include "security/credential_store.h"
#include "ssx/future-util.h"

#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>
#include <fmt/format.h>

#include <chrono>
#include <list>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace pandaproxy {

namespace {
struct timestamped_client {
    kafka::client::client real;
    model::timestamp creation_time;

    timestamped_client(YAML::Node const& cfg, model::timestamp t)
      : real{cfg}
      , creation_time{t} {}
};
using client_ptr = ss::lw_shared_ptr<timestamped_client>;

// First is username, Second is password
using credential_t = std::pair<ss::sstring, ss::sstring>;

ss::shard_id user_shard(const ss::sstring& name) {
    auto hash = xxhash_64(name.data(), name.length());
    return jump_consistent_hash(hash, ss::smp::count);
}

} // namespace

// A LRU cache implemented with a doubly-linked list and an
// unordered map. The list tracks frequency where the most
// recently used client is at the front. When the cache is full
// remove the client from the end of the list. The map is for
// constant time look-ups of the kafka clients.
class kafka_client_cache {
public:
    using user_client_pair = std::pair<ss::sstring, client_ptr>;
    using user_client_list_it = std::list<user_client_pair>::iterator;
    using user_client_map
      = absl::flat_hash_map<ss::sstring, user_client_list_it>;

    kafka_client_cache(
      YAML::Node const& cfg,
      size_t size,
      model::timestamp::type keep_alive = 30)
      : _config{cfg}
      , _cache_size{size}
      , _keep_alive{keep_alive} {}

    ~kafka_client_cache() = default;

    client_ptr fetch(credential_t user) {
        auto it = _user_client_map.find(user.first);

        // User not found
        if (it == _user_client_map.end()) {
            throw std::out_of_range(
              fmt::format("User {} not found", user.first));
        }

        // Otherwise user found. Move it to the begginng
        // of the frequency list. Splice will shift the items
        // over.
        _user_client_list.splice(
          _user_client_list.begin(), _user_client_list, it->second);

        // The client is at the second of the iterator.
        return it->second->second;
    }

    client_ptr make_client() {
        // TODO(@NyaliaLui): Add SASL/SCRAM creds by incorporating
        // the user credentials
        return ss::make_lw_shared<timestamped_client>(
          to_yaml(_config, config::redact_secrets::no), model::new_timestamp());
    }

    void insert(credential_t user, client_ptr client) {
        // First remove the last used client if the
        // cache is full.
        if (_user_client_map.size() > _cache_size) {
            auto lru = _user_client_list.end();
            --lru; // Last item is back one step
            _user_client_map.erase(lru->first);
            _user_client_list.pop_back();
        }

        // See if the user already has a client.
        // If yes, remove that client from our containers
        // because we're about to insert a new one.
        auto it = _user_client_map.find(user.first);
        if (it != _user_client_map.end()) {
            _user_client_list.erase(it->second);
            _user_client_map.erase(it);
        }

        // Now we can insert the client
        // Add the user-client pair to front of frequency
        // list since it will become recently "used"
        _user_client_list.push_front(user_client_pair{user.first, client});

        // Add the user-client iterator to the map so we
        // can do a constant time lookup later. The iterator
        // is already at the front of the list.
        _user_client_map[user.first] = _user_client_list.begin();
    }

    size_t cache_size() const { return _cache_size; }
    model::timestamp::type keep_alive_time() const { return _keep_alive; }
    std::list<user_client_pair>& get_list() { return _user_client_list; }
    user_client_map& get_map() { return _user_client_map; }

private:
    kafka::client::configuration _config;
    size_t _cache_size;
    model::timestamp::type _keep_alive;
    std::list<user_client_pair> _user_client_list;
    user_client_map _user_client_map;
};

class sharded_client_cache {
public:
    ss::future<>
    start(ss::smp_service_group sg, YAML::Node const& cfg, size_t size) {
        _smp_opts = ss::smp_submit_to_options{sg};
        return _cache.start(cfg, size);
    }

    ss::future<> stop() { return _cache.stop(); }

    ss::future<client_ptr> fetch_client(credential_t user) {
        // Access the cache on the appropriate
        // shard please.
        auto u_shard{user_shard(user.first)};
        return _cache.invoke_on(
          u_shard, _smp_opts, [&user](kafka_client_cache& cache) {
              client_ptr client{nullptr};
              try {
                  // Return the client if the user already
                  // has one
                  client = cache.fetch(user);
                  vlog(plog.debug, "Reuse client for user {}", user.first);
                  return ss::make_ready_future<client_ptr>(client);
              } catch (std::out_of_range& ex) {
                  // Otherwise create a client for the user,
                  // insert it to the cache, then return.
                  vlog(plog.debug, "Make client for user {}", user.first);
                  client = cache.make_client(user);
                  cache.insert(user, client);
                  return ss::make_ready_future<client_ptr>(client);
              }
          });
    }

private:
    ss::smp_submit_to_options _smp_opts;
    ss::sharded<kafka_client_cache> _cache;
    ss::gate _gate;
};
} // namespace pandaproxy
