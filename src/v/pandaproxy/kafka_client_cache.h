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
#include "pandaproxy/logger.h"
#include "security/credential_store.h"
#include "utils/mutex.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>

#include <chrono>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace pandaproxy {

namespace {
// First is username, Second is password
using credential_t = std::pair<ss::sstring, ss::sstring>;

ss::shard_id user_shard(const ss::sstring& name) {
    auto hash = xxhash_64(name.data(), name.length());
    return jump_consistent_hash(hash, ss::smp::count);
}

} // namespace

class kafka_client_cache {
public:
    using client_ptr = ss::lw_shared_ptr<kafka::client::client>;
    using client_map = absl::flat_hash_map<ss::sstring, client_ptr>;

    kafka_client_cache(YAML::Node const& cfg, int32_t size)
      : _config{cfg}
      , _cache_size{size} {}

    ~kafka_client_cache() = default;

    client_map::iterator find(credential_t user) {
        return _client_map.find(user.first);
    }

    ss::future<client_ptr> make_client(credential_t user) {
        client_ptr client = ss::make_lw_shared<kafka::client::client>(
          to_yaml(_config, config::redact_secrets::no));
        _client_map[user.first] = client;
        return ss::make_ready_future<client_ptr>(client);
    }

    client_map::iterator end() { return _client_map.end(); }

    int32_t cache_size() const { return _cache_size; }

private:
    kafka::client::configuration _config;
    int32_t _cache_size;
    client_map _client_map;
};

class sharded_client_cache {
public:
    using client_ptr = kafka_client_cache::client_ptr;
    using client_map = kafka_client_cache::client_map;

    ss::future<>
    start(ss::smp_service_group sg, YAML::Node const& cfg, int32_t size) {
        _smp_opts = ss::smp_submit_to_options{sg};
        return _cache.start(cfg, size);
    }

    ss::future<> stop() { return _cache.stop(); }

    ss::future<client_ptr> find_or_create(credential_t user) {
        // Access the cache on the appropriate
        // shard please.
        auto u_shard{user_shard(user.first)};
        return _cache.invoke_on(
          u_shard, _smp_opts, [&user](kafka_client_cache& cache) {
              auto search = cache.find(user);

              // Return the client if the user already
              // has one
              if (search != cache.end()) {
                  vlog(plog.debug, "Reuse client for user {}", user.first);
                  return ss::make_ready_future<client_ptr>(search->second);
              } else {
                  // Otherwise create a client for the user.
                  // The client is automatically stored in
                  // the cache.
                  vlog(plog.debug, "Make client for user {}", user.first);
                  return cache.make_client(user);
              }
          });
    }

private:
    ss::smp_submit_to_options _smp_opts;
    ss::sharded<kafka_client_cache> _cache;
};
} // namespace pandaproxy
