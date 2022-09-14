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

#include "pandaproxy/sharded_client_cache.h"

#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "pandaproxy/kafka_client_cache.h"

namespace pandaproxy {

ss::shard_id user_shard(const ss::sstring& name) {
    auto hash = xxhash_64(name.data(), name.length());
    return jump_consistent_hash(hash, ss::smp::count);
}

ss::future<> sharded_client_cache::start(
  ss::smp_service_group sg,
  YAML::Node const& cfg,
  size_t size,
  std::vector<config::broker_authn_endpoint> kafka_api,
  model::timestamp::type keep_alive) {
    _smp_opts = ss::smp_submit_to_options{sg};
    return _cache.start(cfg, size, std::move(kafka_api), keep_alive);
}

ss::future<> sharded_client_cache::stop() { return _cache.stop(); }

ss::future<client_ptr> sharded_client_cache::fetch_client(
  credential_t user, config::rest_authn_type authn_type) {
    // Access the cache on the appropriate
    // shard please.
    auto u_shard{user_shard(user.name)};
    return _cache.invoke_on(
      u_shard, _smp_opts, [authn_type, &user](kafka_client_cache& cache) {
          // First clean any stale clients
          cache.clean_stale_clients();

          client_ptr client{nullptr};
          try {
              // Return the client if the user already
              // has one
              client = cache.fetch(user);
              vlog(plog.debug, "Reuse client for user {}", user.name);
              return ss::make_ready_future<client_ptr>(client);
          } catch (std::out_of_range& ex) {
              // Otherwise create a client for the user,
              // insert it to the cache, then return.
              vlog(plog.debug, "Make client for user {}", user.name);
              client = cache.make_client(user, authn_type);
              cache.insert(user, client);
              return ss::make_ready_future<client_ptr>(client);
          }
      });
}

} // namespace pandaproxy
