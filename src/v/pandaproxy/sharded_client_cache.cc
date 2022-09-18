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
#include "ssx/future-util.h"

namespace pandaproxy {

ss::shard_id user_shard(const ss::sstring& name) {
    auto hash = xxhash_64(name.data(), name.length());
    return jump_consistent_hash(hash, ss::smp::count);
}

ss::future<> sharded_client_cache::start(
  ss::smp_service_group sg,
  YAML::Node const& cfg,
  size_t max_size,
  std::vector<config::broker_authn_endpoint> kafka_api,
  model::timestamp::type keep_alive) {
    _smp_opts = ss::smp_submit_to_options{sg};
    return _cache.start(cfg, max_size, std::move(kafka_api), keep_alive);
}

ss::future<> sharded_client_cache::stop() { 
  return _gate.close().then([this] {
     return _cache.stop();
  });
}

ss::future<client_ptr> sharded_client_cache::fetch_client(
  credential_t user, config::rest_authn_type authn_type) {

    return ss::with_gate(_gate, [this, user, authn_type] {
      // Access the cache on the appropriate
      // shard please.
      auto u_shard{user_shard(user.name)};
      return _cache.invoke_on(u_shard, _smp_opts, [user, authn_type](kafka_client_cache& cache) {
          client_ptr client{cache.fetch_or_insert(user, authn_type)};
          return ss::make_ready_future<client_ptr>(client);
      });
    });
}

} // namespace pandaproxy
