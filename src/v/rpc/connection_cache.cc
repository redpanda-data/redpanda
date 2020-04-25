#include "rpc/connection_cache.h"

#include "rpc/backoff_policy.h"

#include <fmt/format.h>

#include <chrono>

namespace rpc {

/// \brief needs to be a future, because mutations may come from different
/// fibers and they need to be synchronized
ss::future<> connection_cache::emplace(
  model::node_id n,
  rpc::transport_configuration c,
  backoff_policy backoff_policy) {
    if (auto s = shard_for(n); s != ss::this_shard_id()) {
        throw std::runtime_error(fmt::format(
          "Cannot ::emplace, node:{}, belonging to shard:{}, on shard:{}",
          n,
          s,
          ss::this_shard_id()));
    }
    return with_semaphore(
      _sem,
      1,
      [this,
       n,
       c = std::move(c),
       backoff_policy = std::move(backoff_policy)]() mutable {
          _cache.emplace(
            std::move(n),
            ss::make_lw_shared<rpc::reconnect_transport>(
              std::move(c), std::move(backoff_policy)));
      });
}
ss::future<> connection_cache::remove(model::node_id n) {
    if (auto s = shard_for(n); s != ss::this_shard_id()) {
        throw std::runtime_error(fmt::format(
          "Cannot ::remove, node:{}, belonging to shard:{}, on shard:{}",
          n,
          s,
          ss::this_shard_id()));
    }
    return with_semaphore(
      _sem, 1, [this, n = std::move(n)] { _cache.erase(n); });
}

/// \brief closes all client connections
ss::future<> connection_cache::stop() {
    return parallel_for_each(_cache, [](auto& it) {
        auto& [_, cli] = it;
        return cli->stop();
    });
}

} // namespace rpc
