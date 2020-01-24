#include "rpc/connection_cache.h"

#include <fmt/format.h>

#include <chrono>

namespace rpc {

/// \brief needs to be a future, because mutations may come from different
/// fibers and they need to be synchronized
ss::future<> connection_cache::emplace(
  model::node_id n,
  rpc::transport_configuration c,
  clock_type::duration base_backoff) {
    if (auto s = shard_for(n); s != ss::engine().cpu_id()) {
        throw std::runtime_error(fmt::format(
          "Cannot ::emplace, node:{}, belonging to shard:{}, on shard:{}",
          n,
          s,
          ss::engine().cpu_id()));
    }
    return with_semaphore(
      _sem, 1, [this, n, c = std::move(c), base_backoff]() mutable {
          _cache.emplace(
            std::move(n),
            ss::make_lw_shared<rpc::reconnect_transport>(
              std::move(c), base_backoff));
      });
}
ss::future<> connection_cache::remove(model::node_id n) {
    if (auto s = shard_for(n); s != ss::engine().cpu_id()) {
        throw std::runtime_error(fmt::format(
          "Cannot ::remove, node:{}, belonging to shard:{}, on shard:{}",
          n,
          s,
          ss::engine().cpu_id()));
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
