#include "raft/client_cache.h"

#include <fmt/format.h>

namespace raft {

/// \brief needs to be a future, because mutations may come from different
/// fibers and they need to be synchronized
future<> client_cache::emplace(model::node_id n, rpc::client_configuration c) {
    if (auto s = shard_for(n); s != engine().cpu_id()) {
        throw std::runtime_error(fmt::format(
          "Cannot ::emplace, node:{}, belonging to shard:{}, on shard:{}",
          n,
          s,
          engine().cpu_id()));
    }
    return with_semaphore(
      _sem, 1, [this, n = std::move(n), c = std::move(c)]() mutable {
          _cache.emplace(
            std::move(n), make_lw_shared<client_type>(std::move(c)));
      });
}
future<> client_cache::remove(model::node_id n) {
    if (auto s = shard_for(n); s != engine().cpu_id()) {
        throw std::runtime_error(fmt::format(
          "Cannot ::remove, node:{}, belonging to shard:{}, on shard:{}",
          n,
          s,
          engine().cpu_id()));
    }
    return with_semaphore(
      _sem, 1, [this, n = std::move(n)] { _cache.erase(n); });
}

/// \brief closes all client connections
future<> client_cache::stop() {
    return parallel_for_each(_cache, [](auto& it) {
        auto& [_, cli] = it;
        return cli->stop();
    });
}

} // namespace raft
