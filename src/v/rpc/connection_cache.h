#pragma once
#include "hashing/jump_consistent_hash.h"
#include "model/metadata.h"
#include "rpc/connection.h"
#include "rpc/reconnect_transport.h"
#include "rpc/types.h"

#include <seastar/core/shared_ptr.hh>

#include <unordered_map>

namespace rpc {
class connection_cache final {
public:
    using transport_ptr = ss::lw_shared_ptr<rpc::reconnect_transport>;
    using underlying = std::unordered_map<model::node_id, transport_ptr>;
    using iterator = typename underlying::iterator;

    static inline ss::shard_id shard_for(const model::node_id&);

    connection_cache() = default;
    bool contains(model::node_id n) const {
        return _cache.find(n) != _cache.end();
    }
    transport_ptr get(model::node_id n) const { return _cache.find(n)->second; }

    /// \brief needs to be a future, because mutations may come from different
    /// fibers and they need to be synchronized
    ss::future<> emplace(
      model::node_id n,
      rpc::transport_configuration c,
      clock_type::duration base_backoff = std::chrono::seconds(1));
    ss::future<> remove(model::node_id n);

    /// \brief closes all connections
    ss::future<> stop();

private:
    ss::semaphore _sem{1}; // to add/remove nodes
    underlying _cache;
};
inline ss::shard_id connection_cache::shard_for(const model::node_id& b) {
    auto h = ::std::hash<model::node_id>()(b);
    return jump_consistent_hash(h, ss::smp::count);
}
} // namespace rpc
