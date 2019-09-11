#pragma once

#include "hashing/jump_consistent_hash.h"
#include "raft/raftgen_service.h"
#include "raft/reconnect_client.h"
#include "raft/types.h"

#include <seastar/core/shared_ptr.hh>

#include <unordered_map>

namespace raft {
class client_cache final {
public:
    using client_type = lw_shared_ptr<reconnect_client>;
    using underlying = std::unordered_map<model::node_id, client_type>;
    using iterator = typename underlying::iterator;

    static inline shard_id shard_for(const model::node_id&);

    client_cache() = default;
    bool contains(model::node_id n) {
        return _cache.find(n) != _cache.end();
    }
    client_type get(model::node_id n) {
        return _cache.find(n)->second;
    }

    /// \brief needs to be a future, because mutations may come from different
    /// fibers and they need to be synchronized
    future<> emplace(model::node_id n, rpc::client_configuration c);
    future<> remove(model::node_id n);

    /// \brief closes all client connections
    future<> stop();

private:
    semaphore _sem{1}; // to add/remove nodes
    underlying _cache;
};
inline shard_id client_cache::shard_for(const model::node_id& b) {
    auto h = ::std::hash<model::node_id>()(b);
    return jump_consistent_hash(h, smp::count);
}
} // namespace raft
