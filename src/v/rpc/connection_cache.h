#pragma once
#include "hashing/jump_consistent_hash.h"
#include "model/metadata.h"
#include "outcome.h"
#include "outcome_future_utils.h"
#include "rpc/backoff_policy.h"
#include "rpc/connection.h"
#include "rpc/errc.h"
#include "rpc/reconnect_transport.h"
#include "rpc/types.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <chrono>
#include <unordered_map>

namespace rpc {
class connection_cache final
  : public ss::peering_sharded_service<connection_cache> {
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
    ss::future<>
    emplace(model::node_id n, rpc::transport_configuration c, backoff_policy);
    ss::future<> remove(model::node_id n);

    /// \brief closes all connections
    ss::future<> stop();

    // clang-format off
    template<typename Protocol, typename Func>
    CONCEPT(requires requires(Func&& f, Protocol proto) { 
        f(proto); 
    })
    // clang-format on
    auto with_node_client(model::node_id node_id, Func&& f) {
        using ret_t = result_wrap_t<std::result_of_t<Func(Protocol)>>;
        auto shard = rpc::connection_cache::shard_for(node_id);

        return container().invoke_on(
          shard,
          [node_id,
           f = std::forward<Func>(f)](rpc::connection_cache& cache) mutable {
              if (!cache.contains(node_id)) {
                  // No client available
                  return ss::futurize<ret_t>::convert(
                    rpc::make_error_code(errc::missing_node_rpc_client));
              }
              return cache.get(node_id)->get_connected().then(
                [f = std::forward<Func>(f)](
                  result<rpc::transport*> transport) mutable {
                    if (!transport) {
                        // Connection error
                        return ss::futurize<ret_t>::convert(transport.error());
                    }
                    auto res_f = f(Protocol(*transport.value()));
                    // FIXME: Remove this as soon as we will introduce result
                    //        based error handling in RPC layer
                    return wrap_exception_with_result<
                      rpc::request_timeout_exception,
                      std::error_code>(
                      rpc::make_error_code(errc::client_request_timeout),
                      std::move(res_f));
                });
          });
    }

private:
    ss::semaphore _sem{1}; // to add/remove nodes
    underlying _cache;
};
inline ss::shard_id connection_cache::shard_for(const model::node_id& b) {
    auto h = ::std::hash<model::node_id>()(b);
    return jump_consistent_hash(h, ss::smp::count);
}

} // namespace rpc
