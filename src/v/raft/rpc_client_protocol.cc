#include "raft/rpc_client_protocol.h"

#include "outcome_future_utils.h"
#include "raft/raftgen_service.h"
#include "rpc/connection_cache.h"
#include "rpc/exceptions.h"
#include "rpc/transport.h"
#include "rpc/types.h"

namespace raft {

template<typename Func>
auto with_node_client(
  ss::sharded<rpc::connection_cache>& connection_cache,
  model::node_id node_id,
  Func&& f) {
    using futurize
      = ss::futurize<typename std::result_of_t<Func(raftgen_client_protocol)>>;
    using inner_t =
      typename std::tuple_element<0, typename futurize::value_type>::type;
    using ret_t = result<inner_t>;

    constexpr bool is_already_result = outcome::is_basic_result_v<inner_t>;
    static_assert(!is_already_result, "nested result<T> not yet supported");

    auto shard = rpc::connection_cache::shard_for(node_id);
    return connection_cache.invoke_on(
      shard,
      [node_id,
       f = std::forward<Func>(f)](rpc::connection_cache& cache) mutable {
          if (!cache.contains(node_id)) {
              // No client available
              return ss::make_ready_future<ret_t>(errc::missing_tcp_client);
          }
          return cache.get(node_id)->get_connected().then(
            [f = std::forward<Func>(f)](
              result<rpc::transport*> transport) mutable {
                if (!transport) {
                    // Connection error
                    return ss::make_ready_future<ret_t>(transport.error());
                }
                typename futurize::type res_f = f(
                  raftgen_client_protocol(*transport.value()));

                return wrap_exception_with_result<
                  rpc::request_timeout_exception,
                  std::error_code>(errc::timeout, std::move(res_f));
            });
      });
}

ss::future<result<vote_reply>> rpc_client_protocol::vote(
  model::node_id n, vote_request&& r, clock_type::time_point timeout) {
    return with_node_client(
      _connection_cache,
      n,
      [r = std::move(r), timeout](raftgen_client_protocol client) mutable {
          return client.vote(std::move(r), timeout)
            .then([](rpc::client_context<vote_reply> ctx) {
                return std::move(ctx.data);
            });
      });
}

ss::future<result<append_entries_reply>> rpc_client_protocol::append_entries(
  model::node_id n,
  append_entries_request&& r,
  clock_type::time_point timeout) {
    return with_node_client(
      _connection_cache,
      n,
      [r = std::move(r), timeout](raftgen_client_protocol client) mutable {
          return client.append_entries(std::move(r), timeout)
            .then([](rpc::client_context<append_entries_reply> ctx) {
                return std::move(ctx.data);
            });
      });
}

ss::future<result<heartbeat_reply>> rpc_client_protocol::heartbeat(
  model::node_id n, heartbeat_request&& r, clock_type::time_point timeout) {
    return with_node_client(
      _connection_cache,
      n,
      [r = std::move(r), timeout](raftgen_client_protocol client) mutable {
          return client.heartbeat(std::move(r), timeout)
            .then([](rpc::client_context<heartbeat_reply> ctx) {
                return std::move(ctx.data);
            });
      });
}

} // namespace raft