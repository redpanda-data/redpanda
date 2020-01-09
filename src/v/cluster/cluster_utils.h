#pragma once
#include "cluster/controller_service.h"
#include "cluster/types.h"
#include "rpc/connection_cache.h"

#include <seastar/core/sharded.hh>

namespace cluster {

class metadata_cache;
/// This method calculates the machine nodes that were updated/added
/// and removed
brokers_diff calculate_changed_brokers(
  std::vector<broker_ptr> new_list, std::vector<broker_ptr> old_list);

/// Creates the same topic_result for all requests
// clang-format off
template<typename T>
CONCEPT(requires requires(const T& req) {
    { req.topic } -> model::topic;
})
// clang-format on
std::vector<topic_result> create_topic_results(
  const std::vector<T>& requests, topic_error_code error_code) {
    std::vector<topic_result> results;
    results.reserve(requests.size());
    std::transform(
      std::cbegin(requests),
      std::cend(requests),
      std::back_inserter(results),
      [error_code](const T& r) { return topic_result(r.topic, error_code); });
    return results;
}

std::vector<model::broker> get_replica_set_brokers(
  const metadata_cache& md_cache, std::vector<model::broker_shard> replicas);

ss::future<> update_broker_client(
  ss::sharded<rpc::connection_cache>&,
  model::node_id node,
  unresolved_address addr);
ss::future<>
remove_broker_client(ss::sharded<rpc::connection_cache>&, model::node_id);

/// \brief Dispatches controller service RPC requests to the specified
/// unresolved_address. It uses the connection cached in connection_cache or if
/// it is unavailable it creates a new one.

// clang-format off
template<typename Func>
CONCEPT(requires requires(Func&& f, controller_client_protocol& c) {
        f(c);
})
ss::futurize_t<std::result_of_t<Func(controller_client_protocol&)>>
  // clang-format on
  dispatch_rpc(
    ss::sharded<rpc::connection_cache>& cache,
    model::node_id id,
    unresolved_address addr,
    Func&& f) {
    using ret_t
      = ss::futurize<std::result_of_t<Func(controller_client_protocol&)>>;
    using rpc_protocol = controller_client_protocol;
    return update_broker_client(cache, id, std::move(addr))
      .then([id, &cache, f = std::forward<Func>(f)]() mutable {
          auto shard = rpc::connection_cache::shard_for(id);
          return cache.invoke_on(
            shard,
            [id, f = std::forward<Func>(f)](
              rpc::connection_cache& c_cache) mutable {
                return c_cache.get(id)->get_connected().then(
                  [f = std::forward<Func>(f),
                   id](result<rpc::transport*> r) mutable {
                      if (!r) {
                          return ret_t::make_exception_future(
                            std::runtime_error(
                              fmt::format("Error connecting node {}", id)));
                      }
                      return ss::do_with(
                        rpc_protocol(*r.value()),
                        [f = std::forward<Func>(f)](
                          rpc_protocol& proto) mutable { return f(proto); });
                  });
            });
      });
}

} // namespace cluster
