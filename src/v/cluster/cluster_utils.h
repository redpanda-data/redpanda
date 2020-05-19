#pragma once
#include "cluster/controller_service.h"
#include "cluster/errc.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "outcome_future_utils.h"
#include "rpc/connection_cache.h"

#include <seastar/core/sharded.hh>

#include <utility>

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
    { req.tp_ns } -> model::topic_namespace;
})
// clang-format on
std::vector<topic_result> create_topic_results(
  const std::vector<T>& requests, errc error_code) {
    std::vector<topic_result> results;
    results.reserve(requests.size());
    std::transform(
      std::cbegin(requests),
      std::cend(requests),
      std::back_inserter(results),
      [error_code](const T& r) { return topic_result(r.tp_ns, error_code); });
    return results;
}

std::vector<topic_result> create_topic_results(
  const std::vector<model::topic_namespace>& topics, errc error_code);

std::vector<model::broker> get_replica_set_brokers(
  const metadata_cache& md_cache, std::vector<model::broker_shard> replicas);

ss::future<> update_broker_client(
  ss::sharded<rpc::connection_cache>&,
  model::node_id node,
  unresolved_address addr);
ss::future<>
remove_broker_client(ss::sharded<rpc::connection_cache>&, model::node_id);

// clang-format off
template<typename Proto, typename Func>
CONCEPT(requires requires(Func&& f, Proto c) {
        f(c);
})
// clang-format on
auto with_client(
  ss::sharded<rpc::connection_cache>& cache,
  model::node_id id,
  unresolved_address addr,
  Func&& f) {
    return update_broker_client(cache, id, std::move(addr))
      .then([id, &cache, f = std::forward<Func>(f)]() mutable {
          return cache.local().with_node_client<Proto, Func>(
            id, std::forward<Func>(f));
      });
}

model::record_batch_reader
make_deletion_batches(const std::vector<model::topic_namespace>&);

/// Creates current broker instance using its configuration.
model::broker make_self_broker(const config::configuration& cfg);
} // namespace cluster
