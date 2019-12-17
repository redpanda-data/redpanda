#pragma once
#include "cluster/types.h"
#include "rpc/connection_cache.h"

#include <seastar/core/sharded.hh>

namespace rpc {
class connection_cache;
}

#include <iterator>

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

future<> update_broker_client(sharded<rpc::connection_cache>&, broker_ptr);
future<> remove_broker_client(sharded<rpc::connection_cache>&, model::node_id);
} // namespace cluster