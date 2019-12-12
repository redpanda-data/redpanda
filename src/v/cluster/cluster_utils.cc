#include "cluster/cluster_utils.h"

#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/types.h"
#include "raft/client_cache.h"

namespace cluster {
brokers_diff calculate_changed_brokers(
  std::vector<broker_ptr> new_list, std::vector<broker_ptr> old_list) {
    brokers_diff diff;
    auto compare_by_id = [](const broker_ptr& lhs, const broker_ptr& rhs) {
        return *lhs < *rhs;
    };
    // updated/added brokers
    std::sort(new_list.begin(), new_list.end(), compare_by_id);
    std::sort(old_list.begin(), old_list.end(), compare_by_id);
    std::set_difference(
      std::cbegin(new_list),
      std::cend(new_list),
      std::cbegin(old_list),
      std::cend(old_list),
      std::back_inserter(diff.updated),
      [](const broker_ptr& lhs, const broker_ptr& rhs) {
          return *lhs != *rhs;
      });
    // removed brokers
    std::set_difference(
      std::cbegin(old_list),
      std::cend(old_list),
      std::cbegin(new_list),
      std::cend(new_list),
      std::back_inserter(diff.removed),
      compare_by_id);

    return diff;
}

std::vector<model::broker> get_replica_set_brokers(
  const metadata_cache& md_cache, std::vector<model::broker_shard> replicas) {
    std::vector<model::broker> brokers;
    brokers.reserve(replicas.size());
    std::transform(
      std::cbegin(replicas),
      std::cend(replicas),
      std::back_inserter(brokers),
      [&md_cache](model::broker_shard bs) {
          // executed on target consensus shard
          // cache.local() is a mirror
          auto br = md_cache.get_broker(bs.node_id);
          if (!br) {
              throw std::runtime_error(
                fmt::format("Broker {} not found in cache", bs.node_id));
          }
          return *(br.value());
      });
    return brokers;
}

future<>
remove_broker_client(sharded<raft::client_cache>& clients, model::node_id id) {
    auto shard = raft::client_cache::shard_for(id);
    clusterlog.debug(
      "Removing {} broker client from cache at shard {}", id, shard);
    return clients.invoke_on(
      shard, [id](raft::client_cache& cache) { return cache.remove(id); });
}

future<>
update_broker_client(sharded<raft::client_cache>& clients, broker_ptr node) {
    auto shard = raft::client_cache::shard_for(node->id());
    clusterlog.debug(
      "Updating {} broker client cache at shard {} ", node->id(), shard);
    return clients.invoke_on(
      shard,
      [id = node->id(),
       rpc_address = node->rpc_address()](raft::client_cache& cache) {
          return rpc_address.resolve().then(
            [id, &cache](socket_address new_addr) {
                auto f = make_ready_future<>();
                if (cache.contains(id)) {
                    // client is already there, check if configuration changed
                    if (cache.get(id)->server_address() == new_addr) {
                        // If configuration did not changed, do nothing
                        return f;
                    }
                    // configuration changed, first remove the client
                    f = cache.remove(id);
                }
                // there is no client in cache, create new
                return f.then([&cache, id, new_addr = std::move(new_addr)]() {
                    return cache.emplace(
                      id, rpc::client_configuration{.server_addr = new_addr});
                });
            });
      });
}
} // namespace cluster