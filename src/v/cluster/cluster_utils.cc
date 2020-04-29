#include "cluster/cluster_utils.h"

#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "rpc/backoff_policy.h"
#include "rpc/types.h"
#include "vlog.h"

#include <chrono>

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

ss::future<> remove_broker_client(
  ss::sharded<rpc::connection_cache>& clients, model::node_id id) {
    auto shard = rpc::connection_cache::shard_for(id);
    vlog(
      clusterlog.debug,
      "Removing {} broker client from cache at shard {}",
      id,
      shard);
    return clients.invoke_on(
      shard, [id](rpc::connection_cache& cache) { return cache.remove(id); });
}

ss::future<> update_broker_client(
  ss::sharded<rpc::connection_cache>& clients,
  model::node_id node,
  unresolved_address addr) {
    auto shard = rpc::connection_cache::shard_for(node);
    vlog(
      clusterlog.debug,
      "Updating {} broker client cache at shard {} ",
      node,
      shard);
    return clients.invoke_on(
      shard,
      [node, rpc_address = std::move(addr)](rpc::connection_cache& cache) {
          return rpc_address.resolve().then(
            [node, &cache](ss::socket_address new_addr) {
                auto f = ss::make_ready_future<>();
                if (cache.contains(node)) {
                    // client is already there, check if configuration changed
                    if (cache.get(node)->server_address() == new_addr) {
                        // If configuration did not changed, do nothing
                        return f;
                    }
                    // configuration changed, first remove the client
                    f = cache.remove(node);
                }
                // there is no client in cache, create new
                return f.then([&cache, node, new_addr = std::move(new_addr)]() {
                    return cache.emplace(
                      node,
                      rpc::transport_configuration{
                        .server_addr = new_addr,
                        .disable_metrics = rpc::metrics_disabled(
                          config::shard_local_cfg().disable_metrics)},
                      rpc::make_exponential_backoff_policy<rpc::clock_type>(
                        std::chrono::seconds(1), std::chrono::seconds(60)));
                });
            });
      });
}

std::vector<topic_result> create_topic_results(
  const std::vector<model::topic_namespace>& topics, errc error_code) {
    std::vector<topic_result> results;
    results.reserve(topics.size());
    std::transform(
      std::cbegin(topics),
      std::cend(topics),
      std::back_inserter(results),
      [error_code](const model::topic_namespace& t) {
          return topic_result(t, error_code);
      });
    return results;
}

model::record_batch_reader
make_deletion_batches(const std::vector<model::topic_namespace>& topics) {
    ss::circular_buffer<model::record_batch> batches;
    batches.reserve(topics.size());

    std::transform(
      std::cbegin(topics),
      std::cend(topics),
      std::back_inserter(batches),
      [](const model::topic_namespace& tp_ns) {
          auto builder = simple_batch_builder(
            controller_record_batch_type, model::offset(0));

          builder.add_kv(
            log_record_key{.record_type = log_record_key::type::topic_deletion},
            tp_ns);
          return std::move(builder).build();
      });

    return model::make_memory_record_batch_reader(std::move(batches));
}

} // namespace cluster
