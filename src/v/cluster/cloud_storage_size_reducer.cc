/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cloud_storage_size_reducer.h"

#include "cluster/controller_service.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"

namespace cluster {
cloud_storage_size_reducer_exception::cloud_storage_size_reducer_exception(
  const std::string& m)
  : std::runtime_error(m) {}

cloud_storage_size_reducer::cloud_storage_size_reducer(
  ss::sharded<topic_table>& topic_table,
  ss::sharded<members_table>& members_table,
  ss::sharded<partition_leaders_table>& leaders_table,
  ss::sharded<rpc::connection_cache>& conn_cache,
  size_t batch_size,
  uint8_t retries_allowed)
  : _topic_table(std::ref(topic_table))
  , _members_table(std::ref(members_table))
  , _leaders_table(std::ref(leaders_table))
  , _conn_cache{conn_cache}
  , _batch_size{batch_size}
  , _retries_allowed{retries_allowed} {}

ss::future<std::optional<uint64_t>> cloud_storage_size_reducer::reduce() {
    try {
        co_return co_await do_reduce();
    } catch (const topic_table_partition_generator_exception& e) {
        vlog(
          clusterlog.debug,
          "Failed to generate total cloud storage usage across the cluster "
          "(retries left: {}): {}",
          _retries_allowed - _retries_attempted,
          e.what());
    } catch (const cloud_storage_size_reducer_exception& e) {
        vlog(
          clusterlog.debug,
          "Failed to generate total cloud storage usage across the cluster "
          "(retries left: {}): {}",
          _retries_allowed - _retries_attempted,
          e.what());
    } catch (const std::exception& e) {
        vlog(clusterlog.warn, "Unexpected exception thrown: {}", e.what());
        throw;
    }

    if (++_retries_attempted <= _retries_allowed) {
        co_return co_await reduce();
    } else {
        vlog(
          clusterlog.warn,
          "Failed to generate cloud storage usage for cluster after {} retries",
          _retries_allowed);
        co_return std::nullopt;
    }
}

ss::future<uint64_t> cloud_storage_size_reducer::do_reduce() {
    topic_table_partition_generator partition_gen{_topic_table, _batch_size};

    uint64_t total_cloud_storage_bytes{0};
    auto batch = co_await partition_gen.next_batch();
    while (batch) {
        requests_t request_futures = map_batch(*batch);
        auto results = co_await ss::when_all_succeed(
          request_futures.begin(), request_futures.end());

        for (auto& res : results) {
            if (res.has_error()) {
                throw cloud_storage_size_reducer_exception(
                  "RPC for cloud storage usage failed");
            }

            if (res.value().missing_partitions.size() > 0) {
                throw cloud_storage_size_reducer_exception(fmt::format(
                  "Cloud storage usage for {} partitions was not found",
                  res.value().missing_partitions.size()));
            }

            total_cloud_storage_bytes += res.value().total_size_bytes;
        }

        batch = co_await partition_gen.next_batch();
    }

    co_return total_cloud_storage_bytes;
}

ss::future<result<cloud_storage_usage_reply>>
cloud_storage_size_reducer::send_query(
  const model::broker& destination, cloud_storage_usage_request req) {
    return with_client<controller_client_protocol>(
      _self.id(),
      _conn_cache,
      destination.id(),
      destination.rpc_address(),
      _rpc_tls_config,
      _timeout,
      [this, req = std::move(req)](controller_client_protocol c) mutable {
          return c
            .cloud_storage_usage(std::move(req), rpc::client_opts(_timeout))
            .then(&rpc::get_ctx_data<cloud_storage_usage_reply>);
      });
}

cloud_storage_size_reducer::requests_t cloud_storage_size_reducer::map_batch(
  const topic_table_partition_generator::generator_type_t& batch) {
    absl::flat_hash_map<model::broker, std::vector<model::ntp>> ntps_per_node;

    for (const auto& [ntp, replicas] : batch) {
        auto broker = select_replica(ntp, replicas);
        ntps_per_node[broker].push_back(ntp);
    }

    requests_t futs;
    futs.reserve(ntps_per_node.size());

    for (auto& [broker, ntps] : ntps_per_node) {
        futs.emplace_back(send_query(broker, {.partitions = std::move(ntps)}));
    }

    return futs;
}

// This function picks a replica of a given partition to serve
// the cloud_storage_usage request. If we think the leader is alive,
// pick it, otherwise select the first live replica.
model::broker cloud_storage_size_reducer::select_replica(
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& replicas) const {
    const auto leader = _leaders_table.local().get_leader(ntp);

    std::optional<model::broker> first_live_replica;
    for (const auto& replica : replicas) {
        if (
          auto md = _members_table.local().get_node_metadata_ref(
            replica.node_id)) {
            if (leader && *leader == replica.node_id) {
                return md.value().get().broker;
            }

            if (!first_live_replica) {
                first_live_replica = md.value().get().broker;
            }
        }
    }

    if (first_live_replica) {
        return *first_live_replica;
    }

    throw cloud_storage_size_reducer_exception(
      fmt::format("No live replicas for {}", ntp));
}

} // namespace cluster
