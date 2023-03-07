/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/cluster_utils.h"
#include "cluster/fwd.h"
#include "cluster/topic_table_partition_generator.h"
#include "cluster/types.h"
#include "config/node_config.h"
#include "rpc/connection_cache.h"

namespace cluster {

class cloud_storage_size_reducer_exception : public std::runtime_error {
public:
    explicit cloud_storage_size_reducer_exception(const std::string& m);
};

/*
 * This utility class performs a map-reduce operation accross the cluster
 * in order to determine the total number of cloud storage bytes used by
 * all partitions.
 *
 * It iterates across the current topic table in batche. For each batch,
 * the following sequence of operations is performed:
 * 1. Find the first live replica of each partition in the batch
 * 2. Prepare `cloud_storage_usage` RPC requests for each node in the cluster.
 * The request will contain the partitions being queried by shard.
 * 3. Reduce the reponses and update the accumulator.
 *
 * One important thing to note is that this operation requires topic table
 * stability (i.e. the topic table does not change in the meantime). This
 * property is enfored by the `topic_table_partition_generator`, which will
 * throw if the topic table has been updated during the iteration. If such
 * a change does happen, it's treated as a retryable error.
 *
 * Usage: `reduce` should only be called once per object instance.
 */
class cloud_storage_size_reducer {
public:
    static constexpr uint8_t default_retries_allowed = 3;

    cloud_storage_size_reducer(
      ss::sharded<topic_table>& topic_table,
      ss::sharded<members_table>& members_table,
      ss::sharded<partition_leaders_table>& leaders_table,
      ss::sharded<rpc::connection_cache>& conn_cache,
      size_t batch_size = topic_table_partition_generator::default_batch_size,
      uint8_t retries_allowed = 3);

    ss::future<std::optional<uint64_t>> reduce();

private:
    ss::future<uint64_t> do_reduce();

    ss::future<result<cloud_storage_usage_reply>> send_query(
      const model::broker& destination, cloud_storage_usage_request req);

    using requests_t
      = std::vector<ss::future<result<cloud_storage_usage_reply>>>;

    requests_t
    map_batch(const topic_table_partition_generator::generator_type_t& batch);

    model::broker select_replica(
      const model::ntp& ntp,
      const std::vector<model::broker_shard>& replicas) const;

    const model::broker _self{make_self_broker(config::node())};
    const config::tls_config _rpc_tls_config{config::node().rpc_server_tls()};
    const std::chrono::seconds _timeout{2};

    ss::sharded<topic_table>& _topic_table;
    ss::sharded<members_table>& _members_table;
    ss::sharded<partition_leaders_table>& _leaders_table;
    ss::sharded<rpc::connection_cache>& _conn_cache;

    size_t _batch_size;

    uint8_t _retries_allowed;
    uint8_t _retries_attempted{0};
};

} // namespace cluster
