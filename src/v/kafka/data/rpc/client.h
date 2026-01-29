/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "kafka/data/rpc/deps.h"
#include "model/fundamental.h"
#include "rpc/fwd.h"
// TODO(oren): make data service
#include "kafka/data/rpc/serde.h"
#include "kafka/data/rpc/service.h"

namespace kafka::data::rpc {

/**
 * A client for kafka data plane rpcs.
 *
 * This is a sharded service that exists on every core, requests that can be
 * serviced locally will not go through the rpc boundary but will directly go to
 * the local service.
 */
class client {
public:
    client(
      model::node_id self,
      std::unique_ptr<kafka::data::rpc::partition_leader_cache>,
      std::unique_ptr<kafka::data::rpc::topic_creator>,
      std::unique_ptr<kafka::data::rpc::topic_metadata_cache>,
      ss::sharded<::rpc::connection_cache>*,
      ss::sharded<local_service>*);

    client(client&&) = delete;
    client& operator=(client&&) = delete;
    client(const client&) = delete;
    client& operator=(const client&) = delete;
    ~client() = default;

    /**
     * Produce the following record batches to the specified topic partition in
     * the kafka namespace.
     *
     * This implementation will retry failed produce requests rather
     * aggressively, (without idempotency at the time of writing) so take note
     * that this can easily produce duplicate data.
     */
    ss::future<cluster::errc>
      produce(model::topic_partition, ss::chunked_fifo<model::record_batch>);

    ss::future<cluster::errc>
      produce(model::topic_partition, model::record_batch);

    ss::future<cluster::errc> create_topic(
      model::topic_namespace_view,
      cluster::topic_properties,
      std::optional<int32_t> partition_count = std::nullopt,
      std::optional<int16_t> replication_factor = std::nullopt);

    ss::future<cluster::errc> update_topic(cluster::topic_properties_update);

    ss::future<> start();
    ss::future<> stop();

    ss::future<cluster::errc> try_create_topic(
      model::topic_namespace_view,
      cluster::topic_properties,
      std::optional<int32_t> partition_count,
      std::optional<int16_t> replication_factor = std::nullopt);

    ss::future<result<partition_offsets_map, cluster::errc>>
      get_partition_offsets(chunked_vector<topic_partitions>);

    ss::future<result<consume_reply, cluster::errc>> consume(
      model::topic_partition,
      kafka::offset start_offset,
      kafka::offset max_offset,
      size_t min_bytes,
      size_t max_bytes,
      model::timeout_clock::duration timeout);

private:
    ss::future<cluster::errc> do_produce_once(produce_request);
    ss::future<produce_reply> do_local_produce(produce_request);
    ss::future<produce_reply>
      do_remote_produce(model::node_id, produce_request);

    ss::future<result<partition_offsets_map, cluster::errc>>
    get_remote_partition_offsets(
      model::node_id, chunked_vector<topic_partitions> topics);

    template<typename Func>
    std::invoke_result_t<Func> retry(Func&&);

    model::node_id _self;
    std::unique_ptr<kafka::data::rpc::partition_leader_cache> _leaders;
    std::unique_ptr<kafka::data::rpc::topic_creator> _topic_creator;
    std::unique_ptr<kafka::data::rpc::topic_metadata_cache> _metadata_cache;
    ss::sharded<::rpc::connection_cache>* _connections;
    ss::sharded<local_service>* _local_service;
    ss::abort_source _as;
    ss::gate _gate;
};

} // namespace kafka::data::rpc
