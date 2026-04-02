// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "kafka/data/rpc/deps.h"
#include "kafka/data/rpc/rpc_service.h"
#include "kafka/data/rpc/serde.h"
#include "model/fundamental.h"
#include "ssx/semaphore.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace kafka::data::rpc {

/**
 * A per core sharded service that handles custom data path requests for
 * kafka-like data without the additional baggage of the kafka API.
 */
class local_service {
public:
    local_service(
      std::unique_ptr<kafka::data::rpc::topic_metadata_cache> metadata_cache,
      std::unique_ptr<kafka::data::rpc::partition_manager> partition_manager,
      std::unique_ptr<kafka::data::rpc::shadow_link_registry>
        shadow_link_registry);

    ss::future<ss::chunked_fifo<kafka_topic_data_result>> produce(
      ss::chunked_fifo<kafka_topic_data> topic_data,
      model::timeout_clock::duration timeout);

    ss::future<partition_offsets_map>
    get_offsets(chunked_vector<topic_partitions> topics);

    ss::future<consume_reply> consume(consume_request req);

    ss::future<> stop();

private:
    ss::future<kafka_topic_data_result>
      produce(kafka_topic_data, model::timeout_clock::duration);

    ss::future<result<model::offset, cluster::errc>> produce(
      model::any_ntp auto,
      ss::chunked_fifo<model::record_batch>,
      model::timeout_clock::duration);

    ss::future<result<partition_offsets, cluster::errc>>
      get_partition_offsets(model::topic, model::partition_id);

    ss::future<result<chunked_vector<model::record_batch>, cluster::errc>>
    consume(
      const model::ktp& ktp,
      kafka::offset start_offset,
      kafka::offset max_offset,
      size_t min_bytes,
      size_t max_bytes,
      model::timeout_clock::duration timeout);

    std::unique_ptr<kafka::data::rpc::topic_metadata_cache> _metadata_cache;
    std::unique_ptr<kafka::data::rpc::partition_manager> _partition_manager;
    std::unique_ptr<kafka::data::rpc::shadow_link_registry>
      _shadow_link_registry;
    ss::abort_source _as;
    ss::gate _gate;
};

/**
 * A networked wrapper for the local service.
 */
class network_service final : public impl::kafka_data_rpc_service {
public:
    struct memory_config {
        ssx::semaphore* memory{nullptr};
        ssize_t total{0};
    };

    network_service(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::sharded<local_service>* service,
      memory_config mem_cfg)
      : impl::kafka_data_rpc_service{sc, ssg}
      , _service(service)
      , _server_memory(mem_cfg.memory)
      , _server_memory_total(mem_cfg.total) {
        vassert(
          (_server_memory == nullptr) == (_server_memory_total == 0),
          "memory_config: memory and total must both be set or both be zero");
    }

    ss::future<produce_reply>
    produce(produce_request, ::rpc::streaming_context&) override;

    ss::future<get_offsets_reply>
    get_offsets(get_offsets_request, ::rpc::streaming_context&) override;

    ss::future<consume_reply>
    consume(consume_request, ::rpc::streaming_context&) override;

private:
    ss::sharded<local_service>* _service;
    ssx::semaphore* _server_memory{nullptr};
    ssize_t _server_memory_total{0};
};

} // namespace kafka::data::rpc
