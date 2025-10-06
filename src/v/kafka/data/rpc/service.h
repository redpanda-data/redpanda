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
      std::unique_ptr<kafka::data::rpc::partition_manager> partition_manager);

    ss::future<ss::chunked_fifo<kafka_topic_data_result>> produce(
      ss::chunked_fifo<kafka_topic_data> topic_data,
      model::timeout_clock::duration timeout);

    ss::future<partition_offsets_map>
    get_offsets(chunked_vector<topic_partitions> topics);

    ss::future<delete_records_result_map> delete_records(
      delete_records_cmd_map cmds, model::timeout_clock::duration timeout);

private:
    ss::future<kafka_topic_data_result>
      produce(kafka_topic_data, model::timeout_clock::duration);

    ss::future<result<model::offset, cluster::errc>> produce(
      model::any_ntp auto,
      ss::chunked_fifo<model::record_batch>,
      model::timeout_clock::duration);

    ss::future<result<partition_offsets, cluster::errc>>
      get_partition_offsets(model::topic, model::partition_id);

    ss::future<result<kafka::offset, error_code>> delete_records(
      model::any_ntp auto,
      delete_records_cmd,
      model::timeout_clock::time_point timeout);

    std::unique_ptr<kafka::data::rpc::topic_metadata_cache> _metadata_cache;
    std::unique_ptr<kafka::data::rpc::partition_manager> _partition_manager;
};

/**
 * A networked wrapper for the local service.
 */
class network_service final : public impl::kafka_data_rpc_service {
public:
    network_service(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::sharded<local_service>* service)
      : impl::kafka_data_rpc_service{sc, ssg}
      , _service(service) {}

    ss::future<produce_reply>
    produce(produce_request, ::rpc::streaming_context&) override;

    ss::future<get_offsets_reply>
    get_offsets(get_offsets_request, ::rpc::streaming_context&) override;

    ss::future<delete_records_reply>
    delete_records(delete_records_request, ::rpc::streaming_context&) override;

private:
    ss::sharded<local_service>* _service;
};

} // namespace kafka::data::rpc
