/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/data/rpc/fwd.h"
#include "pandaproxy/schema_registry/transport.h"

namespace pandaproxy::schema_registry {

/// Transport implementation that wraps kafka::data::rpc::client for schema
/// registry internal topic I/O.
///
/// TODO: Add transport-layer retry for topic_not_exists with backoff,
/// analogous to how kafka_client_transport gets SR-specific retry via
/// gated_retry_with_mitigation + mitigate_error. The k/d/rpc client is
/// general-purpose and shouldn't retry missing topics, but this transport
/// knows the _schemas topic must exist and can retry at the right layer.
/// Currently the do_start loop in service.cc handles this as a fallback.
class rpc_transport final : public transport {
public:
    explicit rpc_transport(kafka::data::rpc::client& client);

    ss::future<> stop() final { return ss::now(); }

    ss::future<produce_result> produce(model::record_batch batch) override;
    ss::future<model::offset> get_high_watermark() override;
    ss::future<> consume_range(
      model::offset start,
      model::offset end,
      ss::noncopyable_function<
        ss::future<ss::stop_iteration>(model::record_batch)> consumer) override;
    ss::future<cluster::errc> create_topic(
      model::topic_namespace_view,
      int32_t partition_count,
      cluster::topic_properties,
      int16_t replication_factor) override;

private:
    kafka::data::rpc::client& _client;
};

} // namespace pandaproxy::schema_registry
