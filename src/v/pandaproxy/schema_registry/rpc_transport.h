/*
 * Copyright 2025 Redpanda Data, Inc.
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
/// registry internal topic I/O. Routes internally via RPC — simpler, no
/// auth overhead.
class rpc_transport final : public transport {
public:
    explicit rpc_transport(kafka::data::rpc::client& client);

    ss::future<produce_result> produce(model::record_batch batch) override;
    ss::future<model::offset> get_high_watermark() override;
    ss::future<> consume_range(
      model::offset start,
      model::offset end,
      ss::noncopyable_function<ss::future<>(model::record_batch)> consumer)
      override;

private:
    kafka::data::rpc::client& _client;
};

} // namespace pandaproxy::schema_registry
