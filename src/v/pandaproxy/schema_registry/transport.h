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

#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

namespace pandaproxy::schema_registry {

/// Result of a produce operation — includes offset for collision detection.
struct produce_result {
    model::offset base_offset;
};

/// Abstract transport for schema registry's internal topic I/O.
///
/// Implementations exist for kafka::client (legacy) and kafka::data::rpc
/// (RPC-based, no auth overhead).
class transport {
public:
    transport() = default;
    virtual ~transport() = default;
    transport(const transport&) = delete;
    transport& operator=(const transport&) = delete;
    transport(transport&&) = delete;
    transport& operator=(transport&&) = delete;

    /// Produce a batch to the _schemas topic. Returns the base_offset.
    virtual ss::future<produce_result> produce(model::record_batch batch) = 0;

    /// Get the high watermark (next offset) for the _schemas topic.
    virtual ss::future<model::offset> get_high_watermark() = 0;

    /// Consume batches from [start, end) on the _schemas topic.
    /// Calls consumer(batch) for each batch. Handles pagination internally.
    virtual ss::future<> consume_range(
      model::offset start,
      model::offset end,
      ss::noncopyable_function<ss::future<>(model::record_batch)> consumer)
      = 0;

    /// One-time startup configuration (credentials, ACLs).
    /// Default: no-op (RPC transport needs no configuration).
    virtual ss::future<> configure() { co_return; }

    /// Error callback for the underlying client.
    /// Default: rethrow (RPC transport has no mitigation logic).
    virtual ss::future<> mitigate_error(std::exception_ptr eptr) {
        return ss::make_exception_future<>(eptr);
    }

    /// Check topic-creation authorization.
    /// Default: no-op (RPC transport bypasses Kafka auth).
    virtual ss::future<>
    validate_topic_creation_authorization(int16_t /*replication_factor*/) {
        co_return;
    }

    /// Whether the transport uses ephemeral credentials.
    virtual bool has_ephemeral_credentials() const { return false; }
};

} // namespace pandaproxy::schema_registry
