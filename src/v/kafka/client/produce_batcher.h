/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "kafka/protocol/produce.h"
#include "model/fundamental.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/circular_buffer.hh>

#include <absl/container/flat_hash_map.h>

namespace kafka::client {

template<typename ContainerT>
typename ContainerT::value_type consume_front(ContainerT& c) {
    auto b = std::move(c.front());
    c.pop_front();
    return b;
}

/// \brief Batch multiple client requests and satisfy them by a broker response.
///
/// Clients produce record_batches c_bat<n> of records c_rec<m>
/// For each client batch, hold a promise and record count in c_ctx<p>
/// Consume the entire batch b_bat<q>, to send to the broker.
/// The response from the broker is then handled by satisfying the promises for
/// each client batch, by using a running count.
///
/// |     c_bat0    |  c_bat1   |        c_bat2        | client request batches
/// | c_rec0 c_req1 |  c_req2   | c_req3 c_req4 c_req5 | client request records
/// |   c_ctx0(2)   | c_ctx1(1) |       c_ctx2(3)      | client ctx(rec_count)
/// |           b_bat0          |        b_bat1        | broker request batches
/// |          b_ctx0(3)        |       b_ctx1(3)      | broker ctx(rec_count)
class produce_batcher {
public:
    using partition_response = produce_response::partition;
    explicit produce_batcher()
      : produce_batcher(model::compression::none) {}

    explicit produce_batcher(model::compression c)
      : _c(c)
      , _builder{make_builder()}
      , _client_reqs{}
      , _broker_reqs{} {}

    /// \brief Context for a client request
    struct client_context {
        explicit client_context(int32_t record_count)
          : promise{}
          , record_count(record_count) {}
        ss::promise<partition_response> promise;
        int32_t record_count;
    };

    /// \brief Context for a broker request
    struct broker_context {
        explicit broker_context(size_t record_count)
          : record_count(record_count) {}
        int32_t record_count;
        // TODO(Ben): correlation_id id;
    };

    ss::future<partition_response> produce(model::record_batch&& batch) {
        batch.for_each_record([this](model::record rec) {
            _builder.add_raw_kw(
              rec.release_key(), rec.release_value(), std::move(rec.headers()));
        });

        _client_reqs.emplace_back(batch.record_count());
        return _client_reqs.back().promise.get_future();
    }

    ss::future<model::record_batch> consume() {
        auto batch
          = co_await std::exchange(_builder, make_builder()).build_async();
        _broker_reqs.emplace_back(batch.record_count());
        co_return batch;
    }

    void handle_response(partition_response res) {
        auto running_offset = res.base_offset;
        const auto ctx = consume_front(_broker_reqs);
        // TODO(Ben): Assert ctx.id
        const auto consume_to = running_offset
                                + model::offset(ctx.record_count);
        while (running_offset != consume_to) {
            vassert(
              running_offset < consume_to,
              "Attempt to consume more records than have been received");
            auto response = consume_front(_client_reqs);
            response.promise.set_value(partition_response{
              .partition_index{res.partition_index},
              .error_code = res.error_code,
              .base_offset = res.error_code == error_code::none
                               ? model::offset(running_offset)
                               : model::offset(-1),
              // TODO(Ben): Are these correct?
              .log_append_time_ms = res.log_append_time_ms,
              .log_start_offset = res.log_start_offset});
            running_offset += response.record_count;
        }
    }

private:
    storage::record_batch_builder make_builder() {
        auto builder = storage::record_batch_builder(
          model::record_batch_type::raft_data, model::offset(0));
        builder.set_compression(_c);
        return builder;
    }

    model::compression _c;
    storage::record_batch_builder _builder;
    // TODO(Ben): Maybe these should be a queue for backpressure
    ss::circular_buffer<client_context> _client_reqs;
    ss::circular_buffer<broker_context> _broker_reqs;
};

} // namespace kafka::client
