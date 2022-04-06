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

#include "kafka/client/configuration.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/produce_batcher.h"
#include "model/fundamental.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/flat_hash_map.h>

namespace kafka::client {

/// \brief Batch multiple client requests, flush them based on size or time.
class produce_partition {
public:
    using response = produce_batcher::partition_response;
    using consumer = ss::noncopyable_function<void(model::record_batch&&)>;

    produce_partition(const configuration& config, consumer&& c)
      : _config{config}
      , _batcher{}
      , _timer{[this]() { try_consume(true); }}
      , _consumer{std::move(c)} {}

    ss::future<response> produce(model::record_batch&& batch) {
        _record_count += batch.record_count();
        _size_bytes += batch.size_bytes();
        auto fut = _batcher.produce(std::move(batch));
        try_consume(false);
        return fut;
    }

    void handle_response(response&& res) {
        vassert(_in_flight, "handle_response requires a batch in flight");
        _batcher.handle_response(std::move(res));
        _in_flight = false;
        try_consume(false);
    }

    ss::future<> stop() {
        try_consume(true);
        _timer.cancel();
        return ss::now();
    }

private:
    model::record_batch do_consume() {
        vassert(!_in_flight, "do_consume should not run concurrently");

        _in_flight = true;
        _record_count = 0;
        _size_bytes = 0;
        return _batcher.consume();
    }

    bool try_consume(bool timed_out) {
        if (_in_flight || _record_count == 0) {
            return false;
        }

        auto batch_record_count = _config.produce_batch_record_count();
        auto batch_size_bytes = _config.produce_batch_size_bytes();

        auto threshold_met = _record_count >= batch_record_count
                             || _size_bytes >= batch_size_bytes;

        if (!timed_out && !threshold_met) {
            _timer.cancel();
            _timer.arm(_config.produce_batch_delay());
            return false;
        }

        _consumer(do_consume());
        return true;
    }

    const configuration& _config;
    produce_batcher _batcher{};
    ss::timer<> _timer{};
    consumer _consumer;
    int32_t _record_count{};
    int32_t _size_bytes{};
    bool _in_flight{};
};

} // namespace kafka::client
