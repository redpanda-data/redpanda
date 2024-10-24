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

#include "kafka/client/config_utils.h"
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
      , _batcher{compression_from_str(config.produce_compression_type())}
      , _timer{[this]() {
          ssx::spawn_with_gate(_gate, [this]() { return try_consume(); });
      }}
      , _consumer{std::move(c)} {}

    ss::future<response> produce(model::record_batch&& batch) {
        _record_count += batch.record_count();
        _size_bytes += batch.size_bytes();
        auto fut = _batcher.produce(std::move(batch));
        arm_consumer();
        return fut;
    }

    void handle_response(response&& res) {
        vassert(_in_flight, "handle_response requires a batch in flight");
        _batcher.handle_response(std::move(res));
        _in_flight = false;
        if (_await_in_flight) {
            _await_in_flight->set_value();
            _await_in_flight = std::nullopt;
        }
        arm_consumer();
    }

    ss::future<> await_in_flight() {
        if (!_in_flight) {
            return ss::now();
        }
        vassert(!_await_in_flight, "Double call to await_in_flight()");
        _await_in_flight = ss::promise<>();
        return _await_in_flight->get_future();
    }

    ss::future<> maybe_drain() {
        /// Immediately force flush of buffer
        if (consumer_can_run()) {
            _timer.cancel();
            _consumer(co_await do_consume());
        }
    }

    ss::future<> stop() {
        _timer.set_callback([]() {});
        co_await try_consume();
        _timer.cancel();
        co_await _gate.close();
    }

private:
    ss::future<model::record_batch> do_consume() {
        vassert(!_in_flight, "do_consume should not run concurrently");

        _in_flight = true;
        _record_count = 0;
        _size_bytes = 0;
        return _batcher.consume();
    }

    ss::future<> try_consume() {
        if (consumer_can_run()) {
            _consumer(co_await do_consume());
        }
    }

    /// \brief Arms the timer that starts the consumer
    ///
    /// Will arm the timer only if the consumer can run and then sets the timer
    /// delay depending on whether or not size thresholds have been met
    void arm_consumer() {
        if (!consumer_can_run()) {
            return;
        }

        std::chrono::milliseconds rearm_timer_delay{0};
        // If the threshold is met, then use a delay of 0 so the timer fires
        // nearly immediately after this call.  Otherwise use the produce
        // batch delay when arming the timer.
        if (!threshold_met()) {
            rearm_timer_delay = _config.produce_batch_delay();
        }
        _timer.cancel();
        _timer.arm(rearm_timer_delay);
    }

    /// \brief Validates that the size threshold has been met to trigger produce
    bool threshold_met() const {
        auto batch_record_count = _config.produce_batch_record_count();
        auto batch_size_bytes = _config.produce_batch_size_bytes();

        return _record_count >= batch_record_count
               || _size_bytes >= batch_size_bytes;
    }

    /// \brief Checks to see if the consumer can run
    ///
    /// Consumer can only run if one is not already running and there are
    /// records available
    bool consumer_can_run() const { return !_in_flight && _record_count > 0; }

    const configuration& _config;
    produce_batcher _batcher{};
    ss::timer<> _timer{};
    consumer _consumer;
    int32_t _record_count{};
    int32_t _size_bytes{};
    bool _in_flight{};
    ss::gate _gate;
    std::optional<ss::promise<>> _await_in_flight;
};

} // namespace kafka::client
