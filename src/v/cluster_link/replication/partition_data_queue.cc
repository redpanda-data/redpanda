/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/partition_data_queue.h"

#include <seastar/core/coroutine.hh>

namespace cluster_link::replication {

partition_data_queue::partition_data_queue(size_t max_buffered_bytes)
  : _max_buffered_bytes(max_buffered_bytes)
  , _sem(max_buffered_bytes, "partition_data_queue") {}

void partition_data_queue::update_max_buffered(size_t new_value) {
    // ignore update if gate is closed, the queue is stopping
    if (_gate.is_closed()) {
        return;
    }

    if (new_value == _max_buffered_bytes) {
        return;
    }
    if (new_value > _max_buffered_bytes) {
        _sem.signal(new_value - _max_buffered_bytes);

    } else {
        _sem.consume(_max_buffered_bytes - new_value);
    }
    _max_buffered_bytes = new_value;
}
void partition_data_queue::reset(kafka::offset next) {
    _gate.check();
    do_reset(next);
}

void partition_data_queue::do_reset(kafka::offset next) {
    if (_waiter) {
        _waiter->set_exception(ss::abort_requested_exception{});
    }
    _waiter = {};
    _batches.reset();
    _batch_units = {};
    _next = next;
}

ss::future<> partition_data_queue::stop() noexcept {
    auto f = _gate.close();
    do_reset(kafka::offset{});
    co_await std::move(f);
}

bool partition_data_queue::enqueue(
  chunked_vector<model::record_batch> batches) {
    _gate.check();
    if (!_batches) {
        _batches.emplace();
    }
    if (!batches.empty()) [[likely]] {
        _next = kafka::next_offset(
          model::offset_cast(batches.back().last_offset()));
        auto total_bytes = std::accumulate(
          batches.begin(),
          batches.end(),
          0,
          [](size_t acc, const model::record_batch& batch) {
              return acc + batch.size_bytes();
          });
        auto units = ss::consume_units(_sem, total_bytes);
        if (_batch_units.count()) {
            _batch_units.adopt(std::move(units));
        } else {
            _batch_units = std::move(units);
        }
        for (auto& batch : batches) {
            _batches->push_back(std::move(batch));
        }
    }
    maybe_notify_waiter();
    return _sem.available_units() > 0;
}

ss::future<fetch_data> partition_data_queue::fetch(ss::abort_source& as) {
    auto holder = _gate.hold();
    if (_waiter) {
        throw std::runtime_error("Only one fetch can be in progress at a time");
    }
    _waiter.emplace();
    auto waiter_f = _waiter->get_future();
    auto sub = as.subscribe([this]() noexcept {
        if (_waiter) {
            _waiter->set_exception(ss::abort_requested_exception{});
            _waiter = {};
        }
    });
    if (!sub) {
        _waiter->set_exception(ss::abort_requested_exception{});
        _waiter = {};
        co_return co_await std::move(waiter_f);
    }
    maybe_notify_waiter();
    co_return co_await std::move(waiter_f);
}

void partition_data_queue::maybe_notify_waiter() {
    if (!_batches || !_waiter.has_value()) {
        return;
    }
    auto batches = std::exchange(_batches, std::nullopt);
    _waiter->set_value(
      fetch_data{
        .batches = std::move(*batches),
        .units = std::exchange(_batch_units, {})});
    _waiter = {};
}
} // namespace cluster_link::replication
