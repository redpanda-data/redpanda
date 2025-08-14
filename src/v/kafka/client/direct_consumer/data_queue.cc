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
#include "kafka/client/direct_consumer/data_queue.h"

namespace kafka::client {

data_queue::data_queue(size_t max_bytes, size_t max_count)
  : _max_count(max_count)
  , _max_bytes(max_bytes) {}

bool data_queue::can_insert(size_t bytes) const {
    return _queue.empty()
           || (_current_bytes + bytes <= _max_bytes && _queue.size() < _max_count);
}

ss::future<>
data_queue::push(chunked_vector<fetched_topic_data> data, size_t total_bytes) {
    // optimization, if data can be inserted immediately, do it
    if (can_insert(total_bytes)) {
        _current_bytes += total_bytes;
        _queue.push_back(
          entry{.topics = std::move(data), .total_bytes = total_bytes});
        _updated.signal();
        return ss::now();
    }
    return _updated
      .wait([this, total_bytes] { return can_insert(total_bytes); })
      .then([total_bytes, data = std::move(data), this]() mutable {
          _current_bytes += total_bytes;
          _queue.push_back(
            entry{.topics = std::move(data), .total_bytes = total_bytes});
          _updated.signal();
      });
}

ss::future<> data_queue::push_error(kafka::error_code ec) {
    return _updated.wait([this] { return can_insert(0); })
      .then([ec, this]() mutable {
          _queue.push_back(entry{.error_code = ec});
          _updated.signal();
      });
}

ss::future<fetches> data_queue::pop(std::chrono::milliseconds timeout) {
    co_await _updated.wait(timeout, [this] { return !_queue.empty(); });

    auto entry = std::move(_queue.front());
    _current_bytes -= entry.total_bytes;
    _queue.pop_front();
    _updated.signal();
    if (entry.error_code != kafka::error_code::none) {
        // If there is an error, we return an empty vector
        co_return entry.error_code;
    }
    auto result = std::move(entry.topics);
    co_return result;
}

void data_queue::stop() { _updated.broken(); }

} // namespace kafka::client
