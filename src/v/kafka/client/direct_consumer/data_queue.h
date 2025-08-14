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
#include "kafka/client/direct_consumer/api_types.h"

#include <seastar/core/condition-variable.hh>

namespace kafka::client {
/**
 * Very simple blocking queue that is used in
 * the direct consumer to store data fetched from the broker. The queue is
 * bounded by the number of entries and the total size of the data in the queue.
 * The queue can store a single entry that exceeds the size limit but the
 * prerequisite for that is that the queue is empty.
 */
class data_queue {
public:
    data_queue(size_t max_bytes = 10_MiB, size_t max_count = 10);
    /*
     * Returns true if the queue can accept more data.
     */
    bool can_insert(size_t bytes) const;
    /**
     * Pushes data into the queue. If the queue is full, it will block until
     * there is enough space.
     */
    ss::future<>
    push(chunked_vector<fetched_topic_data> data, size_t total_bytes);

    /**
     * Pushes an error into the queue. If the queue is full, it will block until
     * there is enough space. The error are not accounted for the total bytes.
     */
    ss::future<> push_error(kafka::error_code ec);
    /**
     * Pops data from the queue. If the queue is empty, it will block until
     * there is data available or the timeout is reached.
     * If the timeout is reached, it will throw condition_variable_timed_out.
     */
    ss::future<fetches> pop(std::chrono::milliseconds timeout);

    /**
     * Stops the queue. All methods will return immediately after this
     * method is called. If the methods is waiting it will throw an exception.
     */
    void stop();

    /**
     * Configuration setters for the queue.
     */
    void set_max_bytes(size_t bytes) {
        _max_bytes = bytes;
        _updated.signal();
    }
    void set_max_count(size_t count) {
        _max_count = count;
        _updated.signal();
    }

    size_t size() const { return _queue.size(); }

    size_t current_bytes() const { return _current_bytes; }

private:
    struct entry {
        kafka::error_code error_code{kafka::error_code::none};
        chunked_vector<fetched_topic_data> topics;
        size_t total_bytes{0};
    };
    size_t _max_count{10};
    size_t _max_bytes{10_MiB};

    size_t _current_bytes{0};
    ss::chunked_fifo<entry> _queue;
    ss::condition_variable _updated;
};
} // namespace kafka::client
