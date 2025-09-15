/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "container/chunked_vector.h"
#include "model/record.h"
#include "ssx/semaphore.h"

#include <seastar/core/gate.hh>

namespace cluster_link::replication {

/**
 * @brief A queue for managing partition data with memory-bounded buffering.
 *
 * Partition specific data queue that buffers incoming data batches from remote
 * source. This queue is decoupled from the direct consumer fetch queue so a
 * stuck producer will not block the consumer.
 *
 * Notes:
 * - Memory-bounded: Uses a semaphore to track and limit memory usage
 * - Non-blocking enqueue: Always accepts data but signals when memory limits
 * are exceeded
 * - Single consumer: Only supports one fetch operation at a time
 * - Offset tracking: Maintains the next expected offset for proper ordering
 */
class partition_data_queue {
public:
    explicit partition_data_queue(size_t max_buffered_bytes);
    struct fetch_data {
        chunked_vector<::model::record_batch> batches;
        ssx::semaphore_units units;
    };
    ss::future<> stop() noexcept;
    // note: enqueue never fails, it will oversubscribe memory but the return
    // value is a hint to the caller to not enqueue more until the queue is
    // cleared.
    // returns true if the queue has more memory available for further enqueues,
    // false is the queue is full.
    bool enqueue(chunked_vector<::model::record_batch>);
    ss::future<fetch_data> fetch(ss::abort_source&);
    // Resets consumption to the provided offset. Any inflight fetches are
    // aborted (with ss::abort_requested_exception) and the buffered data is
    // cleared. Fetch operations after reset are guaranteed to see data from the
    // new offset.
    void reset(kafka::offset next);
    kafka::offset next() const { return _next; }
    bool empty() const { return _batches.empty(); }
    bool full() const { return _sem.available_units() <= 0; }

private:
    void do_reset(kafka::offset next);
    kafka::offset _next{};
    void maybe_notify_waiter();
    std::optional<ss::promise<fetch_data>> _waiter;
    ssx::semaphore _sem;
    ssx::semaphore_units _batch_units;
    chunked_vector<::model::record_batch> _batches;
    ss::gate _gate;
};
} // namespace cluster_link::replication
