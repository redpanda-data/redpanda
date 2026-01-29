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

#include "base/seastarx.h"
#include "model/record.h"

#include <memory>

namespace cloud_topics::l0 {

class producer_ticket;

// A producer queue is a linearization point for producers within cloud topics.
//
// In cloud topics we accept requests in order to the batcher from the kafka
// layer, and post upload we also need to release requests into raft in the same
// order we recieved them. However we don't want to limit in flight requests to
// one request per partition, we want to allow multiple per producer. So the
// producer queue is the mechanism to enforce this ordering.
//
// Expected usage looks like this:
//
// ```
// ss::promise<> enqueued_promise = ...;
// ss::promise<> finished_promise = ...;
// auto ticket = producer_queue.reserve(my_pid);
// enqueued_promise.set_value();
// co_await write_and_debounce(...);
// co_await ticket.redeem(); // wait for previous replicate calls
// auto stages = partition->replicate(...);
// // the next request/ticket can be redeemed after the request is in raft
// stages.request_enqueued.then([&ticket] { ticket.release(); });
// stages.request_finished.forward_to(finished_promise);
// ```
//
// NOTE: This class' memory usage is bounded by the concurrency of in flight
// requests and should scale ~linearly with the number of concurrent produce
// requests.
class producer_queue {
public:
    producer_queue();
    ~producer_queue();
    producer_queue(const producer_queue&) = delete;
    producer_queue(producer_queue&&) noexcept;
    producer_queue& operator=(const producer_queue&) = delete;
    producer_queue& operator=(producer_queue&&) noexcept;

    // Reserve a position within the queue for a given producer.
    // The ticket can be redeemed later on to wait for previous tickets to be
    // released. This creates a queuing effect per producer id.
    //
    // NOTE: If the producer id is model::no_producer_id, then this ticket
    // is basically a noop and doesn't wait or preserve any ordering.
    producer_ticket reserve(model::producer_id);

    /// Returns the number of producers currently tracked in the queue.
    /// Used for testing to verify proper cleanup.
    size_t size() const;

private:
    class impl;
    std::unique_ptr<impl> _impl;
};

// A ticket within the producer queue.
class producer_ticket {
public:
    class impl;

    producer_ticket();
    ~producer_ticket();
    producer_ticket(const producer_ticket&) = delete;
    producer_ticket(producer_ticket&&) noexcept;
    producer_ticket& operator=(const producer_ticket&) = delete;
    producer_ticket& operator=(producer_ticket&&) noexcept;

    // Redeem the ticket for usage, this waits for all previous tickets to be
    // released.
    ss::future<> redeem();
    ss::future<> redeem(ss::abort_source&);

    // Release the ticket so that other later tickets in the queue can proceed.
    //
    // Can be called concurrently with `redeem`, which will cause it to return
    // immediately.
    void release();

private:
    explicit producer_ticket(std::unique_ptr<impl> impl);
    friend class producer_queue;

    std::unique_ptr<impl> _impl;
};

} // namespace cloud_topics::l0
