/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "cloud_io/remote_api.h"
#include "cloud_storage_clients/types.h"
#include "metrics/metrics.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

#include <deque>

namespace cloud_topics::l0 {

/// Asynchronously replicates successfully uploaded L0 objects to a secondary
/// bucket that may live on a DIFFERENT cloud provider (the remote it is given
/// carries its own client pool and credentials). The fan-out never
/// contributes to the ack path: enqueue() is called after the primary upload
/// succeeded, the queue is bounded, and overflow drops the oldest pending
/// object (counted) rather than exerting backpressure on producers.
template<class Clock = ss::lowres_clock>
class secondary_fanout {
public:
    secondary_fanout(
      cloud_io::remote_api<Clock>& remote,
      cloud_storage_clients::bucket_name bucket);

    secondary_fanout(const secondary_fanout&) = delete;
    secondary_fanout& operator=(const secondary_fanout&) = delete;

    ss::future<> start();
    ss::future<> stop();

    /// Queue one object for secondary upload. Never blocks.
    void enqueue(cloud_storage_clients::object_key key, iobuf payload);

    uint64_t uploads() const { return _uploads; }
    uint64_t errors() const { return _errors; }
    uint64_t dropped() const { return _dropped; }
    size_t queue_depth() const { return _queue.size(); }

private:
    ss::future<> drain_loop();
    ss::future<bool> put(cloud_storage_clients::object_key key, iobuf payload);
    void setup_metrics();

    static constexpr size_t max_queue_objects = 256;
    static constexpr size_t max_queue_bytes = 512 * 1024 * 1024;

    struct pending {
        cloud_storage_clients::object_key key;
        iobuf payload;
    };

    cloud_io::remote_api<Clock>& _remote;
    cloud_storage_clients::bucket_name _bucket;
    std::deque<pending> _queue;
    size_t _queue_bytes{0};
    ss::condition_variable _cv;
    ss::abort_source _as;
    ss::gate _gate;
    basic_retry_chain_node<Clock> _rtc;

    uint64_t _uploads{0};
    uint64_t _errors{0};
    uint64_t _dropped{0};
    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::l0
