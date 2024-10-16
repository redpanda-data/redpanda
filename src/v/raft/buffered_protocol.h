/**
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/property.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "raft/consensus_client_protocol.h"
#include "raft/fundamental.h"
#include "raft/types.h"
#include "ssx/semaphore.h"
#include "utils/prefix_logger.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <absl/container/flat_hash_map.h>
#include <rpc/types.h>

namespace raft {

namespace internal {
struct append_entries_queue {
public:
    explicit append_entries_queue(
      model::node_id,
      consensus_client_protocol,
      ss::gate::holder,
      config::binding<size_t> max_inflight_requests,
      config::binding<size_t> max_buffered_bytes);
    append_entries_queue(append_entries_queue&&) = delete;
    append_entries_queue(const append_entries_queue&) = delete;
    append_entries_queue& operator=(const append_entries_queue&) = delete;
    append_entries_queue& operator=(append_entries_queue&&) = delete;
    ~append_entries_queue() noexcept = default;

    ss::future<result<append_entries_reply>>
      append_entries(append_entries_request, rpc::client_opts);

    ss::future<> stop();

    size_t inflight_requests() const {
        return _current_max_inflight_requests
               - _inflight_requests_sem.available_units();
    };
    bool is_idle() const;
    void log_status() const;

private:
    ss::future<> dispatch_loop();

    struct request_entry {
        request_entry(append_entries_request request, rpc::client_opts opts)
          : request(std::move(request))
          , opts(std::move(opts)) {}

        append_entries_request request;
        rpc::client_opts opts;
        ss::promise<result<append_entries_reply>> reply;
    };

    bool can_buffer_next_request(size_t) const;

    ss::future<> do_dispatch(request_entry entry, ssx::semaphore_units units);

    void setup_internal_metrics();
    void setup_public_metrics();

    model::node_id _target_node;
    consensus_client_protocol _base_protocol;
    clock_type::time_point _last_sent_timestamp;
    clock_type::time_point _last_reply_timestamp;
    log_hist_internal _hist;
    prefix_logger _logger;

    ss::chunked_fifo<request_entry> _requests;
    ss::gate _gate;
    ss::condition_variable _new_requests;
    ss::condition_variable _dispatched;

    size_t _current_max_inflight_requests;
    config::binding<size_t> _max_inflight_requests;
    config::binding<size_t> _max_buffered_bytes;

    ssx::named_semaphore<> _inflight_requests_sem;
    size_t _buffered_bytes{0};
    metrics::public_metric_groups _public_metrics;
    metrics::internal_metric_groups _internal_metrics;
};

} // namespace internal

/**
 * Buffered protocol is a wrapper around the standard
 * `consensus_client_protocol` providing buffering and basic back-pressure
 * propagation.
 * The buffering is done at the shard level (all append entries requests sent to
 * the same follower node passes through the buffered protocol buffer queue).
 * This makes it possible for the protocol to better amortize follower disk
 * throughput and latency irregularities.
 *
 * Buffering can be controlled by adjusting the max_buffered_bytes property.
 * Another configuration property allows to change the number of inflight
 * requests that are pending to be replied by the follower.
 */
class buffered_protocol : public consensus_client_protocol::impl {
public:
    /**
     * Creates the buffered protocol wrapping a base client protocol. The
     * append_entries_requests will first be enqueued in the buffered protocol
     * internals up to max_buffered_bytes and then dispatched to the target
     * node, keeping up to max_inflight concurrent requests.
     */
    buffered_protocol(
      consensus_client_protocol base,
      config::binding<size_t> max_inflight_requests,
      config::binding<size_t> max_buffered_bytes);

    ss::future<result<vote_reply>>
      vote(model::node_id, vote_request, rpc::client_opts) final;

    ss::future<result<append_entries_reply>> append_entries(
      model::node_id, append_entries_request, rpc::client_opts) final;

    ss::future<result<heartbeat_reply>>
      heartbeat(model::node_id, heartbeat_request, rpc::client_opts) final;

    ss::future<result<heartbeat_reply_v2>> heartbeat_v2(
      model::node_id, heartbeat_request_v2, rpc::client_opts) final;

    ss::future<result<install_snapshot_reply>> install_snapshot(
      model::node_id, install_snapshot_request, rpc::client_opts) final;

    ss::future<result<timeout_now_reply>>
      timeout_now(model::node_id, timeout_now_request, rpc::client_opts) final;
    ss::future<bool> ensure_disconnect(model::node_id) final;

    ss::future<result<transfer_leadership_reply>> transfer_leadership(
      model::node_id, transfer_leadership_request, rpc::client_opts) final;

    ss::future<> reset_backoff(model::node_id n) final;

    ss::future<> stop();

private:
    void garbage_collect_unused_queues();

    /// Number of cluster members is usually rather small we may use an
    /// efficient flat_hash_map here.
    absl::flat_hash_map<
      model::node_id,
      std::unique_ptr<internal::append_entries_queue>>
      _append_entries_queues;

    consensus_client_protocol _base_protocol;
    config::binding<size_t> _max_inflight_requests;
    config::binding<size_t> _max_buffered_bytes;
    ss::gate _gate;
    ss::timer<> _gc_timer;
};
} // namespace raft
