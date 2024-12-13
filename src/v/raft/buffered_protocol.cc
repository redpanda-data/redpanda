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

#include "raft/buffered_protocol.h"

#include "base/vlog.h"
#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"
#include "model/fundamental.h"
#include "raft/consensus_client_protocol.h"
#include "raft/errc.h"
#include "raft/fundamental.h"
#include "raft/logger.h"
#include "raft/types.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"

#include <seastar/core/metrics.hh>

#include <cstdint>

namespace raft {

namespace {
template<typename Func, typename Ret>
ss::future<result<Ret>> try_with_gate(ss::gate& gate, Func&& f) {
    if (gate.is_closed()) {
        return ss::make_ready_future<result<Ret>>(raft::errc::shutting_down);
    }
    return ss::with_gate(gate, std::forward<Func>(f));
}

template<typename Req, typename Resp>
using client_f = ss::future<result<Resp>> (consensus_client_protocol::*)(
  model::node_id, Req, rpc::client_opts);

template<typename Req, typename Ret>
ss::future<result<Ret>> apply_with_gate(
  ss::gate& gate,
  consensus_client_protocol& proto,
  model::node_id target_node,
  Req req,
  rpc::client_opts opts,
  client_f<Req, Ret> f) {
    if (gate.is_closed()) {
        return ss::make_ready_future<result<Ret>>(raft::errc::shutting_down);
    }

    return ss::with_gate(
      gate,
      [f,
       target_node,
       &proto,
       req = std::move(req),
       opts = std::move(opts)]() mutable {
          return std::apply(
            f,
            std::forward_as_tuple(
              proto, target_node, std::move(req), std::move(opts)));
      });
}

} // namespace

buffered_protocol::buffered_protocol(
  consensus_client_protocol base,
  config::binding<size_t> max_inflight_requests,
  config::binding<size_t> max_buffered_bytes)
  : _base_protocol(std::move(base))
  , _max_inflight_requests(std::move(max_inflight_requests))
  , _max_buffered_bytes(std::move(max_buffered_bytes))
  , _gc_timer([this] { garbage_collect_unused_queues(); }) {
    // the timer interval doesn't have to be configurable, it would be
    // additional configuration parameter, the queues doesn't change too often
    _gc_timer.arm_periodic(10s);
}

ss::future<result<vote_reply>> buffered_protocol::vote(
  model::node_id target_node, vote_request req, rpc::client_opts opts) {
    return apply_with_gate(
      _gate,
      _base_protocol,
      target_node,
      std::move(req),
      std::move(opts),
      &consensus_client_protocol::vote);
}

ss::future<result<append_entries_reply>> buffered_protocol::append_entries(
  model::node_id target_node,
  append_entries_request req,
  rpc::client_opts opts) {
    return try_with_gate(
      _gate,
      [this,
       target_node,
       req = std::move(req),
       opts = std::move(opts)]() mutable {
          auto it = _append_entries_queues.find(target_node);
          if (it == _append_entries_queues.end()) [[unlikely]] {
              it = _append_entries_queues.emplace_hint(
                it,
                target_node,
                std::make_unique<internal::append_entries_queue>(
                  target_node,
                  _base_protocol,
                  _gate.hold(),
                  _max_inflight_requests,
                  _max_buffered_bytes));
          }
          return it->second->append_entries(std::move(req), std::move(opts));
      });
};

ss::future<result<heartbeat_reply>> buffered_protocol::heartbeat(
  model::node_id target_node, heartbeat_request req, rpc::client_opts opts) {
    return apply_with_gate(
      _gate,
      _base_protocol,
      target_node,
      std::move(req),
      std::move(opts),
      &consensus_client_protocol::heartbeat);
}

ss::future<result<heartbeat_reply_v2>> buffered_protocol::heartbeat_v2(
  model::node_id target_node, heartbeat_request_v2 req, rpc::client_opts opts) {
    return apply_with_gate(
      _gate,
      _base_protocol,
      target_node,
      std::move(req),
      std::move(opts),
      &consensus_client_protocol::heartbeat_v2);
}

ss::future<result<install_snapshot_reply>> buffered_protocol::install_snapshot(
  model::node_id target_node,
  install_snapshot_request req,
  rpc::client_opts opts) {
    return apply_with_gate(
      _gate,
      _base_protocol,
      target_node,
      std::move(req),
      std::move(opts),
      &consensus_client_protocol::install_snapshot);
}

ss::future<result<timeout_now_reply>> buffered_protocol::timeout_now(
  model::node_id target_node, timeout_now_request req, rpc::client_opts opts) {
    return apply_with_gate(
      _gate,
      _base_protocol,
      target_node,
      std::move(req),
      std::move(opts),
      &consensus_client_protocol::timeout_now);
}

ss::future<result<transfer_leadership_reply>>
buffered_protocol::transfer_leadership(
  model::node_id target_node,
  transfer_leadership_request req,
  rpc::client_opts opts) {
    return apply_with_gate(
      _gate,
      _base_protocol,
      target_node,
      std::move(req),
      std::move(opts),
      &consensus_client_protocol::transfer_leadership);
}

ss::future<bool> buffered_protocol::ensure_disconnect(model::node_id node_id) {
    return _base_protocol.ensure_disconnect(node_id);
}

ss::future<> buffered_protocol::reset_backoff(model::node_id node_id) {
    return _base_protocol.reset_backoff(node_id);
}

ss::future<> buffered_protocol::stop() {
    vlog(raftlog.debug, "stopping buffered protocol");
    _gc_timer.cancel();
    auto f = _gate.close();
    co_await ss::parallel_for_each(
      _append_entries_queues,
      [](decltype(_append_entries_queues)::value_type& queue) {
          return queue.second->stop();
      });
    _append_entries_queues.clear();
    co_await std::move(f);
    vlog(raftlog.debug, "stopped buffered protocol");
}

void buffered_protocol::garbage_collect_unused_queues() {
    for (auto it = _append_entries_queues.begin(),
              last = _append_entries_queues.end();
         it != last;) {
        it->second->log_status();
        if (it->second->is_idle()) {
            std::unique_ptr<internal::append_entries_queue> queue;
            queue.swap(it->second);
            vlog(
              raftlog.info,
              "removing idle append entries queue for node {}",
              it->first);
            ssx::spawn_with_gate(_gate, [q = std::move(queue)]() mutable {
                auto ptr = q.get();
                return ptr->stop().finally([q = std::move(q)] {});
            });
            _append_entries_queues.erase(it++);
        } else {
            ++it;
        }
    }
}

namespace internal {
append_entries_queue::append_entries_queue(
  model::node_id target_node,
  consensus_client_protocol base_protocol,
  ss::gate::holder gate_holder,
  config::binding<size_t> max_inflight_requests,
  config::binding<size_t> max_buffered_bytes)
  : _target_node(target_node)
  , _base_protocol(std::move(base_protocol))
  , _logger(raftlog, fmt::format("[node: {}]", _target_node))
  , _current_max_inflight_requests(max_inflight_requests())
  , _max_inflight_requests(std::move(max_inflight_requests))
  , _max_buffered_bytes(std::move(max_buffered_bytes))
  , _inflight_requests_sem(
      _max_inflight_requests(), "inflight-append-entries") {
    // handler for property updates
    _max_inflight_requests.watch([this] {
        size_t new_value = _max_inflight_requests();

        auto diff = static_cast<int64_t>(new_value)
                    - static_cast<int64_t>(_current_max_inflight_requests);
        if (diff < 0) {
            _inflight_requests_sem.consume(-diff);
        } else if (diff > 0) {
            _inflight_requests_sem.signal(diff);
        }
        _current_max_inflight_requests = new_value;
    });
    setup_internal_metrics();
    // start dispatch loop
    ssx::repeat_until_gate_closed(
      _gate,
      [this, gate_holder = std::move(gate_holder)] { return dispatch_loop(); });
};

ss::future<> append_entries_queue::dispatch_loop() {
    return _new_requests.wait([this] { return !_requests.empty(); })
      .then([this] {
          return ss::get_units(_inflight_requests_sem, 1)
            .then([this](ssx::semaphore_units inflight_units) {
                ssx::spawn_with_gate(
                  _gate,
                  [this, inflight_units = std::move(inflight_units)]() mutable {
                      auto msg_entry = std::move(_requests.front());
                      _requests.pop_front();
                      _buffered_bytes -= msg_entry.request.total_size();
                      auto f = do_dispatch(
                        std::move(msg_entry), std::move(inflight_units));
                      _dispatched.signal();
                      return f;
                  });
            });
      });
}
ss::future<> append_entries_queue::do_dispatch(
  request_entry entry, ssx::semaphore_units inflight_units) {
    auto sent_ts = clock_type::now();
    _last_sent_timestamp = sent_ts;
    return _base_protocol
      .append_entries(
        _target_node, std::move(entry.request), std::move(entry.opts))
      .then_wrapped(
        [this,
         inflight_units = std::move(inflight_units),
         reply_promise = std::move(entry.reply),
         sent_ts](ss::future<result<append_entries_reply>> reply_f) mutable {
            auto now = clock_type::now();
            const auto request_latency = now - sent_ts;
            _last_reply_timestamp = now;
            _hist.record(request_latency);
            reply_f.forward_to(std::move(reply_promise));
        });
}

bool append_entries_queue::can_buffer_next_request(size_t size) const {
    return _requests.empty()
           || (_buffered_bytes + size) < _max_buffered_bytes();
}
bool append_entries_queue::is_idle() const {
    static constexpr auto queue_idle_timeout = 30s;
    return _requests.empty() && inflight_requests() == 0
           && _last_sent_timestamp < clock_type::now() - queue_idle_timeout;
}

ss::future<result<append_entries_reply>> append_entries_queue::append_entries(
  append_entries_request r, rpc::client_opts opts) {
    // or wait until we can buffer next request, this will propagate back
    // pressure to caller
    auto sz = r.total_size();
    return _dispatched.wait([this, sz] { return can_buffer_next_request(sz); })
      .then([this, r = std::move(r), opts = std::move(opts)]() mutable {
          /// consensus is no longer responsible for tracking memory usage and
          /// dispatch ordering after this point
          opts.resource_units.reset();
          _buffered_bytes += r.total_size();
          _requests.emplace_back(std::move(r), std::move(opts));

          _new_requests.signal();
          return _requests.back().reply.get_future();
      });
}
ss::future<> append_entries_queue::stop() {
    vlog(_logger.debug, "stopping append entries queue");
    _new_requests.broken();
    _dispatched.broken();
    _inflight_requests_sem.broken();
    for (auto& r : _requests) {
        r.reply.set_value(raft::errc::shutting_down);
    }
    _requests.clear();
    _public_metrics.clear();
    _internal_metrics.clear();
    return _gate.close().finally(
      [this] { vlog(_logger.debug, "stopped queue"); });
}

void append_entries_queue::log_status() const {
    vlog(
      _logger.info,
      "inflight requests: {}, buffered requests: {}, buffered_bytes: {}",
      inflight_requests(),
      _requests.size(),
      _buffered_bytes);
}

void append_entries_queue::setup_internal_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    sm::label_instance target_node_id_label("target_node_id", _target_node);
    _internal_metrics.add_group(
      prometheus_sanitize::metrics_name("raft:buffered:protocol"),
      {sm::make_gauge(
         "inflight_requests",
         [this] { return inflight_requests(); },
         sm::description("Number of append entries requests that were sent to "
                         "the target node and are awaiting responses."),
         {target_node_id_label}),
       sm::make_gauge(
         "buffered_bytes",
         [this] { return _buffered_bytes; },
         sm::description("Total size of append entries requests in the queue"),
         {target_node_id_label}),
       sm::make_gauge(
         "buffered_requests",
         [this] { return _requests.size(); },
         sm::description(
           "Total number of append entries requests in the queue"),
         {target_node_id_label})});
}

void append_entries_queue::setup_public_metrics() {
    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }
    // TODO: decide which metrics should be public
}

} // namespace internal

} // namespace raft
