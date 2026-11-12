/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/batcher/secondary_fanout.h"

#include "base/vlog.h"
#include "cloud_io/io_result.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>

namespace cloud_topics::l0 {

template<class Clock>
secondary_fanout<Clock>::secondary_fanout(
  cloud_io::remote_api<Clock>& remote,
  cloud_storage_clients::bucket_name bucket)
  : _remote(remote)
  , _bucket(std::move(bucket))
  , _rtc(_as) {}

template<class Clock>
void secondary_fanout<Clock>::setup_metrics() {
    namespace sm = ss::metrics;
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics:secondary_fanout"),
      {
        sm::make_counter(
          "uploads_total",
          [this] { return _uploads; },
          sm::description(
            "L0 objects uploaded to the cross-cloud secondary by the fan-out")),
        sm::make_counter(
          "errors_total",
          [this] { return _errors; },
          sm::description("L0 fan-out uploads to the secondary that failed")),
        sm::make_counter(
          "dropped_total",
          [this] { return _dropped; },
          sm::description(
            "L0 objects dropped from the fan-out queue on overflow "
            "(drop-oldest); the secondary is missing them")),
        sm::make_gauge(
          "queue_objects",
          [this] { return _queue.size(); },
          sm::description("L0 fan-out queue depth in objects (backlog)")),
        sm::make_gauge(
          "queue_bytes",
          [this] { return _queue_bytes; },
          sm::description("L0 fan-out queue depth in bytes (backlog)")),
      });
}

template<class Clock>
ss::future<> secondary_fanout<Clock>::start() {
    setup_metrics();
    ssx::spawn_with_gate(_gate, [this] { return drain_loop(); });
    co_return;
}

template<class Clock>
ss::future<> secondary_fanout<Clock>::stop() {
    vlog(
      cd_log.info,
      "secondary_fanout stopping: uploads={} errors={} dropped={} pending={}",
      _uploads,
      _errors,
      _dropped,
      _queue.size());
    _as.request_abort();
    _cv.broken();
    co_await _gate.close();
}

template<class Clock>
void secondary_fanout<Clock>::enqueue(
  cloud_storage_clients::object_key key, iobuf payload) {
    if (_as.abort_requested()) {
        return;
    }
    while (
      !_queue.empty()
      && (_queue.size() >= max_queue_objects || _queue_bytes + payload.size_bytes() > max_queue_bytes)) {
        _queue_bytes -= _queue.front().payload.size_bytes();
        _queue.pop_front();
        ++_dropped;
    }
    _queue_bytes += payload.size_bytes();
    _queue.push_back(
      pending{.key = std::move(key), .payload = std::move(payload)});
    _cv.signal();
}

template<class Clock>
ss::future<bool> secondary_fanout<Clock>::put(
  cloud_storage_clients::object_key key, iobuf payload) {
    try {
        basic_retry_chain_node<Clock> fib(
          Clock::now() + std::chrono::seconds(30),
          std::chrono::milliseconds(100),
          retry_strategy::backoff,
          &_rtc);
        cloud_io::basic_transfer_details<Clock> td{
          .bucket = _bucket,
          .key = std::move(key),
          .parent_rtc = fib,
        };
        auto result = co_await _remote.upload_object(
          {
            .transfer_details = std::move(td),
            .display_str = "L0_object_secondary",
            .payload = std::move(payload),
          },
          cloud_io::group_id::producer_upload);
        co_return result == cloud_io::upload_result::success;
    } catch (...) {
        auto eptr = std::current_exception();
        if (ssx::is_shutdown_exception(eptr)) {
            co_return false;
        }
        vlog(cd_log.warn, "secondary_fanout: put failed: {}", eptr);
        co_return false;
    }
}

template<class Clock>
ss::future<> secondary_fanout<Clock>::drain_loop() {
    while (!_as.abort_requested()) {
        try {
            co_await _cv.wait(
              [this] { return _as.abort_requested() || !_queue.empty(); });
        } catch (const ss::broken_condition_variable&) {
            break;
        }
        if (_as.abort_requested()) {
            break;
        }
        auto item = std::move(_queue.front());
        _queue.pop_front();
        _queue_bytes -= item.payload.size_bytes();
        auto payload = item.payload.share(0, item.payload.size_bytes());
        auto ok = co_await put(item.key, std::move(payload));
        if (ok) {
            ++_uploads;
            if (_uploads % 100 == 1) {
                vlog(
                  cd_log.info,
                  "secondary_fanout: {} objects replicated to {} ({} errors, "
                  "{} dropped, {} pending)",
                  _uploads,
                  _bucket,
                  _errors,
                  _dropped,
                  _queue.size());
            }
        } else {
            ++_errors;
            // one bounded retry round-trip later: requeue at the BACK so a
            // dead secondary cannot head-of-line block fresher objects, and
            // the overflow policy above bounds total memory.
            enqueue(std::move(item.key), std::move(item.payload));
            co_await ss::sleep_abortable<Clock>(
              std::chrono::milliseconds(500), _as)
              .handle_exception_type([](const ss::sleep_aborted&) {});
        }
    }
}

template class secondary_fanout<ss::lowres_clock>;
template class secondary_fanout<ss::manual_clock>;

} // namespace cloud_topics::l0
