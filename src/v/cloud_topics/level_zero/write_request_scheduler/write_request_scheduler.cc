/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/write_request_scheduler/write_request_scheduler.h"

#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/pipeline/base_pipeline.h"
#include "cloud_topics/level_zero/pipeline/write_pipeline.h"
#include "cloud_topics/level_zero/pipeline/write_request.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "ssx/future-util.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <limits>

namespace cloud_topics::l0 {

namespace {
// Copy extents and share the payload to use on another shard
serialized_chunk shallow_copy(serialized_chunk& chunk) {
    serialized_chunk copy;
    copy.extents = chunk.extents.copy();
    copy.payload = chunk.payload.share(0, chunk.payload.size_bytes());
    return copy;
}
} // namespace

template<typename Clock>
write_request_scheduler<Clock>::write_request_scheduler(
  write_pipeline<Clock>::stage s)
  : _stage(s)
  , _max_buffer_size(
      config::shard_local_cfg()
        .cloud_topics_produce_batching_size_threshold.bind())
  , _max_cardinality(
      config::shard_local_cfg()
        .cloud_topics_produce_cardinality_threshold.bind())
  , _scheduling_interval(
      config::shard_local_cfg().cloud_topics_produce_upload_interval.bind())
  , _probe(config::shard_local_cfg().disable_metrics()) {}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::start() {
    // Start the background time based fallback loop on shard 0
    if (
      ss::this_shard_id() == ss::shard_id(0)
      && !_test_only_disable_time_based_fallback) {
        ssx::spawn_with_gate(
          _gate, [this] { return bg_time_based_fallback(); });
    }
    if (!_test_only_disable_data_threshold) {
        ssx::spawn_with_gate(_gate, [this] { return bg_data_threshold(); });
    }
    co_return;
}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::stop() {
    _as.request_abort();
    co_await _gate.close();
}

template<typename Clock>
size_t write_request_scheduler<Clock>::shard_bytes() noexcept {
    // Count bytes but take limits into account. We intentionally don't use
    // stage_bytes() here because we need to respect _max_cardinality and
    // _max_buffer_size limits.
    size_t total_bytes = 0;
    size_t total_requests = 0;
    _stage.process(
      [&total_bytes, &total_requests, this](
        const l0::write_request<Clock>& req) noexcept
        -> std::expected<l0::request_processing_result, errc> {
          total_bytes += req.size_bytes();
          total_requests++;
          return total_requests < _max_cardinality()
                     && total_bytes < _max_buffer_size()
                   ? request_processing_result::ignore_and_continue
                   : request_processing_result::ignore_and_stop;
      });
    return total_bytes;
}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::bg_data_threshold() {
    vassert(
      !_test_only_disable_data_threshold, "Data threshold is not disabled");
    while (!_as.abort_requested()) {
        auto req = co_await _stage.wait_until(
          _max_buffer_size(), Clock::time_point::max(), &_as);
        if (!req.has_value()) {
            if (req.error() == errc::shutting_down) {
                vlog(cd_log.debug, "bg_data_threshold: shutting down");
                co_return;
            } else {
                vlog(
                  cd_log.error,
                  "bg_data_threshold: error waiting for write requests: {}",
                  req.error());
            }
            co_return;
        }
        vlog(
          cd_log.debug,
          "bg_data_threshold: pending bytes: {}, total bytes: "
          "{}",
          req.value().pending_write_bytes,
          req.value().total_write_bytes);

        // Propagate to the next stage
        _stage.process(
          [this](const l0::write_request<Clock>& r) noexcept
            -> std::expected<l0::request_processing_result, errc> {
              _probe.register_data_threshold(r.size_bytes());
              return l0::request_processing_result::advance_and_continue;
          });
    }
}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::pull_and_roundtrip(
  std::vector<shard_info> infos) {
    // This method is invoked by the target shard to pull requests from
    // other shards and forward them to its own pipeline.
    std::vector<ss::future<>> in_flight;
    // 1. Schedule all other shards to forward their requests.
    // 2. Process own requests to trigger the signal.
    ss::gate target_gate;
    for (const auto& info : infos) {
        if (ss::this_shard_id() != info.shard) {
            // Forward requests to target shard by proxying
            // write requests
            if (info.bytes > 0) {
                // This is not conventional use of the gate. The unique_ptr is
                // used as a RAII container to pass this gate holder into the
                // 'roundtrip' method. The 'roundtrip' method will be invoked on
                // another shard. It guarantees that the gate holder will not be
                // destroyed while it's running on another shard. To make it
                // safe the pointer is wrapped with the foreign_ptr. Even if the
                // exception will be thrown while on another shard the gate
                // holder will be disposed on the target shard.
                //
                // The 'roundtrip' method will submit the continuation back to
                // the target shard that owns the gate and will eventually
                // destroy the holder. The target gate will be kept open until
                // all shards finish forwarding their requests. The foreign_ptr
                // has zero overhead when disposed on the original shard. It's
                // just a safety measure, not a correctness requirement.
                auto ptr = ss::make_foreign(
                  std::make_unique<ss::gate::holder>(target_gate.hold()));
                in_flight.push_back(this->container().invoke_on(
                  info.shard,
                  [target = ss::this_shard_id(), ptr = std::move(ptr)](
                    write_request_scheduler<Clock>& s) mutable {
                      return s.forward_to(target, std::move(ptr));
                  }));
            }
        }
    }
    // Wait until all shards have scheduled their requests.
    // This gate is only closed after every shard has forwarded all its
    // requests to the target shard's pipeline. This means that we can
    // notify the next pipeline stage about the requests and await futures
    // accumulated in the 'in_flight' vector. Instead of simply signalling
    // the gate we first forwarding target shard's own requests to the pipeline
    // which also signals the pipeline stage.
    // After the gate is closed the method awaits in-flight futures. Every
    // future awaits until the target shard uploads its write requests to the
    // cloud storage and then it acknowledges the source write requests (which
    // were proxied).
    co_await target_gate.close();
    // Submit own requests to the pipeline
    bool signaled = false;
    for (const auto& info : infos) {
        if (ss::this_shard_id() == info.shard && info.bytes > 0) {
            // Fast path: process requests locally
            // This shard is the one that has the most data.
            _stage.process(
              [this, &signaled](const write_request<Clock>& r) noexcept {
                  signaled = true;
                  _probe.register_time_fallback(r.size_bytes());
                  return request_processing_result::advance_and_continue;
              });
            break;
        }
    }
    if (!signaled) {
        // It is guaranteed that the shard that has the most data will become
        // the target shard. But it is also possible that the data threshold
        // policy will trigger the upload on this shard. In this case the loop
        // above will see no write requests. Other shards are depositing their
        // requests to the target shard without signalling the next stage. So we
        // need to signal it here.
        _stage.signal_next_stage();
    }
    co_await ss::when_all_succeed(in_flight.begin(), in_flight.end());
}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::apply_time_based_fallback() {
    // The time based fallback works in three stages:
    // 1. Collect information about the requests from every shard.
    // 2. Decide which shard should handle the requests.
    // 3. Tell every shard to forward its requests to the target shard.
    // The whole process is triggered on shard 0. The end to end latency
    // of the process is relatively high (up to 1ms) but it's fine. The
    // method is invoked periodically (e.g. every 500ms) so the
    // additional latency is not significant. If we have very high tput
    // on some shards they will be excluded from the time based fallback
    // because they will trigger the data threshold based uploads.
    auto shard_bytes = co_await this->container().map(
      [](write_request_scheduler<Clock>& s) {
          return shard_info{
            .shard = ss::this_shard_id(), .bytes = s.shard_bytes()};
      });

    // NOTE: the heuristic is based on cache behavior. The shard that
    // uploads the data has to read it. If the write request originates
    // from the same shard it will be hot in the CPU cache. Otherwise
    // it will be cold. So the main heuristic is to use the shard
    // that has the most write requests in the pipeline (by size).
    // In the future more complex partitioning can be used. E.g. we can
    // select more than one shard as a target and take NUMA mapping into
    // account. We can also take into account the load and resources
    // available for upload. For now it's assumed that the client pool
    // will always be able to handle the addition L0 upload (e.g. it has
    // borrowing mechanism to do this).

    if (cd_log.is_enabled(ss::log_level::trace)) {
        for (auto& req : shard_bytes) {
            vlog(
              cd_log.trace,
              "map result: {} requests, {} bytes",
              req.shard,
              req.bytes);
        }
    }

    // Find the shard with the most bytes to write
    auto target_shard = ss::shard_id(0);
    auto max_shard_bytes = std::numeric_limits<size_t>::min();
    for (auto& info : shard_bytes) {
        if (info.bytes > max_shard_bytes) {
            max_shard_bytes = info.bytes;
            target_shard = info.shard;
        }
    }
    vlog(
      cd_log.debug,
      "bg_time_based_fallback: target shard for write requests: {}, "
      "bytes: {}",
      target_shard,
      max_shard_bytes);

    // We can forward write requests to the target shard now
    // Every shards forwards its own requests.
    //
    // The information flow:
    // shard 0:      collect information -> decide target shard
    // target shard: call 'pull_and_roundtrip' and pass the gate holder
    // target shard: invoke 'forward_to' on every shard that has data
    // shard[i]:     call 'roundtrip' method on shard[i], pass gate holder
    // shard[i]:     invoke 'proxy_write_request' on target shard, pass gate
    //               holder
    // target shard: enqueue shard[i] requests to the target shard's pipeline
    //               and dispose the gate holder that belongs to the
    //               target shard
    // target shard: wait until all requests are enqueued by waiting
    //               on the gate
    // target shard: process own requests and wait until all
    //               'proxy_write_request' calls are done on all shards
    //               by waiting on the gate.

    if (max_shard_bytes > 0) {
        co_await this->container().invoke_on(
          target_shard,
          [shard_bytes](write_request_scheduler<Clock>& scheduler) {
              return scheduler.pull_and_roundtrip(shard_bytes);
          });
    }
}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::bg_time_based_fallback() {
    // This is the background fiber that runs only on shard 0
    // It is waking up every N ms and forces uploads on all shards
    // to be aggregated and uploaded to the cloud storage on a single
    // "target" shard.
    //
    // It handles the case when there is not enough updates on every
    // shards to trigger the upload within N ms.
    try {
        vassert(
          ss::this_shard_id() == ss::shard_id(0),
          "bg_time_based_fallback: should only run on shard 0");
        vassert(
          !_test_only_disable_time_based_fallback,
          "time based fallback is not disabled");
        vlog(
          cd_log.debug,
          "Starting write_request_scheduler time based fallback background "
          "loop");
        while (!_as.abort_requested() && !_stage.stopped()) {
            // Sleep before next iteration
            co_await ss::sleep_abortable(_scheduling_interval(), _as);
            co_await apply_time_based_fallback();
        }
    } catch (...) {
        if (ssx::is_shutdown_exception(std::current_exception())) {
            vlog(cd_log.debug, "bg_time_based_fallback: shutting down");
        } else {
            vlog(
              cd_log.error,
              "bg_time_based_fallback: failed: {}",
              std::current_exception());
        }
    }
}

/// Get a single write request which was created on another shard
/// then copy it and enqueue it to the pipeline on this shard.
/// Then await the response and return it.
template<typename Clock>
ss::future<
  std::expected<typename write_request_scheduler<Clock>::foreign_ptr_t, errc>>
write_request_scheduler<Clock>::proxy_write_request(
  write_request<Clock>* req, ss::gate::holder target_gate_holder) noexcept {
    // This is executed in the context of the target shard.
    // It is safe to dispose the gate holder here because
    // the holder was created on the target shard and
    // it will be destroyed on the target shard as well.
    auto h = _gate.hold();
    _probe.register_receive_xshard(req->size_bytes());
    write_request<Clock> proxy(
      req->ntp,
      req->topic_start_epoch,
      shallow_copy(req->data_chunk),
      req->expiration_time);
    auto fut = proxy.response.get_future();
    _stage.push_next_stage(proxy, false);
    target_gate_holder.release();
    auto extents_fut = co_await ss::coroutine::as_future(std::move(fut));
    if (extents_fut.failed()) {
        auto ex = extents_fut.get_exception();
        vlog(cd_log.error, "Proxy write request failed: {}", ex);
        co_return std::unexpected(errc::upload_failure);
    }
    auto extents = extents_fut.get();
    if (!extents.has_value()) {
        // Normal errors (S3 upload failure or timeout)
        // are handled here
        errc e = extents.error();
        vlog(cd_log.info, "Proxy write request failed: {}", e);
        co_return std::unexpected(e);
    }
    auto ptr = ss::make_lw_shared<chunked_vector<extent_meta>>();
    *ptr = std::move(extents.value());
    foreign_ptr_t fp(ss::make_foreign(std::move(ptr)));
    co_return std::move(fp);
}

template<typename Clock>
void write_request_scheduler<Clock>::ack_write_response(
  write_request<Clock>* req,
  std::expected<write_request_scheduler<Clock>::foreign_ptr_t, errc> resp) {
    // The response was created on another shard.
    // The req was created on this shard.
    // The response contains only extent_meta struct so cheap
    // to copy between shards.
    if (!resp.has_value()) {
        req->set_value(resp.error());
    } else {
        req->set_value(std::move(*resp.value()));
    }
}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::roundtrip(
  ss::shard_id shard,
  write_pipeline<Clock>::write_requests_list list,
  ss::foreign_ptr<gate_holder_ptr> target_shard_gate_holder) {
    // This is executed in the context of the shard that owns the data.
    // The method submits the continuation back to the target shard
    // to complete the operation.
    auto h = _gate.hold();
    // Temporary storage for x-shard request and response correlation.
    using response_t
      = std::expected<write_request_scheduler<Clock>::foreign_ptr_t, errc>;
    struct result_t {
        std::optional<response_t> response{std::nullopt};
        write_request<Clock>* request{nullptr};
    };
    chunked_vector<result_t> results;
    for (auto& req : list.requests) {
        results.push_back(result_t{.request = &req});
        _probe.register_send_xshard(req.size_bytes());
    }
    co_await this->container().invoke_on(
      shard,
      [&results, gh = std::move(target_shard_gate_holder)](
        write_request_scheduler<Clock>& balancer) mutable {
          // This is executed on the target shard
          chunked_vector<ss::future<response_t>> futures;
          for (auto& r : results) {
              vassert(
                r.response.has_value() == false,
                "Should not have response yet");
              futures.emplace_back(balancer.proxy_write_request(
                r.request, /*copy gate holder*/ *gh));
          }
          gh->release();
          return ss::when_all(futures.begin(), futures.end())
            .then([&results](auto fut) {
                for (size_t i = 0; i < results.size(); i++) {
                    // This propagate the response or the error code.
                    // 'proxy_write_request' can't throw and the write_request
                    // can only be acknowledged using error code and not
                    // exception.
                    results[i].response = fut[i].get();
                }
            });
      });
    for (auto& r : results) {
        vassert(r.response.has_value(), "Should have response after invoke_on");
        ack_write_response(r.request, std::move(*r.response));
    }
}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::forward_to(
  ss::shard_id target_shard,
  ss::foreign_ptr<gate_holder_ptr> target_shard_gate_holder) {
    // Owning shard
    auto req = _stage.pull_write_requests(
      _max_buffer_size(), _max_cardinality());
    co_await roundtrip(
      target_shard, std::move(req), std::move(target_shard_gate_holder));
}

template class write_request_scheduler<seastar::lowres_clock>;
template class write_request_scheduler<seastar::manual_clock>;

} // namespace cloud_topics::l0
