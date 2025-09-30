/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/partition_replicator.h"

#include "cluster_link/logger.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/switch_to.hh>

using namespace std::chrono_literals;

namespace cluster_link::replication {

static constexpr std::chrono::seconds base_backoff{1};
static constexpr std::chrono::seconds max_backoff{10};

partition_replicator::partition_replicator(
  const model::ntp& ntp,
  model::term_id term,
  std::unique_ptr<data_source> source,
  std::unique_ptr<data_sink> sink,
  ss::scheduling_group scheduling_group)
  : _term(term)
  , _log(cllog, fmt::format("[{}-term-{}] replicator", ntp, term))
  , _source(std::move(source))
  , _sink(std::move(sink))
  , _scheduling_group(scheduling_group)
  , _backoff_policy(
      make_exponential_backoff_policy<ss::lowres_clock>(
        base_backoff, max_backoff)) {}

ss::future<> partition_replicator::start() {
    co_await ss::coroutine::switch_to(_scheduling_group);
    vlog(_log.trace, "Starting replicator");
    co_await _sink->start();
    co_await _source->start(
      kafka::next_offset(_sink->last_replicated_offset()));
    ssx::repeat_until_gate_closed(_gate, [this] {
        return fetch_and_replicate().handle_exception(
          [this](const std::exception_ptr& e) {
              auto log_level = ssx::is_shutdown_exception(e)
                                 ? ss::log_level::trace
                                 : ss::log_level::warn;
              vlogl(_log, log_level, "Error in fetch_and_replicate: {}", e);
          });
    });
}

ss::future<> partition_replicator::stop() {
    co_await ss::coroutine::switch_to(_scheduling_group);
    vlog(_log.trace, "Stopping replicator");
    _as.request_abort();
    // closing the gate first ensures all the units are returned to the
    // semaphores before the source is stopped.
    co_await _gate.close();
    co_await ss::when_all_succeed(_source->stop(), _sink->stop());
    vlog(_log.trace, "Stopped replicator");
}

void partition_replicator::notify_sink_on_failure(model::term_id term) const {
    _sink->notify_replicator_failure(term);
}

ss::future<bool> partition_replicator::handle_replication_result(
  ss::future<result<raft::replicate_result>> f,
  model::offset begin,
  model::offset end) noexcept {
    try {
        auto result = co_await std::move(f);
        if (result.has_error()) {
            vlog(
              _log.warn,
              "Replication of batches in range [{} - {}] failed with error: {}",
              begin,
              end,
              result.error());
            co_return false;
        }
        vlog(
          _log.trace,
          "Replicated batches in kafka range [{} - {}] ending at raft offset: "
          "{}",
          begin,
          end,
          result.value().last_offset);
        // A successful end to end replication indicates everything worked from
        // source to sink
        // We only intend to backoff on consecutive failures.
        _backoff_policy.reset();
        co_return true;
    } catch (...) {
        auto eptr = std::current_exception();
        auto log_level = ssx::is_shutdown_exception(eptr)
                           ? ss::log_level::debug
                           : ss::log_level::error;
        vlogl(
          _log,
          log_level,
          "Exception during replication: {}",
          std::current_exception());
    }
    co_return false;
}

ss::future<> partition_replicator::replicate_and_wait(
  replicate_ctx ctx, ss::gate& gate, ss::abort_source& as) {
    static constexpr auto large_timeout
      = std::chrono::duration_cast<model::timeout_clock::duration>(5min);
    auto stages = _sink->replicate(std::move(ctx.batches), large_timeout, as);
    auto enqueue_f = co_await ss::coroutine::as_future(
      std::move(stages.request_enqueued));
    std::exception_ptr eptr = nullptr;
    if (enqueue_f.failed()) {
        eptr = enqueue_f.get_exception();
        auto log_level = ssx::is_shutdown_exception(eptr)
                           ? ss::log_level::debug
                           : ss::log_level::error;
        vlogl(
          _log,
          log_level,
          "Exception during replicate request enqueue: {}",
          eptr);
    }
    if (eptr != nullptr || gate.is_closed()) [[unlikely]] {
        // always ensure `replicate_finished` is waited on.
        // This branch is always called in error scenarios, so waiting in the
        // foreground is acceptable and avoids dangling future.
        vlog(
          _log.trace,
          "Waiting for replication to finish in the "
          "foreground");
        co_await std::move(stages.replicate_finished).discard_result();
        if (eptr != nullptr) {
            std::rethrow_exception(eptr);
        }
        co_return;
    }
    ssx::spawn_with_gate(
      gate,
      [this,
       f = std::move(stages.replicate_finished),
       begin = ctx.begin,
       end = ctx.end,
       inflight = std::move(ctx.inflight_units),
       data = std::move(ctx.data_units),
       &as]() mutable {
          return handle_replication_result(std::move(f), begin, end)
            .then([&as](bool success) {
                if (!success) {
                    as.request_abort();
                }
            })
            .finally(
              [inflight = std::move(inflight), data = std::move(data)]() {});
      });
}

ss::future<> partition_replicator::fetch_and_replicate() {
    _gate.check();
    // abort source for this iteration of fetch_and_replicate
    ss::abort_source as;
    auto subscription = _as.subscribe([&as] noexcept { as.request_abort(); });
    ss::gate gate;
    try {
        while (!_gate.is_closed() && !as.abort_requested()) {
            auto inflight_units = co_await ss::get_units(_max_requests, 1, as);
            auto data = co_await _source->fetch_next(as);
            if (data.batches.empty()) {
                continue;
            }
            co_await replicate_and_wait(
              {.begin = data.batches.front().base_offset(),
               .end = data.batches.back().last_offset(),
               .batches = std::move(data.batches),
               .inflight_units = std::move(inflight_units),
               .data_units = std::move(data.units)},
              gate,
              as);
        }
    } catch (const ss::sleep_aborted&) {
        // ignore, sleep from fetch was aborted.
    } catch (...) {
        auto eptr = std::current_exception();
        auto log_level = ssx::is_shutdown_exception(eptr)
                           ? ss::log_level::debug
                           : ss::log_level::error;
        vlogl(_log, log_level, "Error in fetch_and_replicate: {}", eptr);
        as.request_abort();
    }
    co_await gate.close();
    if (!_gate.is_closed() && !_as.abort_requested()) {
        co_await _source->reset(
          kafka::next_offset(_sink->last_replicated_offset()));
        auto sleep_for = _backoff_policy.current_backoff_duration();
        vlog(
          _log.trace,
          "Backing off for {}ms",
          sleep_for / std::chrono::milliseconds{1});
        co_await ss::sleep_abortable(sleep_for, _as);
        _backoff_policy.next_backoff();
    }
}

} // namespace cluster_link::replication
