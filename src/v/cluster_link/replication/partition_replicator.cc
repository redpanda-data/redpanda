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

namespace cluster_link::replication {

static constexpr std::chrono::seconds base_backoff{1};
static constexpr std::chrono::seconds max_backoff{10};

partition_replicator::partition_replicator(
  const model::ntp& ntp,
  model::term_id term,
  std::unique_ptr<data_source> source,
  std::unique_ptr<data_sink> sink)
  : _term(term)
  , _log(cllog, fmt::format("[{}-term-{}] replicator", ntp, term))
  , _source(std::move(source))
  , _sink(std::move(sink))
  , _backoff_policy(make_exponential_backoff_policy<ss::lowres_clock>(
      base_backoff, max_backoff)) {}

ss::future<> partition_replicator::start() {
    vlog(_log.trace, "Starting replicator");
    co_await _source->start();
    co_await _sink->start();
    ssx::repeat_until_gate_closed(_gate, [this] {
        return fetch_and_replicate().handle_exception(
          [this](const std::exception_ptr& e) {
              auto log_level = ssx::is_shutdown_exception(e)
                                 ? ss::log_level::trace
                                 : ss::log_level::warn;
              _log.log(log_level, "Error in partition replicator: {}", e);
          });
    });
}

ss::future<> partition_replicator::stop() {
    vlog(_log.trace, "Stopping replicator");
    _as.request_abort();
    auto f = _gate.close();
    co_await _source->stop();
    co_await _sink->stop();
    co_await std::move(f);
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
          "Replicated batches in range [{} - {}] with at offset: {}",
          begin,
          end,
          result.value().last_offset);
        // A successful end to end replication indicates everything worked from
        // source to sink
        // We only intend to backoff on consecutive failures.
        _backoff_policy.reset();
        co_return true;
    } catch (...) {
        vlog(
          _log.error,
          "Exception during replication: {}",
          std::current_exception());
    }
    co_return false;
}

ss::future<> partition_replicator::replicate_and_wait(
  replicate_ctx ctx, ss::gate& gate, ss::abort_source& as) {
    auto stages = _sink->replicate(
      std::move(ctx.batches), model::max_duration, as);
    co_await std::move(stages.request_enqueued);
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
    co_await _source->reset(
      kafka::next_offset(_sink->last_replicated_offset()));
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
        vlog(
          _log.error,
          "Error in fetch_and_replicate: {}",
          std::current_exception());
        as.request_abort();
    }
    co_await gate.close();
    if (!_gate.is_closed() && !_as.abort_requested()) {
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
