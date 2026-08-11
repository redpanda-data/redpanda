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
#include "cluster_link/replication/replication_probe.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/switch_to.hh>

using namespace std::chrono_literals;

namespace cluster_link::replication {

static constexpr std::chrono::seconds base_backoff{1};
static constexpr std::chrono::seconds max_backoff{10};

partition_replicator::partition_replicator(
  const model::ntp& ntp,
  model::term_id term,
  link_configuration_provider& config_provider,
  std::unique_ptr<data_source> source,
  std::unique_ptr<data_sink> sink,
  ss::scheduling_group scheduling_group,
  std::optional<replication_probe::configuration> cfg,
  link_data_probe_ptr ldp)
  : _ntp(ntp)
  , _term(term)
  , _config_provider(config_provider)
  , _log(cllog, fmt::format("[{}-term-{}] replicator", ntp, term))
  , _source(std::move(source))
  , _sink(std::move(sink))
  , _scheduling_group(scheduling_group)
  , _backoff_policy(
      make_exponential_backoff_policy<ss::lowres_clock>(
        base_backoff, max_backoff))
  , _probe{}
  , _link_data_probe{std::move(ldp)} {
    if (cfg.has_value()) {
        _probe.emplace(std::move(cfg.value()), ntp, *this);
    }
}

ss::future<> partition_replicator::start() {
    co_await ss::coroutine::switch_to(_scheduling_group);
    vlog(_log.trace, "Starting replicator");
    auto holder = _gate.hold();
    co_await _sink->start();
    auto last_replicated = _sink->last_replicated_offset();
    if (last_replicated < kafka::offset{0}) {
        // Sink has not replicated anything yet.
        // We will start from configured start offset.
        _start_offset = co_await _config_provider.start_offset(_ntp, _as);
        vlog(
          _log.debug,
          "Starting replication from configured start offset {}",
          _start_offset);
    } else {
        _start_offset = kafka::next_offset(last_replicated);
        vlog(_log.debug, "Resuming replication from offset {}", _start_offset);
    }
    co_await _source->start(_start_offset);
    ssx::repeat_until_gate_closed_or_aborted(_gate, _as, [this] {
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
    initiate_shutdown();
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

    auto probe_update = fetch_counters::from_fetch_data(ctx.fdata);
    if (_link_data_probe) {
        _link_data_probe->add_fetched(probe_update);
    }

    auto stages = _sink->replicate(
      std::move(ctx.fdata.batches), large_timeout, as);
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
       data = std::move(ctx.fdata.units),
       &as,
       probe_update]() mutable {
          return handle_replication_result(std::move(f), begin, end)
            .then([this, &as, probe_update](bool success) {
                if (!success) {
                    as.request_abort();
                    return;
                }
                if (_link_data_probe) {
                    _link_data_probe->add_written(probe_update);
                }
            })
            .finally(
              [inflight = std::move(inflight), data = std::move(data)]() {});
      });
    co_await _sink->maybe_sync_pid();
}

partition_offsets_report
partition_replicator::get_partition_offsets_report() const {
    auto source_info = _source->get_offsets();
    auto sink_info = _sink->high_watermark();

    return {
      .source_start_offset = source_info.has_value()
                               ? source_info->source_start_offset
                               : kafka::offset{},
      .source_hwm = source_info.has_value() ? source_info->source_hwm
                                            : kafka::offset{},
      .source_lso = source_info.has_value() ? source_info->source_lso
                                            : kafka::offset{},
      .update_time = source_info.has_value() ? source_info->update_time
                                             : ss::lowres_clock::time_point{},
      .shadow_hwm = sink_info,
    };
}

void partition_replicator::set_data_probe(link_data_probe_ptr ldp) {
    _link_data_probe = std::move(ldp);
}

void partition_replicator::unset_data_probe() { _link_data_probe = nullptr; }

kafka::offset partition_replicator::get_partition_lag() const {
    constexpr kafka::offset invalid{-1};

    auto source_info = _source->get_offsets();
    if (!source_info.has_value()) {
        return invalid;
    }

    auto lso = source_info->source_lso;
    if (lso == kafka::offset{-1}) {
        return invalid;
    }

    auto hwm = _sink->high_watermark();
    return lso - hwm;
}

void partition_replicator::initiate_shutdown() noexcept { _as.request_abort(); }
bool partition_replicator::shutdown_initiated() noexcept {
    return _as.abort_requested();
}

ss::future<> partition_replicator::fetch_and_replicate() {
    _as.check();
    _gate.check();
    // abort source for this iteration of fetch_and_replicate
    ss::abort_source as;
    auto subscription = _as.subscribe([&as] noexcept { as.request_abort(); });
    ss::gate gate;
    try {
        while (!_gate.is_closed() && !as.abort_requested()) {
            auto inflight_units = co_await ss::get_units(_max_requests, 1, as);
            auto data = co_await _source->fetch_next(as);
            co_await maybe_synchronize_start_offset();
            if (data.batches.empty()) {
                continue;
            }
            co_await replicate_and_wait(
              {
                .begin = data.batches.front().base_offset(),
                .end = data.batches.back().last_offset(),
                .fdata
                = {.batches = std::move(data.batches), .units = std::move(data.units)},
                .inflight_units = std::move(inflight_units),
              },
              gate,
              as);
        }
    } catch (const ss::sleep_aborted&) {
        // ignore, sleep from fetch was aborted.
    } catch (const monotonicity_violation_exception& ex) {
        // step down and try to recover on next leader term
        vlog(
          _log.warn,
          "Monotonicity violation detected in replicator: {}, stepping down "
          "partition",
          ex.what());
        _sink->notify_replicator_failure(_term);
        as.request_abort();
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
        auto sink_reset_f = co_await ss::coroutine::as_future(_sink->reset());
        if (sink_reset_f.failed()) {
            auto ex = sink_reset_f.get_exception();
            vlog(_log.warn, "Failed to reset data sink on failure: {}", ex);
            _sink->notify_replicator_failure(_term);
            co_return;
        }
        auto reset_offset = _start_offset;
        if (_sink->last_replicated_offset() >= kafka::offset{0}) {
            reset_offset = kafka::next_offset(_sink->last_replicated_offset());
        }
        co_await _source->reset(reset_offset);
        auto sleep_for = _backoff_policy.current_backoff_duration();
        vlog(
          _log.trace,
          "Backing off for {}ms, reset source to offset {}",
          sleep_for / std::chrono::milliseconds{1},
          reset_offset);
        co_await ss::sleep_abortable(sleep_for, _as);
        _backoff_policy.next_backoff();
    }
}

ss::future<> partition_replicator::maybe_synchronize_start_offset() {
    auto shadow_partition_hwm = _sink->high_watermark();
    auto shadow_partition_start_offset = _sink->start_offset();
    auto source_offsets = _source->get_offsets();

    if (!_sink->can_prefix_truncate()) {
        vlog(_log.trace, "Partition does not support prefix truncation");
        co_return;
    }

    if (!source_offsets.has_value()) {
        vlog(_log.debug, "Source partition not reporting offsets");
        co_return;
    }

    auto source_start_offset = source_offsets->source_start_offset;
    auto source_lso = source_offsets->source_lso;

    if (source_start_offset <= shadow_partition_start_offset) {
        vlog(
          _log.trace,
          "Source and shadow partition start offsets are in sync: source: {}, "
          "shadow: {}",
          source_start_offset,
          shadow_partition_start_offset);
        co_return;
    }

    // The source partition may perform a prefix truncation that lands in the
    // middle of a batch. Redpanda's data replicators will replicate the whole
    // batch, including data that starts before the start offset. If we prefix
    // truncate the shadow partition before that batch is replicated, this will
    // interfere with our ability to replicate the entire batch, leading to data
    // loss. To prevent being too eager, we only perform prefix truncation when
    // we know that we have fully replicated all batches up to or past the
    // source start offset. This means: source_start_offset <
    // shadow_partition_hwm OR source_start_offset == source_lso.
    if (
      source_lso != source_start_offset
      && source_start_offset > shadow_partition_hwm) {
        vlog(
          _log.debug,
          "Shadow partition has not replicated up to the source start offset "
          "yet, deferring prefix truncation. source_start_offset: {}, "
          "source_lso: {}, shadow_partition_hwm: {}",
          source_start_offset,
          source_lso,
          shadow_partition_hwm);
        co_return;
    }

    auto truncate_offset = std::max(_start_offset, source_start_offset);
    vlog(
      _log.debug,
      "Truncating shadow partition from {} -> {}",
      shadow_partition_start_offset,
      truncate_offset);

    co_await prefix_truncate(truncate_offset);
}

ss::future<> partition_replicator::prefix_truncate(kafka::offset o) {
    static constexpr auto prefix_truncate_timeout = 5s;
    auto prefix_f = co_await ss::coroutine::as_future(_sink->prefix_truncate(
      o, ss::lowres_clock::now() + prefix_truncate_timeout));
    if (prefix_f.failed()) {
        auto ex = prefix_f.get_exception();
        auto level = ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                                    : ss::log_level::warn;
        vlogl(_log, level, "Exception during prefix truncation: {}", ex);
        co_return;
    }
    auto ec = prefix_f.get();
    // It is possible for another truncation to be queued up if it's higher than
    // the offset we just truncated to.  Only reset if the in progress offset is
    // less than or equal to this offset

    if (ec != kafka::error_code::none) {
        vlog(
          _log.warn,
          "Failed to prefix truncate shadow partition to {}: {}",
          o,
          ec);
        co_return;
    }
    vlog(_log.debug, "Successfully prefix truncated shadow partition to {}", o);
}

} // namespace cluster_link::replication
