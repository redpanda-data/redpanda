/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/partition_translator.h"

#include "config/configuration.h"
#include "datalake/logger.h"
#include "ssx/watchdog.h"
#include "utils/retry_chain_node.h"
#include "utils/to_string.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

namespace datalake::translation {

namespace {
using namespace std::chrono_literals;
// A simple utility to conditionally retry with backoff on failures.
constexpr model::timeout_clock::duration wait_timeout = 5s;

template<
  typename Func,
  typename ShouldRetry,
  typename FuncRet = std::invoke_result_t<Func>,
  typename RetValueType = ss::futurize<FuncRet>::value_type>
requires std::predicate<ShouldRetry, RetValueType>
ss::futurize_t<FuncRet> retry_with_backoff(
  retry_chain_node& parent_rcn, Func&& f, ShouldRetry&& should_retry) {
    parent_rcn.check_abort();
    auto rcn = retry_chain_node(&parent_rcn);
    while (true) {
        auto result_f = co_await ss::coroutine::as_future<RetValueType>(
          ss::futurize_invoke(f));
        auto failed = result_f.failed();
        // eagerly take the exception out to avoid ignored exceptional
        // futures as retry() below can throw.
        std::exception_ptr ex = failed ? result_f.get_exception() : nullptr;
        auto retry = rcn.retry();
        if (!retry.is_allowed) {
            // No more retries allowed, propagated whatever we have.
            if (failed) {
                vassert(
                  ex != nullptr,
                  "Invalid exception, should be non null on a failed future.");
                std::rethrow_exception(ex);
            }
            co_return result_f.get();
        }
        // Further retries are allowed, check for exceptions if any.
        if (!failed) {
            auto result = result_f.get();
            if (!should_retry(result)) {
                co_return result;
            }
        }
        co_await ss::sleep_abortable(retry.delay, *retry.abort_source);
    }
}

} // namespace

partition_translator::partition_translator(
  ss::scheduling_group sg,
  std::unique_ptr<coordinator_api> coordinator,
  std::unique_ptr<data_source> data_source,
  std::unique_ptr<translation_context> translation_ctx,
  std::unique_ptr<translation_lag_tracker> lag_tracker,
  jitter_t jitter)
  : _sg(sg)
  , _coordinator(std::move(coordinator))
  , _data_source(std::move(data_source))
  , _translation_ctx(std::move(translation_ctx))
  , _lag_tracking(std::move(lag_tracker))
  , _jitter{std::move(jitter)}
  , _term(_data_source->term())
  , _logger(
      datalake_log, fmt::format("{}-term-{}", _data_source->ntp(), _term)) {}

ss::future<coordinator::fetch_latest_translated_offset_reply>
partition_translator::fetch_latest_translated_offset(retry_chain_node& rcn) {
    auto request = coordinator::fetch_latest_translated_offset_request{};
    request.tp = _data_source->ntp().tp;
    request.topic_revision = _data_source->topic_revision();

    co_return co_await retry_with_backoff(
      rcn,
      [this, request] {
          return _coordinator->fetch_latest_translated_offset(request);
      },
      [this](coordinator::fetch_latest_translated_offset_reply reply) {
          return !_as.abort_requested()
                 && coordinator::is_retriable(reply.errc);
      });
}

ss::future<coordinator::add_translated_data_files_reply>
partition_translator::checkpoint_translation_result(
  retry_chain_node& rcn, coordinator::translated_offset_range range) {
    auto request = coordinator::add_translated_data_files_request{};
    request.tp = _data_source->ntp().tp;
    request.topic_revision = _data_source->topic_revision();
    request.ranges.push_back(std::move(range));

    co_return co_await retry_with_backoff(
      rcn,
      [this, request = std::move(request)] {
          return _coordinator->add_translated_data_files(request.copy());
      },
      [this](coordinator::add_translated_data_files_reply reply) {
          return !_as.abort_requested()
                 && coordinator::is_retriable(reply.errc);
      });
}

/*
 * datalake_translator_flush_bytes (per partition): finish after a flush limit
 * is reached. this is in place as a safety net for controlling disk usage.
 * TODO: this limit can be removed and the configuration option be deprecated
 * after we gain more confidence in the ability of space management to control
 * datalake disk usage.
 */
bool partition_translator::should_finish_inflight_translation() const {
    auto bytes_flushed_pending_upload = _translation_ctx->flushed_bytes();
    auto lag_window_ended = _lag_tracking->should_finish_inflight_translation();
    vlog(
      _logger.trace,
      "Checking if translation can be finished, current bytes flushed: {}, lag "
      "window roll: {}",
      bytes_flushed_pending_upload,
      lag_window_ended);
    return bytes_flushed_pending_upload
             >= config::shard_local_cfg().datalake_translator_flush_bytes()
           || lag_window_ended;
}

ss::future<std::optional<partition_translator::translation_offsets>>
partition_translator::fetch_translation_offsets(retry_chain_node& rcn) {
    // Reconcile with the coordinator
    auto result = co_await fetch_latest_translated_offset(rcn);
    if (result.errc != coordinator::errc::ok) {
        vlog(_logger.warn, "Failed to fetch translated offset: {}", result);
        co_return std::nullopt;
    }

    vlog(_logger.trace, "Latest coordinator fetch result: {}", result);

    auto last_committed_offset = result.last_iceberg_committed_offset;
    // Update partition metrics. Note that last committed offset here
    // is NOT synchronized with outstanding commit operations. Therefore
    // if we reach this point before the most recent batch of files has
    // been committed, the commit lag metric will be out of sync at
    // least until 'wait_for_data' returns and we re-enter the loop.
    auto max_translatable_offset = _data_source->max_offset_for_translation();
    if (
      max_translatable_offset.value_or(kafka::offset::min())
      >= kafka::offset{0}) {
        int64_t lag = max_translatable_offset.value()
                      - last_committed_offset.value_or(kafka::offset{-1});
        _translation_ctx->report_commit_lag(lag);
    }

    auto next_start_offset = result.last_added_offset
                               ? kafka::next_offset(*result.last_added_offset)
                               : _data_source->min_offset_for_translation();

    // Replicate an initialized last translated offset to unblock the max
    // removable offset while pinning this Kafka offset from
    // application-facing retention or compaction.
    //
    // NOTE: kafka::prev_offset(0) is kafka::offset::min() (uninitialized).
    // in this case we want -1 to differentiate from uninitialized.
    auto checkpointed_lto = next_start_offset == kafka::offset{0}
                              ? kafka::offset{-1}
                              : kafka::prev_offset(next_start_offset);
    /**
     * We do not replicate the timestamp of the highest translated offset
     * here as this information is not present in coordinator. This is fine
     * as the translation stm will simply use the previous timestamp value.
     */
    auto reset_error
      = co_await _data_source->replicate_highest_translated_offset(
        checkpointed_lto, std::nullopt, _term, wait_timeout, _as);

    if (reset_error) {
        vlog(
          _logger.warn,
          "error updating highest translated offset: {}, translation "
          "will "
          "be retried",
          reset_error);
        co_return std::nullopt;
    }

    auto current_translation_lto = _translation_ctx->last_translated_offset();
    _lag_tracking->notify_inflight_translation_iteration(
      current_translation_lto);
    /**
     * If there is no current translation lto or checkpointed value is
     * greater than the current translation lto update it.
     */
    if (
      !current_translation_lto || checkpointed_lto > current_translation_lto) {
        _lag_tracking->notify_data_translated(checkpointed_lto);
        if (
          max_translatable_offset.value_or(kafka::offset::min())
          >= kafka::offset{0}) {
            int64_t lag = max_translatable_offset.value()
                          - std::max(checkpointed_lto, kafka::offset{-1});
            _translation_ctx->report_translation_lag(lag);
        }
        current_translation_lto = checkpointed_lto;
    }

    translation_offsets offsets;
    offsets.coordinator_lto = checkpointed_lto;

    if (result.backpressure) {
        // The coordinator is shedding load for this topic. We've reported lag
        // and reconciled the translated offset above, but don't proceed with
        // translation.
        _translation_ctx->report_backpressure_backoff();
        static constexpr auto log_freq = 30s;
        thread_local static ss::logger::rate_limit rate(log_freq);
        vloglr(
          datalake_log,
          ss::log_level::info,
          rate,
          "[{}] Coordinator applying backpressure (too many pending files); "
          "backing off translation",
          _data_source->ntp());
        co_return offsets;
    }

    static constexpr auto data_wait_duration = 3s;
    // Wait until some data is ready to be translated.
    auto maybe_begin_offset = co_await _data_source->wait_for_data_to_translate(
      current_translation_lto,
      ss::lowres_clock::now() + data_wait_duration,
      _as);

    if (!maybe_begin_offset) {
        vlog(
          _logger.trace,
          "No new data to translate, last translated offset:{}",
          checkpointed_lto);
        co_return offsets;
    }
    offsets.next_translation_begin_offset = maybe_begin_offset.value();
    co_return offsets;
}

ss::future<partition_translator::finish_immediately>
partition_translator::run_one_translation_iteration(
  kafka::offset begin_offset) {
    _lag_tracking->notify_new_data_for_translation(begin_offset);
    // Notify the scheduler that there is some data to translate
    _scheduler->notify_ready(id());
    // wait for the scheduler to notify back that we've been given a
    // time slice (i.e. scheduled in), then translate until the time
    // slice expires or we run out of data
    std::exception_ptr unexpected_ex = nullptr;
    auto result = finish_immediately::no;
    try {
        co_await _ready_to_translate.wait(
          [this] { return _inflight_translation_state.has_value(); });
        auto& as = _inflight_translation_state->as;
        /*
         * before going to the trouble of building a reader and poking the
         * translation context, check if an abort has been requested. this
         * enables the scheduler to execute:
         *
         *    start_translation();
         *    stop_translation();
         *
         * to realize a very fast responsiveness for driving a translator state
         * change such as finishing the on-going translation.
         */
        as.check();
        auto reader = co_await _data_source->make_log_reader(begin_offset, as);
        if (!reader) {
            co_return result;
        }
        vlog(
          _logger.trace, "starting translation from offset: {}", begin_offset);
        ss::timer<scheduling::clock> cancellation_timer;
        cancellation_timer.set_callback([&as] {
            as.request_abort_ex(translator_time_quota_exceeded_error{});
        });

        auto translation_f = _translation_ctx
                               ->translate_now(
                                 std::move(reader.value()),
                                 begin_offset,
                                 _inflight_translation_state->as)
                               .finally(
                                 [this] { return _translation_ctx->flush(); });
        cancellation_timer.arm(_inflight_translation_state->translate_for);
        co_await std::move(translation_f).finally([&cancellation_timer] {
            cancellation_timer.cancel();
        });
        _inflight_translation_state->as.check();
    } catch (const translator_out_of_memory_error&) {
        // We just swallow the exception because the underlying result state
        // is still safe to be flushed.
        if (_finish_translation_requested) {
            vlog(_logger.debug, "Translation requested to finish immediately");
        } else {
            // `translator_out_of_memory_error is pulling double duty for
            // preemption requests and out-of-memory requests. until stop
            // translation is more expressive, we silence the out-of-memory
            // warning if a finish translation request was also made.
            vlog(
              _logger.warn,
              "Translation exceeded memory budget, result will be flushed "
              "immediately");
        }
        // We force a finish immediately to make forward progress and avoid
        // cases where the translator is stuck in this memory exhaustion loop.
        result = finish_immediately::yes;
    } catch (const translator_time_quota_exceeded_error&) {
        // We just swallow the exception because the underlying result state
        // is still safe to be flushed.
        vlog(
          _logger.debug,
          "Translation attempt exceeded scheduler time limit quota");
    } catch (const translator_out_of_disk_error&) {
        // Finishing will free up scratch space on disk
        result = finish_immediately::yes;
    } catch (...) {
        // unknown exception or shutdown exception.
        unexpected_ex = std::current_exception();
        vlog(
          _logger.warn,
          "Translation attempt ran into an unexpected exception: {}",
          unexpected_ex);
    }
    // inflight_translation_state tracks a single scheduled chunk of
    // work, so we reset it to nullopt for the next time we're scheduled
    // in
    _inflight_translation_state.reset();
    // Let the scheduler know we are done
    _scheduler->notify_done(id());

    if (unexpected_ex) {
        co_await _translation_ctx->discard();
        std::rethrow_exception(unexpected_ex);
    }
    co_return result;
}

ss::future<bool> partition_translator::finish_inflight_translation(
  kafka::offset coordinator_lto, retry_chain_node& rcn) {
    auto finish_result = co_await _translation_ctx->finish(rcn, _as);
    if (finish_result.has_error()) {
        auto error = finish_result.error();
        vlog(_logger.trace, "Translation finish ran into an error: {}", error);
        if (
          error == translation_errc::no_data
          || error == translation_errc::discard_error) {
            // no data -- translation is past its lag window but nothing got
            // translated.
            // discard -- certain properties changed that
            // requiring a translator reset
            co_return true;
        }
        vlog(
          _logger.warn, "Failed to translate with error: {}, retrying", error);
        co_return false;
    }
    auto translation_result = std::move(finish_result.value());
    vlog(_logger.trace, "Translation finish result: {}", translation_result);

    // Check if the translated offset space is contiguous, if not make it
    // so.
    auto expected_begin = kafka::next_offset(coordinator_lto);
    if (expected_begin != translation_result.start_offset) {
        // This is possible if there is a gap in offsets range, eg from
        // compaction. Normally that shouldn't be the case, as translation
        // enforces max_removable_local_log_offset which prevents compaction or
        // other forms of retention from kicking in before translation
        // actually happens. However there could be a sequence of enabling /
        // disabling iceberg configuration on the topic that can temporarily
        // unblock compaction thus creating gaps. Here we adjust the offset
        // range to so the coordinator sees a contiguous offset range.
        vlog(
          _logger.info,
          "detected an offset range gap in [{}, {}), adjusting the begin "
          "offset to avoid gaps in coordinator tracked offsets.",
          expected_begin,
          translation_result.start_offset);
        translation_result.start_offset = expected_begin;
    }

    auto last_translated_offset = translation_result.last_offset;
    auto checkpoint_rcn = retry_chain_node(1min, 100ms, &rcn);
    auto checkpoint_result = co_await checkpoint_translation_result(
      checkpoint_rcn, std::move(translation_result));
    if (checkpoint_result.errc != coordinator::errc::ok) {
        vlog(
          _logger.warn,
          "Failed to checkpoint translated files: {}",
          checkpoint_result);
        co_return false;
    }
    /**
     * Lag tracker will return a timestamp only if translation is cougth up
     * with the target translation offset.
     */
    const auto translated_offset_ts
      = _lag_tracking->get_translated_offset_timestamp_estimate(
        last_translated_offset);

    _logger.trace(
      "Replicating translation checkpoint with offset: {} and timestamp: "
      "{}",
      last_translated_offset,
      translated_offset_ts);

    // Generous timeout to let STM catch up and avoid throwing work away on slow
    // operations. By this point we have accumulated translation data and
    // erroring out here would mean we need to re-translate the data.
    // Better wait a bit longer.
    auto replicate_result
      = co_await _data_source->replicate_highest_translated_offset(
        last_translated_offset, translated_offset_ts, _term, 1min, _as);
    if (replicate_result) {
        vlog(
          _logger.warn,
          "error updating highest translated offset: {}",
          replicate_result);
        co_return false;
    }
    co_return true;
}

ss::future<> partition_translator::translate_until_stopped() {
    const auto& id = _data_source->ntp();
    vassert(
      _initialized && _scheduler && _reservations,
      "[{}] Translation started before the translator is properly initialized",
      id);
    bool needs_jitter = false;
    while (!_as.abort_requested()) {
        if (needs_jitter) {
            co_await ss::sleep_abortable(_jitter.next_duration(), _as);
        }
        // We'll keep track of if we exit early out of this iteration, in which
        // case the next iteration should see some jitter.
        auto scoped_set_jitter = ss::defer(
          [&needs_jitter] { needs_jitter = true; });
        // Clear the flag as it is a one-shot request, and since
        // translate_until_stopped can be restarted, for example if this
        // workloop throws. This avoids clearing the flag before running
        // run_one_translation_iteration which uses the flag to silence the
        // out-of-memory warning message.
        auto clear_finish_request = ss::defer(
          [this] { _finish_translation_requested = false; });

        retry_chain_node fetch_offsets_rcn{_as, 1min, 100ms};
        auto offsets = co_await fetch_translation_offsets(fetch_offsets_rcn);
        // this test of the finish translation request flag works here because
        // we are executing in a polling loop.
        auto finish_now = _finish_translation_requested
                            ? finish_immediately::yes
                            : finish_immediately::no;
        if (finish_now) {
            vlog(_logger.debug, "Requested for immediate finish");
        }
        if (!offsets && !finish_now) {
            continue;
        }
        if (offsets->next_translation_begin_offset && !finish_now) {
            // new data is available to translate
            auto translate_f = co_await ss::coroutine::as_future(
              run_one_translation_iteration(
                offsets->next_translation_begin_offset.value()));
            if (translate_f.failed()) {
                translate_f.ignore_ready_future();
                continue;
            }
            finish_now = translate_f.get();
        }
        if (finish_now || should_finish_inflight_translation()) {
            // No global timeout for finishing. We are not blocking the
            // scheduler here and if we can't make progress on this partition
            // timing out/retrying can't help but wastes work.
            retry_chain_node finish_rcn{
              _as, ss::lowres_clock::time_point::max(), 100ms};

            ssx::watchdog wd1min(1min, [ntp = _data_source->ntp()] {
                vlog(
                  datalake_log.debug,
                  "Finishing inflight translation is taking more than 5min for "
                  "{}",
                  ntp);
            });

            ssx::watchdog wd5min(5min, [ntp = _data_source->ntp()] {
                vlog(
                  datalake_log.debug,
                  "Finishing inflight translation is taking more than 5min "
                  "for {}",
                  ntp);
            });

            auto success = co_await finish_inflight_translation(
              offsets->coordinator_lto, finish_rcn);
            if (!success) {
                continue;
            }
        }
        scoped_set_jitter.cancel();
        needs_jitter = false;
    }
}

const scheduling::translator_id& partition_translator::id() const {
    return _data_source->ntp();
}

ss::future<> partition_translator::init(
  scheduling::scheduling_notifications& scheduler,
  scheduling::reservations_tracker& reservations) {
    _scheduler = &scheduler;
    _reservations = &reservations;
    _initialized = true;
    ssx::repeat_until_gate_closed_or_aborted(_gate, _as, [this] {
        return ss::with_scheduling_group(_sg, [this] {
            return translate_until_stopped()
              .handle_exception([this](const std::exception_ptr& ex) {
                  auto log_level = ssx::is_shutdown_exception(ex)
                                     ? ss::log_level::debug
                                     : ss::log_level::warn;
                  vlogl(
                    datalake_log,
                    log_level,
                    "[{}] Encountered exception in translation loop: {}",
                    ex,
                    _data_source->ntp());
              })
              .then([this] {
                  // discard any inflight state and start from scratch
                  return _translation_ctx->discard().then_wrapped(
                    [this](ss::future<> f) {
                        if (f.failed()) {
                            auto ex = f.get_exception();
                            vlog(
                              _logger.warn,
                              "Exception cleaning up inflight translation: {}",
                              ex);
                        }
                        return ss::make_ready_future();
                    });
              });
        });
    });
    return ss::make_ready_future();
}

ss::future<> partition_translator::close() noexcept {
    vlog(_logger.debug, "stopping partition translator in term {}", _term);
    _as.request_abort();
    _ready_to_translate.broken();
    if (_inflight_translation_state) {
        _inflight_translation_state->as.request_abort_ex(
          translator_shutdown_error{});
    }
    _data_source->close();
    co_await _gate.close();
    vlog(_logger.debug, "stopped partition translator in term {}", _term);
}

scheduling::translation_status partition_translator::status() const {
    return scheduling::translation_status{
      .target_lag = _lag_tracking->target_lag(),
      .next_checkpoint_deadline = _lag_tracking->next_checkpoint_deadline(),
      .memory_bytes_reserved = _translation_ctx->buffered_bytes(),
      .disk_bytes_flushed = _translation_ctx->flushed_bytes(),
      .translation_backlog = _lag_tracking->translation_backlog(),
    };
}

std::chrono::milliseconds partition_translator::current_lag_ms() const {
    return _lag_tracking->current_lag_ms();
}

void partition_translator::start_translation(
  scheduling::clock::duration duration) {
    if (_gate.is_closed()) {
        return;
    }
    vassert(
      !_inflight_translation_state.has_value(),
      "Invalid translation state - attempt to start translation on "
      "{} when already in flight.",
      _data_source->ntp());
    _inflight_translation_state = inflight_translation_state{
      .translate_for = duration};
    _ready_to_translate.broadcast();
}

void partition_translator::stop_translation(translator::stop_reason reason) {
    if (_gate.is_closed() || !_inflight_translation_state) {
        return;
    }

    switch (reason) {
    case stop_reason::oom:
        _inflight_translation_state->as.request_abort_ex(
          translator_out_of_memory_error{});
        break;
    case stop_reason::out_of_disk:
        _inflight_translation_state->as.request_abort_ex(
          translator_out_of_disk_error{});
        break;
    }
}

void partition_translator::set_finish_translation() {
    _finish_translation_requested = true;
}

bool partition_translator::get_finish_translation() {
    return _finish_translation_requested;
}

} // namespace datalake::translation
